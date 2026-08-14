//
// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "googlesql/analyzer/rewriters/sql_function_inliner.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/common/errors.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output_properties.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/function.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/rewriter_interface.h"
#include "googlesql/public/sql_function.h"
#include "googlesql/public/sql_tvf.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/templated_sql_function.h"
#include "googlesql/public/templated_sql_tvf.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_builder.h"
#include "googlesql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "googlesql/resolved_ast/resolved_ast_enums.pb.h"
#include "googlesql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "googlesql/resolved_ast/rewrite_utils.h"
#include "absl/base/nullability.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "googlesql/base/map_util.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {
namespace {

using ArgNameToExprMap =
    absl::flat_hash_map</*argument_name=*/absl::string_view,
                        const ResolvedExpr*>;
using ArgScanBuilder =
    std::function<absl::StatusOr<std::unique_ptr<const ResolvedScan>>(
        const ResolvedScan* arg_scan)>;
using ArgNameToScanBuilderMap =
    absl::flat_hash_map</*argument_name=*/absl::string_view, ArgScanBuilder>;
using WithExprColumnDepthMap =
    absl::flat_hash_map<ResolvedColumn, /*depth=*/int>;

// Helps rewriting a SQL function body during inlining.
//
// This rewriter replaces argument references with references to the columns or
// scans that contain the argument values. It operates across scalar UDFs,
// TVFs, and UDAs using argument_map_ for scalar/expression arguments and
// table_arg_map_ for relation arguments.
//
// For subqueries inside the function body, when argument columns or outer WITH
// columns are referenced within those subqueries, they must be correlated and
// appended to the subqueries' parameter lists.
class ResolvedArgumentRefReplacer : public ResolvedASTRewriteVisitor {
 public:
  // Entry point for SQL function, TVF, and UDA argument replacement.
  // Replaces argument references by copying expressions from `argument_map`
  // and building CTE scan references from `table_arg_map`. Tracks active WITH
  // columns and correlates references inside nested subqueries. Passing null
  // for `column_factory` signals the replacer to preserve exact column IDs
  // without remapping across multiple argument references.
  template <typename T>
  static absl::StatusOr<std::unique_ptr<T>> Replace(
      std::unique_ptr<T> body, const ArgNameToExprMap& argument_map,
      ArgNameToScanBuilderMap& table_arg_map,
      const WithExprColumnDepthMap& active_with_expr_columns_depth,
      ColumnFactory* /*absl_nullable*/ column_factory = nullptr) {
    ResolvedArgumentRefReplacer replacer(argument_map, table_arg_map,
                                         active_with_expr_columns_depth,
                                         column_factory);
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const T> result,
                     replacer.VisitAll<T>(std::move(body)));
    return absl::WrapUnique(const_cast<T*>(result.release()));
  }

 private:
  ResolvedArgumentRefReplacer(
      const ArgNameToExprMap& argument_map,
      ArgNameToScanBuilderMap& table_arg_map,
      const WithExprColumnDepthMap& active_with_expr_columns_depth,
      ColumnFactory* /*absl_nullable*/ column_factory)
      : argument_map_(argument_map),
        table_arg_map_(table_arg_map),
        active_with_expr_columns_depth_(active_with_expr_columns_depth),
        column_factory_(column_factory) {
    // Collect all column references from call-site argument expressions so they
    // are recognized as outer columns (definition depth 0) and marked as
    // correlated references when accessed inside inner subqueries.
    std::vector<std::unique_ptr<const ResolvedColumnRef>> column_refs;
    for (const auto& [_, expr] : argument_map_) {
      if (expr != nullptr && CollectColumnRefs(*expr, &column_refs).ok()) {
        for (const auto& ref : column_refs) {
          outer_argument_columns_.insert(ref->column());
        }
        column_refs.clear();
      }
    }
  }

  // WITH expr scope tracking. Registers the columns introduced by each
  // WITH expr assignment along with their definition subquery depth so they are
  // recognized as local bindings rather than function arguments.
  absl::Status PreVisitResolvedWithExpr(const ResolvedWithExpr& node) override {
    std::vector<ResolvedColumn> added_cols;
    added_cols.reserve(node.assignment_list_size());
    for (const auto& col : node.assignment_list()) {
      if (active_with_expr_columns_depth_
              .try_emplace(col->column(), subquery_depth_)
              .second) {
        added_cols.push_back(col->column());
      }
    }
    added_with_expr_columns_stack_.push_back(std::move(added_cols));
    return absl::OkStatus();
  }

  // Pops the WITH expr column scope, erasing the columns introduced by the
  // corresponding PreVisit.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> PostVisitResolvedWithExpr(
      std::unique_ptr<const ResolvedWithExpr> node) override {
    for (const ResolvedColumn& col : added_with_expr_columns_stack_.back()) {
      active_with_expr_columns_depth_.erase(col);
    }
    added_with_expr_columns_stack_.pop_back();
    return node;
  }

  absl::Status PreVisitResolvedWithEntry(
      const ResolvedWithEntry& node) override {
    with_entry_depth_++;
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedWithEntry(
      std::unique_ptr<const ResolvedWithEntry> node) override {
    with_entry_depth_--;
    return node;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedArgumentRef(
      std::unique_ptr<const ResolvedArgumentRef> node) override {
    // Function argument references will be ResolvedArgumentRef when a
    // function's body is resolved as part of the CREATE FUNCTION statement.
    return ReferenceArgumentColumn(node->name());
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedExpressionColumn(
      std::unique_ptr<const ResolvedExpressionColumn> node) override {
    // Function argument references will be ResolvedExpressionColumn when a
    // function's body is resolved using AnalyzeExpressionForAssignmentToType.
    return ReferenceArgumentColumn(node->name());
  }

  // Central dispatch for argument replacement.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> ReferenceArgumentColumn(
      absl::string_view arg_name) {
    if (with_entry_depth_ > 0) {
      return absl::UnimplementedError(
          "SQL defined functions that contain argument references inside "
          "embedded WITH clauses are not implemented.");
    }

    auto it = argument_map_.find(arg_name);
    GOOGLESQL_RET_CHECK(it != argument_map_.end())
        << "Unresolved parameter reference without argument map: " << arg_name;

    std::unique_ptr<ResolvedExpr> copy;
    // For subsequent references to TVF scalar subqueries, remap columns to
    // avoid duplicate column definitions. Initial references and leaf
    // references (such as scalar UDF arguments) are copied directly.
    if (column_factory_ != nullptr &&
        it->second->node_kind() == RESOLVED_SUBQUERY_EXPR &&
        !copied_subquery_args_.insert(it->second).second) {
      const auto* subquery = it->second->GetAs<ResolvedSubqueryExpr>();
      GOOGLESQL_RET_CHECK_NE(subquery, nullptr);
      GOOGLESQL_RET_CHECK_NE(column_factory_, nullptr);
      GOOGLESQL_ASSIGN_OR_RETURN(copy,
                       RemapTvfSubqueryArgument(subquery, *column_factory_));
    } else {
      GOOGLESQL_ASSIGN_OR_RETURN(copy, ResolvedASTDeepCopyVisitor::Copy(it->second));
    }

    // Inside a nested subquery (`subquery_depth_ > 0`), run `VisitAll` across
    // `copy` so `PostVisitResolvedColumnRef` sets `is_correlated = true` on any
    // outer WITH column references (`subquery_depth_ > definition_depth`) and
    // records them in `correlated_columns_stack_` for enclosing parameter
    // lists.
    if (subquery_depth_ > 0) {
      GOOGLESQL_ASSIGN_OR_RETURN(auto adjusted, VisitAll<ResolvedExpr>(std::move(copy)));
      copy = absl::WrapUnique(const_cast<ResolvedExpr*>(adjusted.release()));
    }
    return copy;
  }

  // Allocates fresh column IDs for a TVF scalar subquery argument when it is
  // referenced multiple times across the function body, ensuring that each
  // reference defines distinct column IDs in the rewritten AST.
  static absl::StatusOr<std::unique_ptr<ResolvedExpr>> RemapTvfSubqueryArgument(
      const ResolvedSubqueryExpr* subquery, ColumnFactory& column_factory) {
    const ResolvedScan* scan = subquery->subquery();
    GOOGLESQL_RET_CHECK_NE(scan, nullptr);

    ColumnReplacementMap column_map;
    auto allocate_replacement_columns = [&column_factory, &column_map](
                                            const ResolvedColumnList& columns) {
      for (const ResolvedColumn& col : columns) {
        // TODO: Use col.annotated_type() instead of col.type() to
        // preserve type annotations when remapping TVF scalar subquery columns.
        // Use try_emplace to avoid allocating duplicate column IDs when a
        // ProjectScan passes through columns from its input scan.
        column_map.try_emplace(
            col,
            column_factory.MakeCol(col.table_name(), col.name(), col.type()));
      }
    };

    // Pre-seed column replacement IDs in topological order (input scan before
    // project scan) so new column IDs are allocated sequentially without gaps.
    if (scan->node_kind() == RESOLVED_PROJECT_SCAN) {
      const auto* project = scan->GetAs<ResolvedProjectScan>();
      if (project->input_scan() != nullptr) {
        allocate_replacement_columns(project->input_scan()->column_list());
      }
    }
    allocate_replacement_columns(scan->column_list());

    return CopyResolvedASTAndRemapColumns(*subquery, column_factory,
                                          column_map);
  }

  // Adjusts ResolvedColumnRef nodes that reference outer WITH expr columns or
  // call-site argument columns to set the correct correlation flag based on
  // the current subquery depth.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedColumnRef(
      std::unique_ptr<const ResolvedColumnRef> node) override {
    std::optional<int> definition_depth =
        GetColumnDefinitionDepth(node->column());
    if (!definition_depth.has_value()) {
      return node;
    }

    // Mark columns correlated only if defined outside the current subquery
    // (definition_depth < subquery_depth_). Columns defined at or within the
    // current subquery depth are local and excluded from parameter_list.
    bool is_correlated = subquery_depth_ > *definition_depth;
    if (is_correlated && !correlated_columns_stack_.empty()) {
      correlated_columns_stack_.back().insert(node->column());
    }
    return ToBuilder(std::move(node)).set_is_correlated(is_correlated).Build();
  }

  // Replaces ResolvedRelationArgumentScan nodes for TVF table arguments
  // with the scan produced by the ArgScanBuilder closure.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedRelationArgumentScan(
      std::unique_ptr<const ResolvedRelationArgumentScan> node) override {
    if (table_arg_map_.empty()) {
      return node;
    }
    absl::string_view arg_name = node->name();
    ArgScanBuilder* scan_builder = googlesql_base::FindOrNull(table_arg_map_, arg_name);
    GOOGLESQL_RET_CHECK_NE(scan_builder, nullptr);
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> arg_scan,
                     (*scan_builder)(node.get()));
    return arg_scan;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedSubqueryExpr(
      std::unique_ptr<const ResolvedSubqueryExpr> node) override {
    auto builder = ToBuilder(std::move(node));

    // in_expr was visited in the outer enclosing scope during default
    // traversal. Re-visit subquery in the inner subquery scope.
    std::unique_ptr<const ResolvedScan> subquery = builder.release_subquery();
    GOOGLESQL_RET_CHECK_NE(subquery, nullptr);
    subquery_depth_++;
    correlated_columns_stack_.push_back({});

    GOOGLESQL_ASSIGN_OR_RETURN(subquery, VisitAll<ResolvedScan>(std::move(subquery)));

    subquery_depth_--;
    absl::btree_set<ResolvedColumn> captured =
        std::move(correlated_columns_stack_.back());
    correlated_columns_stack_.pop_back();

    builder.set_subquery(std::move(subquery));

    GOOGLESQL_ASSIGN_OR_RETURN(
        auto adjusted_param_list,
        AdjustParameterList(builder.release_parameter_list(), captured));
    builder.set_parameter_list(std::move(adjusted_param_list));

    return std::move(builder).Build();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> PostVisitResolvedJoinScan(
      std::unique_ptr<const ResolvedJoinScan> node) override {
    if (!node->is_lateral()) {
      return node;
    }

    auto builder = ToBuilder(std::move(node));

    // Lateral joins allow right_scan and join_expr to reference output columns
    // from left_scan. Re-visit right_scan and join_expr in a new nested scope
    // while left_scan remains evaluated in the outer enclosing scope. This
    // re-visit incurs an additional traversal pass over right_scan and
    // join_expr.
    std::unique_ptr<const ResolvedScan> right_scan =
        builder.release_right_scan();
    std::unique_ptr<const ResolvedExpr> join_expr = builder.release_join_expr();

    subquery_depth_++;
    correlated_columns_stack_.push_back({});

    GOOGLESQL_ASSIGN_OR_RETURN(right_scan, VisitAll<ResolvedScan>(std::move(right_scan)));
    if (join_expr != nullptr) {
      GOOGLESQL_ASSIGN_OR_RETURN(join_expr, VisitAll<ResolvedExpr>(std::move(join_expr)));
    }

    subquery_depth_--;
    absl::btree_set<ResolvedColumn> captured =
        std::move(correlated_columns_stack_.back());
    correlated_columns_stack_.pop_back();

    builder.set_right_scan(std::move(right_scan));
    if (join_expr != nullptr) {
      builder.set_join_expr(std::move(join_expr));
    }

    GOOGLESQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters,
        AdjustParameterList(builder.release_parameter_list(), captured));
    builder.set_parameter_list(std::move(parameters));
    return std::move(builder).Build();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedGraphCallScan(
      std::unique_ptr<const ResolvedGraphCallScan> node) override {
    auto builder = ToBuilder(std::move(node));

    // ResolvedGraphCallScan represents a GQL CALL operation, which acts
    // similarly to a lateral join. Its `input_scan` is evaluated in the outer
    // scope, while `subquery` runs in the inner lateral scope. Re-visit
    // `subquery` in a new nested scope while `input_scan` remains evaluated in
    // the outer enclosing scope. This re-visit incurs an additional traversal
    // pass over `subquery`.
    std::unique_ptr<const ResolvedScan> subquery = builder.release_subquery();
    if (subquery != nullptr) {
      subquery_depth_++;
      correlated_columns_stack_.push_back({});

      GOOGLESQL_ASSIGN_OR_RETURN(subquery, VisitAll<ResolvedScan>(std::move(subquery)));

      subquery_depth_--;
      absl::btree_set<ResolvedColumn> captured =
          std::move(correlated_columns_stack_.back());
      correlated_columns_stack_.pop_back();

      builder.set_subquery(std::move(subquery));

      GOOGLESQL_ASSIGN_OR_RETURN(
          std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters,
          AdjustParameterList(builder.release_parameter_list(), captured));
      builder.set_parameter_list(std::move(parameters));
    }

    return std::move(builder).Build();
  }

  absl::Status PreVisitResolvedInlineLambda(
      const ResolvedInlineLambda& node) override {
    subquery_depth_++;
    correlated_columns_stack_.push_back({});
    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedInlineLambda(
      std::unique_ptr<const ResolvedInlineLambda> node) override {
    subquery_depth_--;
    absl::btree_set<ResolvedColumn> captured =
        std::move(correlated_columns_stack_.back());
    correlated_columns_stack_.pop_back();

    auto builder = ToBuilder(std::move(node));
    GOOGLESQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters,
        AdjustParameterList(builder.release_parameter_list(), captured));
    builder.set_parameter_list(std::move(parameters));
    return std::move(builder).Build();
  }

  // Returns the subquery definition depth for a column if it is an active
  // WITH expr column or an outer argument column. Returns std::nullopt if the
  // column is not tracked in either scope.
  std::optional<int> GetColumnDefinitionDepth(const ResolvedColumn& col) const {
    auto it = active_with_expr_columns_depth_.find(col);
    if (it != active_with_expr_columns_depth_.end()) {
      return it->second;
    }
    if (outer_argument_columns_.contains(col)) {
      // Call-site argument expressions are evaluated outside the function body,
      // so any column referenced by them has definition depth 0.
      return 0;
    }
    return std::nullopt;
  }

  // Adjusts existing parameter entries for correct correlation flags and
  // appends newly correlated WITH expr or outer argument columns to the
  // parameter list.
  absl::StatusOr<std::vector<std::unique_ptr<const ResolvedColumnRef>>>
  AdjustParameterList(
      std::vector<std::unique_ptr<const ResolvedColumnRef>> parameters,
      const absl::btree_set<ResolvedColumn>& correlated_columns) {
    // Update existing parameters to reflect correlation relative to the current
    // subquery depth.
    for (auto& param : parameters) {
      std::optional<int> definition_depth =
          GetColumnDefinitionDepth(param->column());
      if (!definition_depth.has_value()) {
        continue;
      }
      const bool is_correlated = subquery_depth_ > *definition_depth;
      GOOGLESQL_ASSIGN_OR_RETURN(
          param,
          ToBuilder(std::move(param)).set_is_correlated(is_correlated).Build());
    }

    // Append newly correlated WITH expr columns or outer argument columns
    // gathered from inner scopes while avoiding duplicate parameter entries.
    for (const ResolvedColumn& col : correlated_columns) {
      bool already_present = false;
      for (const auto& param : parameters) {
        if (param->column() == col) {
          already_present = true;
          break;
        }
      }

      std::optional<int> definition_depth = GetColumnDefinitionDepth(col);
      if (!definition_depth.has_value()) {
        return MakeSqlError()
               << "Correlated column not found in active scopes: "
               << col.name();
      }
      const bool is_correlated = subquery_depth_ > *definition_depth;
      if (!already_present) {
        parameters.push_back(
            MakeResolvedColumnRef(col, /*is_correlated=*/is_correlated));
      }

      // Propagate columns upward if they remain correlated at the current depth
      // so enclosing scopes also include them in their parameter lists.
      if (is_correlated && !correlated_columns_stack_.empty()) {
        correlated_columns_stack_.back().insert(col);
      }
    }
    return parameters;
  }

  // Maps scalar argument names to their replacement expressions.
  const ArgNameToExprMap& argument_map_;

  // Maps table argument names to their replacement relation scans.
  ArgNameToScanBuilderMap& table_arg_map_;

  // Tracks the columns defined in active WITH expr expressions along with the
  // subquery depth at which they were introduced, used to distinguish local
  // bindings from function arguments and to set correct correlation flags
  // across subquery boundaries.
  WithExprColumnDepthMap active_with_expr_columns_depth_;

  // Tracks the columns referenced across all call-site argument expressions in
  // argument_map_. These columns have definition depth 0 and must be marked as
  // correlated references when accessed from within inner subquery scopes.
  absl::flat_hash_set<ResolvedColumn> outer_argument_columns_;

  // Optional column factory used during parameter replacement to allocate
  // fresh column IDs when remapping subqueries across multiple argument
  // references. Passing null signals the replacer to preserve exact column
  // IDs without remapping across multiple argument references.
  ColumnFactory* /*absl_nullable*/ column_factory_;

  // Tracks pre-constructed TVF scalar subquery AST nodes that have already been
  // copied into the rewritten AST. Used inside ReferenceArgumentColumn() to
  // ensure the first reference preserves exact CTE column IDs while subsequent
  // references remap their columns to prevent duplicate column ID errors across
  // multiple argument references.
  absl::flat_hash_set<const ResolvedNode*> copied_subquery_args_;

  // Current nesting depth across subquery, lambda, and lateral join scopes.
  // Used to determine whether a referenced WITH expr column is correlated
  // relative to its definition depth.
  int subquery_depth_ = 0;

  // Stack of sets accumulating correlated ResolvedWithExpr binding columns
  // referenced within each nested subquery, lambda, or lateral join scope so
  // they can be appended to parameter_list. A set is pushed on entry (PreVisit)
  // and popped on exit (PostVisit) for each nested scope. Uses btree_set to
  // ensure deterministic parameter ordering.
  std::vector<absl::btree_set<ResolvedColumn>> correlated_columns_stack_;

  // Stack of column lists introduced by each traversed ResolvedWithExpr so they
  // can be erased from active_with_expr_columns_depth_ upon leaving that WITH
  // expression's scope.
  std::vector<std::vector<ResolvedColumn>> added_with_expr_columns_stack_;

  // Track depth under a WITH entry (which must be a with on subquery).
  // Argument references in WITH scan are not supported.
  int with_entry_depth_ = 0;
};

// Helper function that checks to see if a ResolvedFunctionCall is a call to a
// function that may be inlined. If the function call is inlininable, metadata
// that is useful to the inliner is populated in 'arg_names' and
// 'fn_expression'.
//
// 'arg_names' is the name of the arguments to this function call.
// 'fn_expression' is the ResolvedAST representation of the function body. These
//     nodes may not be owned by the SQL statement being rewritten. For example,
//     they may be owned by the catalog implementation.
static absl::StatusOr<bool> IsCallInlinableAndCollectInfo(
    const ResolvedFunctionCall* call, std::vector<std::string>& arg_names,
    const ResolvedExpr*& fn_expression) {
  const Function* function = call->function();
  GOOGLESQL_RET_CHECK(function != nullptr);
  if (function->Is<SQLFunctionInterface>()) {
    auto sql_fn = call->function()->GetAs<SQLFunctionInterface>();
    arg_names = sql_fn->GetArgumentNames();
    // In case a re-resolved body is attached (for annotations) treat it as a
    // templated function.
    if (call->function_call_info() != nullptr &&
        call->function_call_info()->Is<TemplatedSQLFunctionCall>()) {
      fn_expression =
          call->function_call_info()->GetAs<TemplatedSQLFunctionCall>()->expr();
    } else {
      fn_expression = sql_fn->FunctionExpression();
    }
  } else if (function->Is<TemplatedSQLFunction>()) {
    auto sql_fn = call->function()->GetAs<TemplatedSQLFunction>();
    auto fn_call_info =
        call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
    GOOGLESQL_RET_CHECK_NE(fn_call_info, nullptr);
    arg_names = sql_fn->GetArgumentNames();
    fn_expression = fn_call_info->expr();
  } else {
    return false;
  }
  if (call->hint_list_size() > 0) {
    // Function inlining leaves no place to attach function call hints. It's not
    // clear that inlining a function call with hints is the right thing to do.
    return absl::UnimplementedError(
        absl::StrCat("Hinted calls to SQL defined function '", function->Name(),
                     "' are not supported."));
  }
  return true;
}

// A visitor that replaces calls to SQL UDFs with the resolved function body.
class SqlFunctionInlineVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  SqlFunctionInlineVisitor(const AnalyzerOptions& analyzer_options,
                           Catalog& catalog, ColumnFactory* column_factory,
                           TypeFactory& type_factory)
      : column_factory_(column_factory),
        fn_builder_(analyzer_options, catalog, type_factory) {}

 private:
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    std::vector<std::string> arg_names;
    const ResolvedExpr* fn_expression;
    GOOGLESQL_ASSIGN_OR_RETURN(bool is_inlinable, IsCallInlinableAndCollectInfo(
                                            node, arg_names, fn_expression));
    if (is_inlinable) {
      GOOGLESQL_RET_CHECK_NE(fn_expression, nullptr)
          << "No function expression supplied with resolved call to SQL "
          << "function " << node->DebugString();
      return InlineSqlFunction(node, arg_names, fn_expression);
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  // This function replaces a ResolvedFunctionCall that invokes a SQL function
  // with an expression that computes the function result directly. The
  // transformation looks a bit like this:
  //
  // MySqlFunction(arg0=>Expr0, arg1=>Expr1)
  // ~~>
  // WITH (
  //   arg0 AS Expr0,
  //   arg1 AS Expr1,
  //   FunctionBodyExpr
  // )
  absl::Status InlineSqlFunction(const ResolvedFunctionCall* call,
                                 absl::Span<const std::string> argument_names,
                                 const ResolvedExpr* fn_expression) {
    GOOGLESQL_RET_CHECK_EQ(call->argument_list_size(), argument_names.size());
    GOOGLESQL_RET_CHECK_EQ(call->generic_argument_list_size(), 0);
    GOOGLESQL_RET_CHECK_NE(column_factory_, nullptr);

    // The input function body is potentially owned by a catalog or some other
    // component. Copy the body so that its column ids are compatible with the
    // invoking query and the expression is locally owned.
    ColumnReplacementMap column_map;
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> body_expr,
                     CopyResolvedASTAndRemapColumns(
                         *fn_expression, *column_factory_, column_map));

    if (call->error_mode() == ResolvedFunctionCall::SAFE_ERROR_MODE) {
      GOOGLESQL_RETURN_IF_ERROR(
          fn_builder_.CheckCatalogSupportsSafeMode(call->function()->Name()));
      Value null_value = Value::Null(body_expr->type());
      GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> iferror_call,
                       fn_builder_.IfError(std::move(body_expr),
                                           MakeResolvedLiteral(null_value)));
      body_expr =
          absl::WrapUnique(const_cast<ResolvedExpr*>(iferror_call.release()));
    }

    // Nullary functions get special treatment because we don't have to do any
    // special argument processing.
    if (argument_names.empty()) {
      PushNodeToStack(std::move(body_expr));
      return absl::OkStatus();
    }

    ArgNameToExprMap arg_map;
    std::vector<std::unique_ptr<const ResolvedExpr>> col_refs;
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        with_expr_bindings;
    col_refs.reserve(argument_names.size());
    with_expr_bindings.reserve(argument_names.size());

    for (int i = 0; i < call->argument_list_size(); ++i) {
      // Copy the reference expression.
      GOOGLESQL_RETURN_IF_ERROR(call->argument_list(i)->Accept(this));
      auto arg_expr = ConsumeTopOfStack<ResolvedExpr>();
      ResolvedColumn arg_column = column_factory_->MakeCol(
          absl::StrCat("$inlined_", call->function()->Name()),
          argument_names[i], arg_expr->annotated_type());
      col_refs.push_back(
          MakeResolvedColumnRef(arg_column, /*is_correlated=*/false));
      arg_map[argument_names[i]] = col_refs.back().get();
      with_expr_bindings.push_back(
          MakeResolvedComputedColumn(arg_column, std::move(arg_expr)));
    }

    // Rewrite the function body so that it references the columns in
    // with_expr_bindings rather than having ResolvedArgumentRefs.
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> inlined,
                     InlineFunction(std::move(body_expr), arg_map,
                                    std::move(with_expr_bindings),
                                    /*column_factory=*/nullptr));
    if (call->type_annotation_map() != nullptr) {
      const_cast<ResolvedExpr*>(inlined.get())
          ->set_type_annotation_map(call->type_annotation_map());
    }
    return inlined->Accept(this);
  }

  // Performs parameter substitution across the copied function 'body' using
  // 'ResolvedArgumentRefReplacer', mapping argument names to their
  // inlined column expressions in 'argument_map'. If 'with_expr_bindings' is
  // non-empty, wraps the substituted body inside a ResolvedWithExpr.
  static absl::StatusOr<std::unique_ptr<const ResolvedExpr>> InlineFunction(
      std::unique_ptr<const ResolvedExpr> body,
      const ArgNameToExprMap& argument_map,
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>
          with_expr_bindings,
      ColumnFactory* column_factory = nullptr) {
    WithExprColumnDepthMap active_with_expr_columns;
    for (const auto& binding : with_expr_bindings) {
      GOOGLESQL_RET_CHECK(active_with_expr_columns
                    .emplace(binding->column(), /*subquery_depth=*/0)
                    .second);
    }

    ArgNameToScanBuilderMap no_table_args;
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> replaced_body,
                     ResolvedArgumentRefReplacer::Replace(
                         std::move(body), argument_map, no_table_args,
                         active_with_expr_columns, column_factory));

    if (with_expr_bindings.empty()) {
      return std::move(replaced_body);
    }

    return ResolvedWithExprBuilder()
        .set_type(replaced_body->type())
        .set_type_annotation_map(replaced_body->type_annotation_map())
        .set_assignment_list(std::move(with_expr_bindings))
        .set_expr(std::move(replaced_body))
        .Build();
  }

  ColumnFactory* column_factory_;
  FunctionCallBuilder fn_builder_;
};

class SqlFunctionInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    GOOGLESQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());

    SqlFunctionInlineVisitor rewriter(options, catalog, &column_factory,
                                      type_factory);
    GOOGLESQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "SqlFunctionInliner"; }
};

// A visitor that replaces calls to SQL TVFs with the resolved function body.
class SqlTableFunctionInlineVistor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit SqlTableFunctionInlineVistor(ColumnFactory* column_factory)
      : column_factory_(column_factory) {}

 private:
  absl::StatusOr<bool> IsCallInlinable(const ResolvedTVFScan* scan) {
    if (scan->hint_list_size() > 0) {
      // Function inlining leaves no place to hang function call hints. It's not
      // clear that inlining a function call with hints is even the right thing
      // to do.
      return false;
    }
    const TableValuedFunction* function = scan->tvf();
    GOOGLESQL_RET_CHECK_NE(function, nullptr)
        << "Expected ResolvedTableFunctionScan to have non-null function";
    return function->Is<SQLTableValuedFunction>() ||
           function->Is<TemplatedSQLTVF>();
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* tvf_scan) override {
    GOOGLESQL_ASSIGN_OR_RETURN(bool inlinable, IsCallInlinable(tvf_scan));
    if (inlinable) {
      return InlineTVF(tvf_scan);
    }
    return CopyVisitResolvedTVFScan(tvf_scan);
  }

  absl::Status ErrorIfArgumentIsCorrelated(const ResolvedNode& arg,
                                           int64_t arg_number,
                                           absl::string_view arg_name) {
    std::vector<std::unique_ptr<const ResolvedColumnRef>> free_vars;
    GOOGLESQL_RETURN_IF_ERROR(CollectColumnRefs(arg, &free_vars));
    if (!free_vars.empty()) {
      return absl::UnimplementedError(absl::StrCat(
          "TVF arguments that reference columns are not supported. ", "Arg #",
          arg_number, " ('", arg_name, "') references column '",
          free_vars[0]->column().name(), "'."));
    }
    return absl::OkStatus();
  }

  // This function replaces a ResolvedTVFScan that invokes a SQL table function
  // with a query that computes the function result directly. The
  // transformation looks a bit like this:
  //
  // SELECT ... FROM MyTvf() AS t;
  // ~~>
  // (SELECT ... FROM (tvf_query) AS t
  absl::Status InlineTVF(const ResolvedTVFScan* scan) {
    GOOGLESQL_RET_CHECK_NE(scan, nullptr);
    GOOGLESQL_RET_CHECK_NE(column_factory_, nullptr);
    const ResolvedScan* query = nullptr;
    std::vector<std::string> argument_names;
    if (scan->tvf()->Is<SQLTableValuedFunction>()) {
      const auto* sql_tvf = scan->tvf()->GetAs<SQLTableValuedFunction>();
      GOOGLESQL_RET_CHECK_NE(sql_tvf, nullptr);
      query = sql_tvf->query();
      GOOGLESQL_RET_CHECK_NE(query, nullptr);
      argument_names = sql_tvf->GetArgumentNames();
    } else if (scan->tvf()->Is<TemplatedSQLTVF>()) {
      const auto* sql_tvf = scan->tvf()->GetAs<TemplatedSQLTVF>();
      GOOGLESQL_RET_CHECK_NE(sql_tvf, nullptr);
      query = scan->signature()
                  ->GetAs<TemplatedSQLTVFSignature>()
                  ->resolved_templated_query()
                  ->query();
      argument_names = sql_tvf->GetArgumentNames();
    } else {
      return absl::InternalError(
          "Inlining only supports SQL TVFs and TemplateTVFs.");
    }

    // The input function body is potentially owned by a catalog or some other
    // component. Copy the body so that its column ids are compatible with the
    // invoking query and the scan is locally owned.
    ColumnReplacementMap column_map;
    for (int i = 0; i < scan->column_list_size(); ++i) {
      column_map.insert({query->column_list()[scan->column_index_list()[i]],
                         scan->column_list()[i]});
    }

    std::unique_ptr<ResolvedScan> body_scan;
    if (scan->tvf()->sql_security() ==
        ResolvedCreateStatementEnums::SQL_SECURITY_DEFINER) {
      GOOGLESQL_ASSIGN_OR_RETURN(
          body_scan,
          ReplaceScanColumns(
              *column_factory_, *query, scan->column_index_list(),
              CreateReplacementColumns(*column_factory_, scan->column_list())));
      body_scan = MakeResolvedExecuteAsRoleScan(
          scan->column_list(), std::move(body_scan),
          /*original_inlined_view=*/nullptr, scan->tvf());
    } else {
      // TODO We should decide what to do in the case of
      // UNSPECIFIED, to be consistent with VIEWs and the desired behavior.
      GOOGLESQL_ASSIGN_OR_RETURN(body_scan, ReplaceScanColumns(*column_factory_, *query,
                                                     scan->column_index_list(),
                                                     scan->column_list()));
    }

    // Nullary functions get special treatment because we don't have to do any
    // special argument processing.
    if (scan->argument_list_size() == 0) {
      PushNodeToStack(std::move(body_scan));
      return absl::OkStatus();
    }

    GOOGLESQL_RET_CHECK_EQ(argument_names.size(), scan->argument_list_size());

    // The inlined TVF will become a subquery that contains one CTE query per
    // table argument and one CTE query that computes all scalar arguments with
    // as-if-once semantics.
    std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entry_list;

    // Traverse TVF arguments: wrap relation arguments in CTE scans (table_args)
    // and collect scalar argument expressions to be computed together inside a
    // single shared CTE row (scalars_cte_name).
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> scalar_arg_exprs;
    std::vector<ResolvedColumn> arg_columns;
    ArgNameToExprMap scalar_args;
    std::vector<std::unique_ptr<const ResolvedExpr>> owned_scalar_subqueries;
    std::vector<std::string> scalar_arg_names;
    scalar_arg_names.reserve(scan->argument_list_size());
    std::string scalars_cte_name =
        absl::StrCat("$inlined_", scan->tvf()->Name(), "_scalar_args");
    ArgNameToScanBuilderMap table_args = ArgNameToScanBuilderMap{};
    for (int i = 0; i < scan->argument_list_size(); ++i) {
      const ResolvedFunctionArgument* arg = scan->argument_list(i);
      const std::string& arg_name = argument_names[i];
      // Handle relation/table arguments by wrapping each in a WITH CTE entry.
      if (scan->argument_list(i)->scan() != nullptr) {
        GOOGLESQL_ASSIGN_OR_RETURN(auto arg_scan, ProcessNode<ResolvedScan>(arg->scan()));
        GOOGLESQL_RET_CHECK_GE(scan->argument_list_size(), 1);
        GOOGLESQL_RETURN_IF_ERROR(
            ErrorIfArgumentIsCorrelated(*arg_scan, i + 1, arg_name));
        const std::string& arg_cte_name = argument_names[i];
        with_entry_list.emplace_back(
            MakeResolvedWithEntry(arg_cte_name, std::move(arg_scan)));
        table_args[argument_names[i]] =
            [arg_cte_name](const ResolvedScan* arg_scan)
            -> absl::StatusOr<std::unique_ptr<const ResolvedScan>> {
          auto with_ref =
              ResolvedWithRefScanBuilder().set_with_query_name(arg_cte_name);
          for (ResolvedColumn col : arg_scan->column_list()) {
            with_ref.add_column_list(col);
          }
          return std::move(with_ref).Build();
        };
        continue;
      }

      // Handle scalar arguments by collecting expressions into a shared CTE
      // row.
      const ResolvedExpr* argument = scan->argument_list(i)->expr();
      if (argument == nullptr) {
        return absl::UnimplementedError(
            absl::StrCat("TVF argument #", i + 1, " ('", arg_name,
                         "') is not an argument kind supported by inlining."));
      }
      GOOGLESQL_RET_CHECK_NE(argument, nullptr);
      GOOGLESQL_RETURN_IF_ERROR(ErrorIfArgumentIsCorrelated(*argument, i + 1, arg_name));
      GOOGLESQL_RETURN_IF_ERROR(argument->Accept(this));
      auto arg_expr = ConsumeTopOfStack<ResolvedExpr>();
      scalar_arg_names.push_back(argument_names[i]);
      ResolvedColumn arg_column = column_factory_->MakeCol(
          absl::StrCat("$inlined_", scan->tvf()->Name()), arg_name,
          arg_expr->annotated_type());
      scalar_arg_exprs.push_back(
          MakeResolvedComputedColumn(arg_column, std::move(arg_expr)));
      arg_columns.push_back(arg_column);
    }

    // Build a scalar subquery (SELECT arg_idx FROM scalars_cte_name) for each
    // scalar argument and populate scalar_args so that argument references in
    // the TVF body can be replaced directly.
    for (int idx = 0; idx < arg_columns.size(); ++idx) {
      std::string scan_name = absl::StrCat("$inlined_", scan->tvf()->Name());
      auto with_ref =
          ResolvedWithRefScanBuilder().set_with_query_name(scalars_cte_name);
      ResolvedProjectScanBuilder project;
      ResolvedSubqueryExprBuilder subquery;
      for (int c = 0; c < arg_columns.size(); ++c) {
        // TODO: Use arg_columns[c].annotated_type() and propagate
        // type annotations on the scalar subquery expression.
        ResolvedColumn col = column_factory_->MakeCol(
            scan_name, arg_columns[c].name(), arg_columns[c].type());
        with_ref.add_column_list(col);
        if (c == idx) {
          project.add_column_list(col);
          subquery.set_type(col.type());
        }
      }
      GOOGLESQL_ASSIGN_OR_RETURN(auto subquery_expr,
                       std::move(subquery)
                           .set_subquery_type(ResolvedSubqueryExpr::SCALAR)
                           .set_in_expr(nullptr)
                           .set_subquery(std::move(project).set_input_scan(
                               std::move(with_ref)))
                           .Build());
      owned_scalar_subqueries.push_back(std::move(subquery_expr));
      scalar_args[scalar_arg_names[idx]] = owned_scalar_subqueries.back().get();
    }
    if (!scalar_arg_exprs.empty()) {
      with_entry_list.emplace_back(MakeResolvedWithEntry(
          scalars_cte_name,
          MakeResolvedProjectScan(arg_columns, std::move(scalar_arg_exprs),
                                  MakeResolvedSingleRowScan())));
    }

    // Rewrite the function body so that scalar argument references are replaced
    // by scalar subqueries scanning the scalars_cte_name CTE. TVFs do not use
    // top-level ResolvedWithExpr bindings.
    WithExprColumnDepthMap empty_with_expr_columns;
    GOOGLESQL_ASSIGN_OR_RETURN(body_scan,
                     ResolvedArgumentRefReplacer::Replace(
                         std::move(body_scan), scalar_args, table_args,
                         empty_with_expr_columns, column_factory_));

    GOOGLESQL_RET_CHECK(!with_entry_list.empty());
    // This variable prevents use-after move ambiguity in the following stmt.
    const std::vector<ResolvedColumn>& columns = body_scan->column_list();
    PushNodeToStack(MakeResolvedWithScan(columns, std::move(with_entry_list),
                                         std::move(body_scan),
                                         /*recursive=*/false));
    return absl::OkStatus();
  }

 private:
  ColumnFactory* column_factory_;
};

class SqlTvfInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    GOOGLESQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    SqlTableFunctionInlineVistor rewriter(&column_factory);
    GOOGLESQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "SqlTvfInliner"; }
};

// Returns the `TemplatedSQLFunctionCall` representing the attached SQL body
// resolved for that particular SQL function call. The result is never null when
// the call if for a templated SQL function, or a concrete SQL function with
// annotated args that differ from the function declaration.
// For non-SQL functions, the result is always `nullptr`.
//
// When a concrete SQL function is invoked with annotated args which do not
// match the annotations on the argument declaration, the function acts as a
// templated function: it is re-resolved and the annotated body for that
// invocation is attached.
static const TemplatedSQLFunctionCall*
GetInfoForTemplatedOrReResolvedSqlFunctionCall(
    const ResolvedAggregateFunctionCall* call) {
  if (call->function()->Is<TemplatedSQLFunction>()) {
    return call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
  }
  if (!call->function()->Is<SQLFunctionInterface>()) {
    // Not a SQL function.
    return nullptr;
  }
  if (call->function_call_info() == nullptr ||
      !call->function_call_info()->Is<TemplatedSQLFunctionCall>()) {
    return nullptr;
  }
  return call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
}

class SqlAggregateFunctionInlineVisitor : public ResolvedASTRewriteVisitor {
 public:
  explicit SqlAggregateFunctionInlineVisitor(ColumnFactory& column_factory)
      : column_factory_(column_factory) {}

 private:
  // The data-structures representing concrete and template SQL functions are
  // different. This struct lets us gather the common bits so that inlining
  // logic can be more agnostic to whether the function was concrete or a
  // template.
  struct AggregateFnDetails {
    const ResolvedAggregateFunctionCall* call;
    const ResolvedExpr* expr;
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        aggregate_expression_list;
    std::vector<std::string> arg_names;
    ResolvedColumn computed_column;
  };

  // These lists usually travel together as we handle UDAs on the same scan.
  struct UdaRewriteContext {
    // Pre-aggregate expressions collected for inlined UDAs.
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        pre_aggregate_exprs;
    // Pre-aggregate columns, including those collected for inlined UDAs.
    // Initialized with columns from the original input scan. Expressions added
    // to `pre_aggregate_exprs` are appended to this list.
    std::vector<ResolvedColumn> pre_aggregate_cols;
    // The new list of aggregates. Includes aggregations that were not
    // rewritten.
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        new_aggregates;
    // The column list produced for the list of aggregates post-rewrite.
    // Corresponds 1:1 with `new_aggregates`.
    std::vector<ResolvedColumn> new_aggr_col_list;
    // Post-aggregate expressions produced by the rewrite.
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        post_aggregate_exprs;

    explicit UdaRewriteContext(
        absl::Span<const ResolvedColumn> pre_aggregate_cols)
        : pre_aggregate_cols(pre_aggregate_cols.begin(),
                             pre_aggregate_cols.end()) {}
  };

  // Returns true if the function body is SQL-defined.
  bool IsSqlDefined(const Function* function) {
    return function->Is<SQLFunctionInterface>() ||
           function->Is<TemplatedSQLFunction>();
  }

  // Returns true if the aggregate function `call` (or any of its nested
  // aggregate functions) contain a nested SQL-defined function.
  // Note that this does not check if `call` itself is a SQL-defined function;
  // it just checks for the existence of a nested SQL-defined function within
  // the aggregate function subtree.
  absl::StatusOr<bool> ContainsNestedSqlDefinedFunction(
      const ResolvedAggregateFunctionCall* call) {
    for (const auto& nested_agg : call->group_by_aggregate_list()) {
      GOOGLESQL_RET_CHECK(nested_agg->expr()->Is<ResolvedAggregateFunctionCall>());
      const auto& nested_agg_call =
          nested_agg->expr()->GetAs<ResolvedAggregateFunctionCall>();
      if (IsSqlDefined(nested_agg_call->function())) {
        return true;
      }
      GOOGLESQL_ASSIGN_OR_RETURN(bool contains_nested_sql_defined_function,
                       ContainsNestedSqlDefinedFunction(nested_agg_call));
      if (contains_nested_sql_defined_function) {
        return true;
      }
    }
    return false;
  }

  // Check to see if the aggregate function (or one of its nested aggregate
  // functions) is SQL-defined and return details required for inlining if so.
  // Returns std::nullopt if no SQL-defined functions are found.
  // Returns an error if the function (or one of its nested aggregate
  // functions) requires inlining but has a shape not supported by the inliner.
  absl::StatusOr<std::optional<AggregateFnDetails>> IsInlineable(
      const ResolvedAggregateFunctionCall* call) {
    const Function* function = call->function();
    const ParseLocationRange* error_location =
        call->GetParseLocationRangeOrNULL();

    // Check if any nested aggregate function is a SQL-defined function. The
    // inliner does not currently support inlining nested UDAs.
    GOOGLESQL_ASSIGN_OR_RETURN(bool contains_nested_sql_defined_function,
                     ContainsNestedSqlDefinedFunction(call));
    if (contains_nested_sql_defined_function) {
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with nested UDA";
    }

    if (!IsSqlDefined(function)) {
      return std::nullopt;
    }
    if (call->error_mode() == ResolvedFunctionCall::SAFE_ERROR_MODE) {
      // TODO: Support SAFE mode calls using IFERROR.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with SAFE mode modifier";
    }
    if (call->distinct()) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with DISTINCT modifier";
    }
    if (call->limit() != nullptr) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with LIMIT modifier";
    }
    if (call->order_by_item_list_size() > 0) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with ORDER BY modifier";
    }
    if (call->having_modifier() != nullptr) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with HAVING modifier";
    }
    if (call->null_handling_modifier() ==
        ResolvedNonScalarFunctionCallBase::RESPECT_NULLS) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with RESPECT NULLS modifier";
    }
    if (call->null_handling_modifier() ==
        ResolvedNonScalarFunctionCallBase::IGNORE_NULLS) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with IGNORE NULLS modifier";
    }
    if (!call->group_by_list().empty()) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with GROUP BY modifier";
    }
    if (call->where_expr() != nullptr) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with WHERE filter modifier";
    }
    if (call->having_expr() != nullptr) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "SQL function inliner cannot inline aggregate function "
             << function->SQLName() << " with HAVING filter modifier";
    }
    const TemplatedSQLFunctionCall* fn_call_info =
        GetInfoForTemplatedOrReResolvedSqlFunctionCall(call);
    if (fn_call_info != nullptr) {
      return AggregateFnDetails{
          .call = call,
          .expr = fn_call_info->expr(),
          .aggregate_expression_list =
              fn_call_info->aggregate_expression_list(),
          .arg_names = call->function()->Is<TemplatedSQLFunction>()
                           ? call->function()
                                 ->GetAs<TemplatedSQLFunction>()
                                 ->GetArgumentNames()
                           : call->function()
                                 ->GetAs<SQLFunctionInterface>()
                                 ->GetArgumentNames(),
      };
    } else if (function->Is<SQLFunctionInterface>()) {
      auto* fn = function->GetAs<SQLFunctionInterface>();
      // If a body were attached, we would have already treated it as a
      // templated function.
      GOOGLESQL_RET_CHECK(call->function_call_info() == nullptr ||
                !call->function_call_info()->Is<TemplatedSQLFunctionCall>());
      return AggregateFnDetails{
          .call = call,
          .expr = fn->FunctionExpression(),
          .aggregate_expression_list = *fn->aggregate_expression_list(),
          .arg_names = fn->GetArgumentNames(),
      };
    }
    GOOGLESQL_RET_CHECK_FAIL() << "Return should be unreachable.";
  }

  absl::Status RewriteAggregation(
      const AggregateFnDetails& details,
      std::unique_ptr<const ResolvedAggregateFunctionCall> aggr,
      UdaRewriteContext& context) {
    auto aggr_expr_builder = ToBuilder(std::move(aggr));
    auto aggr_args = aggr_expr_builder.release_argument_list();
    ArgNameToExprMap aggregate_args;
    ArgNameToExprMap non_aggregate_args;
    std::vector<std::unique_ptr<const ResolvedExpr>> owned_aggregate_arg_refs;
    FunctionSignature signature = aggr_expr_builder.signature();

    // This logic assumes no repeated args.
    GOOGLESQL_RET_CHECK_EQ(details.arg_names.size(), aggr_args.size());
    for (int i = 0; i < aggr_args.size(); ++i) {
      bool is_non_aggregate_arg =
          signature.arguments()[i].options().is_not_aggregate();
      std::unique_ptr<const ResolvedExpr>& arg = aggr_args[i];
      if (is_non_aggregate_arg) {
        // If we ever extend non-aggregate args beyond these types, the
        // rewriter will need to change to accommodate as-if-evaluated-once
        // semantics. The ResolvedAST is not expressive enough for that right
        // now without introducing an artificial array construction above the
        // aggregation which some query optimizers would not remove. The
        // expressive power that is needed is a lateral join with a single row
        // table on the LHS.
        const ResolvedExpr* without_cast = arg.get();
        // LINT.IfChange(non_aggregate_args_def)
        while (without_cast->node_kind() == RESOLVED_CAST) {
          without_cast = without_cast->GetAs<ResolvedCast>()->expr();
        }
        ResolvedNodeKind expr_kind = without_cast->node_kind();
        GOOGLESQL_RET_CHECK(expr_kind == RESOLVED_LITERAL ||
                  expr_kind == RESOLVED_PARAMETER ||
                  expr_kind == RESOLVED_ARGUMENT_REF);
        // LINT.ThenChange(../expr_resolver_helper.cc:non_aggregate_args_def)
        // This collection is used exclusively by the post-aggregate
        // expression.
        non_aggregate_args.emplace(details.arg_names[i], arg.get());
        // This collection is used for the arguments to the aggregate
        // functions. Non-aggregate args can be used there too, so we add
        // these args to both collections.
        aggregate_args.emplace(details.arg_names[i], arg.get());
      } else {
        // This is an aggregate arg.
        ResolvedColumn new_arg_column = column_factory_.MakeCol(
            absl::StrCat("$inlined_", details.call->function()->Name()),
            details.arg_names[i], arg->annotated_type());
        GOOGLESQL_ASSIGN_OR_RETURN(auto new_arg_computed_col,
                         ResolvedComputedColumnBuilder()
                             .set_column(new_arg_column)
                             .set_expr(std::move(arg))
                             .Build());
        context.pre_aggregate_exprs.push_back(std::move(new_arg_computed_col));
        context.pre_aggregate_cols.push_back(new_arg_column);
        owned_aggregate_arg_refs.push_back(
            MakeResolvedColumnRef(new_arg_column, /*is_correlated=*/false));
        aggregate_args.emplace(details.arg_names[i],
                               owned_aggregate_arg_refs.back().get());
      }
    }

    // SQL-defined aggregates have any aggregations internal to the function
    // body already factored out. Those aggregations will be promoted into the
    // new copy of the AggregateScan. Those are processed in this loop,
    // collecting the processed aggregations in `new_aggregates` and also the
    // columns they are written into in `new_aggr_col_list`. These lists will
    // later be used to build the new AggregateScan.

    // Aggregates that are internal to the function body, once copied, have
    // new column id. This map is used to replace references to those column
    // ids in the post-aggregate expression.
    ColumnReplacementMap internal_aggregate_remapping;
    ColumnReplacementMap no_replacements;
    ArgNameToScanBuilderMap no_table_args;
    // UDAs do not use top-level ResolvedWithExpr bindings.
    WithExprColumnDepthMap empty_with_expr_columns;
    for (auto& aggr_computed_col : details.aggregate_expression_list) {
      GOOGLESQL_ASSIGN_OR_RETURN(
          auto new_aggr_computed_col,
          CopyResolvedASTAndRemapColumns(*aggr_computed_col, column_factory_,
                                         no_replacements));
      GOOGLESQL_ASSIGN_OR_RETURN(new_aggr_computed_col,
                       ResolvedArgumentRefReplacer::Replace(
                           std::move(new_aggr_computed_col), aggregate_args,
                           no_table_args, empty_with_expr_columns));
      internal_aggregate_remapping.emplace(aggr_computed_col->column(),
                                           new_aggr_computed_col->column());
      context.new_aggr_col_list.push_back(new_aggr_computed_col->column());
      context.new_aggregates.push_back(std::move(new_aggr_computed_col));
    }
    GOOGLESQL_ASSIGN_OR_RETURN(
        auto post_aggregate_function_body,
        CopyResolvedASTAndRemapColumns(*details.expr, column_factory_,
                                       internal_aggregate_remapping));
    GOOGLESQL_ASSIGN_OR_RETURN(
        auto post_aggregate_expr,
        ResolvedArgumentRefReplacer::Replace(
            std::move(post_aggregate_function_body), non_aggregate_args,
            no_table_args, empty_with_expr_columns));
    GOOGLESQL_ASSIGN_OR_RETURN(auto post_aggregate_computed_col,
                     ResolvedComputedColumnBuilder()
                         .set_column(details.computed_column)
                         .set_expr(std::move(post_aggregate_expr))
                         .Build());
    context.post_aggregate_exprs.emplace_back(
        std::move(post_aggregate_computed_col));
    return absl::OkStatus();
  }

  absl::Status RewriteAggregations(
      std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
          old_aggregates,
      std::unique_ptr<const ResolvedScan>& input_scan,
      absl::flat_hash_map<const ResolvedAggregateFunctionCall*,
                          AggregateFnDetails>& calls_to_inline,
      absl::Span<const ResolvedColumn> post_aggregate_column_list,
      UdaRewriteContext& context) {
    absl::flat_hash_set<ResolvedColumn> columns_to_remove_from_aggr;

    for (auto& aggr_column : old_aggregates) {
      GOOGLESQL_RET_CHECK(aggr_column->Is<ResolvedComputedColumn>());
      auto aggr = aggr_column->GetAs<ResolvedComputedColumn>();
      GOOGLESQL_RET_CHECK(aggr->expr()->Is<ResolvedAggregateFunctionCall>());
      const ResolvedAggregateFunctionCall* aggr_function_call =
          aggr->expr()->GetAs<ResolvedAggregateFunctionCall>();

      if (!calls_to_inline.contains(aggr_function_call)) {
        context.new_aggregates.emplace_back(std::move(aggr_column));
        continue;
      }
      AggregateFnDetails& details = calls_to_inline.at(aggr_function_call);
      details.computed_column = aggr->column();
      columns_to_remove_from_aggr.insert(aggr->column());

      auto agg_call = absl::WrapUnique(
          ToBuilder(absl::WrapUnique(
                        aggr_column.release()->GetAs<ResolvedComputedColumn>()))
              .release_expr()
              .release()
              ->GetAs<ResolvedAggregateFunctionCall>());

      GOOGLESQL_RETURN_IF_ERROR(
          RewriteAggregation(details, std::move(agg_call), context));
    }

    for (const auto& old_aggr_col : post_aggregate_column_list) {
      if (!columns_to_remove_from_aggr.contains(old_aggr_col)) {
        context.new_aggr_col_list.push_back(old_aggr_col);
      }
    }

    if (!context.pre_aggregate_exprs.empty()) {
      GOOGLESQL_ASSIGN_OR_RETURN(
          input_scan,
          ResolvedProjectScanBuilder()
              .set_input_scan(std::move(input_scan))
              .set_expr_list(std::move(context.pre_aggregate_exprs))
              .set_column_list(std::move(context.pre_aggregate_cols))
              .Build());
    }
    return absl::OkStatus();
  }

  absl::StatusOr<absl::flat_hash_map<const ResolvedAggregateFunctionCall*,
                                     AggregateFnDetails>>
  GetCallsToInline(const ResolvedAggregateScan* node) {
    absl::flat_hash_map<const ResolvedAggregateFunctionCall*,
                        AggregateFnDetails>
        calls_to_inline;
    for (const auto& column : node->aggregate_list()) {
      const auto* col = column->GetAs<ResolvedComputedColumnImpl>();
      GOOGLESQL_RET_CHECK(col->expr()->Is<ResolvedAggregateFunctionCall>());
      const ResolvedAggregateFunctionCall* aggr_function_call =
          col->expr()->GetAs<ResolvedAggregateFunctionCall>();
      GOOGLESQL_ASSIGN_OR_RETURN(std::optional<AggregateFnDetails> details,
                       IsInlineable(aggr_function_call));
      if (details.has_value()) {
        calls_to_inline.emplace(aggr_function_call, *details);
      }
    }
    return calls_to_inline;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateScan(
      std::unique_ptr<const ResolvedAggregateScan> node) override {
    GOOGLESQL_ASSIGN_OR_RETURN(auto calls_to_inline, GetCallsToInline(node.get()));
    if (calls_to_inline.empty()) {
      return node;
    }

    ResolvedAggregateScanBuilder aggr_builder = ToBuilder(std::move(node));

    // The post-aggregation Project will have the same column list as the input
    // aggregate scan. The new aggregate scan will not have columns associated
    // with re-written function calls.
    std::vector<ResolvedColumn> post_aggregate_column_list =
        aggr_builder.column_list();

    std::unique_ptr<const ResolvedScan> input_scan =
        aggr_builder.release_input_scan();

    UdaRewriteContext context(input_scan->column_list());
    GOOGLESQL_RETURN_IF_ERROR(RewriteAggregations(aggr_builder.release_aggregate_list(),
                                        input_scan, calls_to_inline,
                                        post_aggregate_column_list, context));

    return ResolvedProjectScanBuilder()
        .set_input_scan(
            std::move(aggr_builder)
                .set_input_scan(std::move(input_scan))
                .set_aggregate_list(std::move(context.new_aggregates))
                .set_column_list(std::move(context.new_aggr_col_list)))
        .set_expr_list(std::move(context.post_aggregate_exprs))
        .set_column_list(std::move(post_aggregate_column_list))
        .Build();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregationThresholdAggregateScan(
      std::unique_ptr<const ResolvedAggregationThresholdAggregateScan> node)
      override {
    for (const auto& computed_column : node->aggregate_list()) {
      GOOGLESQL_RET_CHECK(computed_column->Is<ResolvedComputedColumnImpl>());
      auto computed_col = computed_column->GetAs<ResolvedComputedColumnImpl>();
      if (computed_col->expr()->Is<ResolvedAggregateFunctionCall>() &&
          (computed_col->expr()
               ->GetAs<ResolvedAggregateFunctionCall>()
               ->function()
               ->Is<SQLFunctionInterface>() ||
           computed_col->expr()
               ->GetAs<ResolvedAggregateFunctionCall>()
               ->function()
               ->Is<TemplatedSQLFunction>())) {
        return MakeSqlErrorAtStart(computed_col->expr()
                                       ->GetAs<ResolvedAggregateFunctionCall>()
                                       ->GetParseLocationRangeOrNULL())
               << "Aggregation threshold is not supported with user defined "
                  "aggregate function";
      }
    }
    return node;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedPivotScan(
      std::unique_ptr<const ResolvedPivotScan> node) override {
    absl::flat_hash_map<const ResolvedAggregateFunctionCall*,
                        AggregateFnDetails>
        calls_to_inline;
    for (const auto& expr : node->pivot_expr_list()) {
      GOOGLESQL_RET_CHECK(expr->Is<ResolvedAggregateFunctionCall>());
      const auto* call = expr->GetAs<ResolvedAggregateFunctionCall>();
      GOOGLESQL_ASSIGN_OR_RETURN(std::optional<AggregateFnDetails> details,
                       IsInlineable(call));
      if (details.has_value()) {
        calls_to_inline.emplace(call, *details);
      }
    }
    if (!calls_to_inline.empty()) {
      return absl::InvalidArgumentError(
          "SQL-defined aggregate functions are not supported in PIVOT");
    }
    return node;
  }

 private:
  ColumnFactory& column_factory_;
};

class SqlUdaInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    GOOGLESQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    SqlAggregateFunctionInlineVisitor rewriter(column_factory);
    return rewriter.VisitAll(std::move(input));
  }

  std::string Name() const override { return "SqlUdaInliner"; }
};

}  // namespace

const Rewriter* GetSqlFunctionInliner() {
  static const auto* const kRewriter = new SqlFunctionInliner;
  return kRewriter;
}

const Rewriter* GetSqlTvfInliner() {
  static const auto* const kRewriter = new SqlTvfInliner;
  return kRewriter;
}

const Rewriter* GetSqlAggregateInliner() {
  static const auto* const kRewriter = new SqlUdaInliner;
  return kRewriter;
}

}  // namespace googlesql
