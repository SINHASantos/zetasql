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

#include "googlesql/analyzer/rewriters/measure_reference_rewrite_util.h"

#include <memory>
#include <utility>
#include <vector>

#include "googlesql/analyzer/annotation_propagator.h"
#include "googlesql/analyzer/rewriters/measure_collector.h"
#include "googlesql/analyzer/rewriters/measure_type_rewriter_util.h"
#include "googlesql/common/type_visitors.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/function.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_builder.h"
#include "googlesql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "googlesql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "googlesql/resolved_ast/resolved_ast_visitor.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/resolved_ast/rewrite_utils.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

class AggedMeasureMarker : public ResolvedASTVisitor {
 public:
  explicit AggedMeasureMarker(MeasureCollector& measure_collector)
      : measure_collector_(measure_collector) {}

  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    if (IsMeasureAggFunction(node)) {
      GOOGLESQL_RET_CHECK_EQ(node->argument_list().size(), 1);
      const ResolvedExpr* arg = node->argument_list()[0].get();
      const MeasureType* measure_type = arg->type()->AsMeasure();
      GOOGLESQL_RET_CHECK(measure_type != nullptr);
      measure_collector_.MarkAgged(measure_type);
    }
    return DefaultVisit(node);
  }

 private:
  MeasureCollector& measure_collector_;
};

absl::Status MarkAggedMeasures(const ResolvedNode* resolved_ast,
                               MeasureCollector& measure_collector) {
  AggedMeasureMarker visitor(measure_collector);
  return resolved_ast->Accept(&visitor);
}

namespace {

// Helper class to:
// 1. Check if a type is or recursively contains an AGG'ed MeasureType.
// 2. Compute the replacement type by recursively replacing AGG'ed MeasureTypes
//    with their corresponding closure struct types.
//
// We can't use `TypeRewriter` directly because it creates new MeasureTypes, and
// the measure type rewriter rely on the original pointer identity of the
// MeasureType to work.
class MeasureTypeReplacer : public TypeRewriter {
 public:
  MeasureTypeReplacer(const MeasureCollector& measure_collector,
                      TypeFactory& type_factory)
      : TypeRewriter(type_factory), measure_collector_(measure_collector) {}
  MeasureTypeReplacer(const MeasureTypeReplacer&) = delete;
  MeasureTypeReplacer& operator=(const MeasureTypeReplacer&) = delete;

  // Returns true if `type` is an AGG'ed measure or a composite type that
  // transitively contains an AGG'ed measure.
  //
  // Also enforces the invariant that measures can only appear under struct
  // types, not other composite types like arrays or maps.
  absl::StatusOr<bool> IsOrContainsAggedMeasure(const Type* type) {
    auto it = is_or_contains_cache_.find(type);
    if (it != is_or_contains_cache_.end()) {
      return it->second;
    }

    MeasureTypeFinder visitor(measure_collector_);
    GOOGLESQL_RETURN_IF_ERROR(
        visitor.Visit(AnnotatedType(type, /*annotation_map=*/nullptr)));

    bool result = visitor.found_agged_measure();
    is_or_contains_cache_[type] = result;
    return result;
  }

  absl::StatusOr<AnnotatedType> PostVisit(
      AnnotatedType annotated_type) override {
    const Type* type = annotated_type.type;
    if (type->IsMeasureType()) {
      if (measure_collector_.IsAgged(type->AsMeasure())) {
        GOOGLESQL_ASSIGN_OR_RETURN(MeasureInfo info,
                         measure_collector_.GetMeasureInfo(type->AsMeasure()));
        return info.closure_struct_annotated_type;
      }
    }
    return annotated_type;
  }

 private:
  const MeasureCollector& measure_collector_;

  // Caches results of `IsOrContainsAggedMeasure`.
  absl::flat_hash_map<const Type*, bool> is_or_contains_cache_;

  // Visitor to check if a type is or contains AGG'ed measures, while validating
  // that struct is the only composite type that can contain measures.
  class MeasureTypeFinder : public TypeVisitor {
   public:
    explicit MeasureTypeFinder(const MeasureCollector& collector)
        : collector_(collector) {}
    MeasureTypeFinder(const MeasureTypeFinder&) = delete;
    MeasureTypeFinder& operator=(const MeasureTypeFinder&) = delete;

    absl::Status PostVisit(AnnotatedType annotated_type) override {
      const Type* type = annotated_type.type;
      if (type->IsMeasureType()) {
        found_measure_ = true;
        if (collector_.IsAgged(type->AsMeasure())) {
          found_agged_measure_ = true;
        }
        return absl::OkStatus();
      }

      if (!type->ComponentTypes().empty() && found_measure_) {
        // STRUCTs are the only container type that can contain measures.
        GOOGLESQL_RET_CHECK(type->IsStruct());
      }
      return absl::OkStatus();
    }

    bool found_agged_measure() const { return found_agged_measure_; }

   private:
    const MeasureCollector& collector_;
    // Whether we have encountered any measure type.
    bool found_measure_ = false;
    // Whether we have encountered any AGG'ed measure type.
    bool found_agged_measure_ = false;
  };
};

// Each GetRowField::type is unique within the resolved ast. When copying a
// ResolvedGetRowField node, we need to create a new type with the same
// underlying table to ensure that the copied node has a unique type.
class RowTypeAllocatingCopier : public ResolvedASTDeepCopyVisitor {
 public:
  explicit RowTypeAllocatingCopier(TypeFactory& type_factory)
      : type_factory_(type_factory) {}

 protected:
  absl::Status VisitResolvedGetRowField(
      const ResolvedGetRowField* node) override {
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> expr,
                     ProcessNode(node->expr()));
    const Type* type = node->type();
    if (type->IsRow()) {
      GOOGLESQL_RETURN_IF_ERROR(type_factory_.MakeRowType(
          type->AsRowType()->table(), type->AsRowType()->table_name(), &type));
    }
    PushNodeToStack(
        MakeResolvedGetRowField(type, std::move(expr), node->column()));
    return absl::OkStatus();
  }

 private:
  TypeFactory& type_factory_;
};

// Wraps the `input_scan` in a chain of `ProjectScan`s to evaluate the
// `pending_computed_columns` before aggregation. Chaining is required because
// a computed column may depend on another one in the list.
//
// Input:
// - `input_scan`: The scan to wrap. Must not be null.
// - `pending_computed_columns`: The computed columns to evaluate. The
//   depdencies of a computed column must be put before it in the list.
//
// Returns:
// - The wrapped scan, which is a ProjectScan (or chain of them) over the
//   original `input_scan`.
absl::StatusOr<std::unique_ptr<const ResolvedScan>>
WrapInputScanWithPendingComputedColumns(
    std::unique_ptr<const ResolvedScan> input_scan,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        pending_computed_columns) {
  GOOGLESQL_RET_CHECK(input_scan != nullptr);
  if (pending_computed_columns.empty()) {
    return input_scan;
  }

  std::unique_ptr<const ResolvedScan> current_scan = std::move(input_scan);
  for (auto& cc : pending_computed_columns) {
    ResolvedColumnList new_project_column_list = current_scan->column_list();
    new_project_column_list.push_back(cc->column());
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> expr_list;
    expr_list.push_back(std::move(cc));
    current_scan = MakeResolvedProjectScan(
        new_project_column_list, std::move(expr_list), std::move(current_scan));
  }
  return current_scan;
}

// Rewrites measure columns to closure struct columns and expands AGG(m) calls
// into expressions over constituent aggregates.
class MeasureColumnRewriter : public ResolvedASTRewriteVisitor {
 public:
  MeasureColumnRewriter(MeasureCollector& measure_collector,
                        const Function* any_value_fn,
                        FunctionCallBuilder& function_call_builder,
                        const LanguageOptions& language_options,
                        ColumnFactory& column_factory,
                        TypeFactory& type_factory,
                        AnnotationPropagator& annotation_propagator)
      : measure_collector_(measure_collector),
        any_value_fn_(any_value_fn),
        function_call_builder_(function_call_builder),
        language_options_(language_options),
        column_factory_(column_factory),
        type_factory_(type_factory),
        annotation_propagator_(annotation_propagator),
        measure_type_replacer_(measure_collector, type_factory) {}

 protected:
  // Creates a new stack frame to hold constituent aggregates produced by
  // rewriting AGG(m) calls under this AggregateScan. Also collects
  // columns that are outputs of AGG(m) calls.
  absl::Status PreVisitResolvedAggregateScan(
      const ResolvedAggregateScan& node) override {
    constituent_aggregates_stack_.emplace_back();
    pending_computed_columns_stack_.emplace_back();
    for (const auto& agg_col : node.aggregate_list()) {
      if (IsMeasureAggFunction(agg_col->expr())) {
        constituent_aggregates_stack_.back().agg_m_columns.insert(
            agg_col->column());
      }
    }
    return absl::OkStatus();
  }

  absl::Status PreVisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall& node) override {
    if (!IsMeasureAggFunction(&node)) {
      return absl::OkStatus();
    }
    // We do not support nested AGG calls currently.
    GOOGLESQL_RET_CHECK(active_measure_type_ == nullptr);
    GOOGLESQL_RET_CHECK(node.argument_list().size() == 1);
    const ResolvedExpr* arg = node.argument_list()[0].get();
    GOOGLESQL_RET_CHECK(arg->type()->IsMeasureType());
    active_measure_type_ = arg->type()->AsMeasure();
    return absl::OkStatus();
  }

  // If node is an AGG(m) call, rewrites it into an expression over
  // constituent aggregates, collects constituent aggregates in
  // `constituent_aggregates_stack_`, and returns the rewritten expression.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateFunctionCall(
      std::unique_ptr<const ResolvedAggregateFunctionCall> node) override {
    if (!IsMeasureAggFunction(node.get())) {
      return node;
    }
    GOOGLESQL_RET_CHECK(active_measure_type_ != nullptr);
    const MeasureType* measure_type = active_measure_type_;
    active_measure_type_ = nullptr;
    GOOGLESQL_ASSIGN_OR_RETURN(MeasureInfo measure_info,
                     measure_collector_.GetMeasureInfo(measure_type));

    const ResolvedExpr* arg = node->argument_list()[0].get();
    // `temp_closure_struct_ref` is a temporary column reference to the
    // created computed column. It does not need to persist because
    // `RewriteMeasureExpr` creates its own copy of it.
    std::unique_ptr<const ResolvedColumnRef> temp_closure_struct_ref;
    GOOGLESQL_ASSIGN_OR_RETURN(const ResolvedColumnRef* closure_struct_ref,
                     ComputeClosureStructRef(arg, temp_closure_struct_ref));
    GOOGLESQL_RET_CHECK(closure_struct_ref != nullptr);
    GOOGLESQL_RET_CHECK(closure_struct_ref->type() ==
              measure_info.closure_struct_annotated_type.type);

    GOOGLESQL_ASSIGN_OR_RETURN(
        RewriteMeasureExprResult result,
        RewriteMeasureExpr(measure_type, closure_struct_ref, measure_collector_,
                           any_value_fn_, function_call_builder_,
                           language_options_, column_factory_, type_factory_,
                           annotation_propagator_));
    for (auto& cc : result.closure_computed_columns) {
      pending_computed_columns_stack_.back().push_back(std::move(cc));
    }
    for (auto& agg : result.constituent_aggregate_list) {
      constituent_aggregates_stack_.back().constituent_aggregate_list.push_back(
          std::move(agg));
    }
    return std::move(result.rewritten_measure_expr);
  }

  // GetRowField access of a measure-typed column is a source for measure.
  //
  // Replace it with the corresponding closure struct expression constructed
  // by accessing the fields required for measure evaluation from the input
  // row.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedGetRowField(
      std::unique_ptr<const ResolvedGetRowField> node) override {
    if (!node->column()->GetType()->IsMeasureType()) {
      return node;
    }
    const MeasureType* measure_type = node->column()->GetType()->AsMeasure();
    if (!measure_collector_.IsAgged(measure_type)) {
      return node;
    }
    // `node->type()` has been rewritten to the closure struct type by
    // `PostVisitType` before we visit this node.
    GOOGLESQL_RET_CHECK(node->type()->IsStruct());
    GOOGLESQL_RET_CHECK_OK(measure_collector_.GetMeasureInfo(measure_type).status())
        << "Measure type " << measure_type->DebugString()
        << " is not collected";
    return BuildClosureExprFromRow(node->type()->AsStruct(), node->expr());
  }

  absl::StatusOr<ResolvedColumn> PostVisitResolvedColumn(
      const ResolvedColumn& column) override {
    GOOGLESQL_ASSIGN_OR_RETURN(
        bool contains_agged_measure,
        measure_type_replacer_.IsOrContainsAggedMeasure(column.type()));

    // If the column type is not or does not contain an AGG'ed measure, no
    // rewrite is needed.
    if (!contains_agged_measure) {
      return column;
    }

    // If the column type is an AGG'ed measure:
    // - If we do not reuse the original measure column id (i.e.,
    //   closure_columns has a value), we replace it with the closure column
    //   to propagate information (e.g., row identity columns) required for
    //   AGG(m) evaluation.
    // - Otherwise (when we reuse the original measure column id), we simply
    //   update the type of the column to the closure struct type.
    if (column.type()->IsMeasureType()) {
      const MeasureType* measure_type = column.type()->AsMeasure();
      GOOGLESQL_ASSIGN_OR_RETURN(MeasureInfo info,
                       measure_collector_.GetMeasureInfo(measure_type));

      if (info.closure_column.has_value()) {
        return measure_collector_.GetClosureColumn(column);
      }

      // Reuse the original measure column and update its type to the closure
      // struct type.
      return ReplaceMeasureColumnTypeToClosureStruct(column);
    }

    // Otherwise, the column type is a composite type that contains an AGG'ed
    // measure, e.g., the `s` column in:
    //
    // ```sql
    // FROM T
    // |> SELECT STRUCT(m) AS s
    // |> AGGREGATE AGG(s.m)
    // ```
    //
    // We need to fix its ResolvedColumn::type() because the AGG'ed measure type
    // has been rewritten to the closure struct type by `PostVisitType`, which
    // doesn't update the type of `ResolvedColumn`.
    return ReplaceMeasureColumnTypeToClosureStruct(column);
  }

  absl::StatusOr<AnnotatedType> PostVisitAnnotatedType(
      const Type* type,
      std::optional<const AnnotationMap*> annotation_map) override {
    AnnotatedType annotated_type(type, annotation_map.value_or(nullptr));
    return measure_type_replacer_.Visit(annotated_type);
  }

  // If AGG(m) calls are present under `node`, this function rewrites `node`
  // into a ProjectScan over an AggregateScan to capture the rewritten result
  // of AGG(m) by `PostVisitResolvedAggregateFunctionCall`.
  //
  // The overall algorithm is:
  //
  // If an AggregateScan contains AGG(m) in its aggregate list,
  // `PostVisitResolvedAggregateFunctionCall` rewrites AGG(m) into an
  // expression that computes AGG(m) from constituent aggregate functions
  // (e.g. SUM, COUNT), where the constituent aggregate function calls are
  // stored in `constituent_aggregates_stack_`.
  //
  // Then this function takes those constituent aggregate function calls and
  // adds them to `node`'s aggregate list. The rewritten AGG(m) expression is
  // added to the expr_list of the new ProjectScan layered on top of `node`.
  //
  // For example:
  //
  // AggregateScan
  // +-column_list=[agg1, agg2, key]
  // +-grouping_key_list=[key]
  // +-aggregate_list=
  // | +-agg1 := AGG(m),
  // | +-agg2 := COUNT(*)
  //
  // is rewritten to:
  //
  // ProjectScan
  // +-column_list=[agg1, agg2, key]
  // +-expr_list=
  // | +-agg1 := rewritten AGG(m) expression
  // +-input_scan=
  //   +-AggregateScan
  //     +-column_list=[constituent_agg1, constituent_agg2, agg2, key]
  //     +-grouping_key_list=[key]
  //     +-aggregate_list=
  //       +-constituent_agg1 := SUM(...)
  //       +-constituent_agg2 := COUNT(...)
  //       +-agg2 := COUNT(*)
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateScan(
      std::unique_ptr<const ResolvedAggregateScan> node) override {
    ConstituentAggregates agg_info =
        std::move(constituent_aggregates_stack_.back());
    constituent_aggregates_stack_.pop_back();

    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        pending_computed_columns =
            std::move(pending_computed_columns_stack_.back());
    pending_computed_columns_stack_.pop_back();

    // If `agg_info.agg_m_columns` is empty, it means no AGG(m) function calls
    // were found under this AggregateScan. No rewrite is needed.
    if (agg_info.agg_m_columns.empty()) {
      GOOGLESQL_RET_CHECK(pending_computed_columns.empty())
          << "Found pending computed columns for AggregateScan without AGG(m) "
             "calls";
      return node;
    }

    ResolvedColumnList original_column_list = node->column_list();
    // This will store AGG(m) expressions which are rewritten by
    // `PostVisitResolvedAggregateFunctionCall`. These expressions will become
    // computed columns in ProjectScan.
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        computed_columns_for_project_scan;

    ResolvedAggregateScanBuilder agg_scan_builder = ToBuilder(std::move(node));

    // If there are pending computed columns for the closure structs, wrap the
    // input scan in a (chain of) `ProjectScan`s to evaluate them before
    // aggregation.
    if (!pending_computed_columns.empty()) {
      GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> wrapped_input_scan,
                       WrapInputScanWithPendingComputedColumns(
                           agg_scan_builder.release_input_scan(),
                           std::move(pending_computed_columns)));
      agg_scan_builder.set_input_scan(std::move(wrapped_input_scan));
    }

    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        original_aggregate_list = agg_scan_builder.release_aggregate_list();

    // `rewritten_agg_scan_aggregate_list` will store aggregate expressions for
    // the rewritten AggregateScan. It includes non-AGG(m) aggregates and
    // constituent aggregates for AGG(m).
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        rewritten_agg_scan_aggregate_list;

    // Go through `node`'s aggregate list. If an aggregate expression is
    // AGG(m), it has been rewritten to an expression over constituent
    // aggregates by `PostVisitResolvedAggregateFunctionCall`. Move this
    // expression to `computed_columns_for_project_scan` which will be
    // computed by the ProjectScan. Otherwise, it's a non-AGG(m) aggregate
    // expression, simply move it to `rewritten_agg_scan_aggregate_list`.
    for (auto& agg_col : original_aggregate_list) {
      if (agg_info.agg_m_columns.contains(agg_col->column())) {
        // This column corresponds to an AGG(m) function call. The column
        // expression holds the rewritten expression for AGG(m), which should
        // be computed in ProjectScan.
        GOOGLESQL_RET_CHECK(agg_col->Is<ResolvedComputedColumn>());
        computed_columns_for_project_scan.push_back(absl::WrapUnique(
            agg_col.release()->GetAs<ResolvedComputedColumn>()));
      } else {
        rewritten_agg_scan_aggregate_list.push_back(std::move(agg_col));
      }
    }

    // Build `rewritten_agg_scan_column_list` which is output column list of
    // the rewritten AggregateScan. It should contain all columns from
    // `original_column_list` except for AGG(m) columns, plus columns for
    // constituent aggregates.
    std::vector<ResolvedColumn> rewritten_agg_scan_column_list;
    for (const auto& col : original_column_list) {
      if (!agg_info.agg_m_columns.contains(col)) {
        rewritten_agg_scan_column_list.push_back(col);
      }
    }
    for (auto& constituent_agg : agg_info.constituent_aggregate_list) {
      rewritten_agg_scan_column_list.push_back(constituent_agg->column());
      rewritten_agg_scan_aggregate_list.push_back(std::move(constituent_agg));
    }

    // Build the rewritten AggregateScan with the new
    // `rewritten_agg_scan_column_list` and `rewritten_agg_scan_aggregate_list`.
    agg_scan_builder.set_aggregate_list(
        std::move(rewritten_agg_scan_aggregate_list));
    agg_scan_builder.set_column_list(std::move(rewritten_agg_scan_column_list));
    GOOGLESQL_ASSIGN_OR_RETURN(auto rewritten_agg_scan,
                     std::move(agg_scan_builder).Build());

    // Build and return a ProjectScan on top of rewritten AggregateScan.
    return MakeResolvedProjectScan(original_column_list,
                                   std::move(computed_columns_for_project_scan),
                                   std::move(rewritten_agg_scan));
  }

 private:
  // Updates the type of `column` to the closure struct type.
  absl::StatusOr<ResolvedColumn> ReplaceMeasureColumnTypeToClosureStruct(
      const ResolvedColumn& column) {
    GOOGLESQL_ASSIGN_OR_RETURN(AnnotatedType new_annotated_type,
                     measure_type_replacer_.Visit(column.annotated_type()));
    return ResolvedColumn(column.column_id(), column.table_name_id(),
                          column.name_id(), new_annotated_type);
  }

  // Builds the closure struct expression for a measure `m` which comes from
  // the RowType expression `row_expr`.
  //
  // - `closure_struct_type`: The closure struct type of the measure `m`.
  // - `row_expr`: The RowType expression that contains the measure `m`.
  //
  // To handle potential NULL RowType values, we return `IF(row IS NULL,
  // ClosureStruct(NULL), ClosureStruct(row))`.
  absl::StatusOr<std::unique_ptr<const ResolvedExpr>> BuildClosureExprFromRow(
      const StructType* closure_struct_type, const ResolvedExpr* row_expr) {
    GOOGLESQL_RET_CHECK(closure_struct_type != nullptr);
    GOOGLESQL_RET_CHECK(row_expr != nullptr);
    GOOGLESQL_RET_CHECK(row_expr->type()->IsRow());

    GOOGLESQL_RET_CHECK_EQ(closure_struct_type->num_fields(), 2);
    const Type* ref_type =
        closure_struct_type->field(kReferencedColumnsFieldIndex).type;
    GOOGLESQL_RET_CHECK(ref_type->IsStruct()) << ref_type->DebugString();
    const StructType* ref_struct_type = ref_type->AsStruct();

    const Type* key_type =
        closure_struct_type->field(kKeyColumnsFieldIndex).type;
    GOOGLESQL_RET_CHECK(key_type->IsStruct()) << key_type->DebugString();
    const StructType* key_struct_type = key_type->AsStruct();

    GOOGLESQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> ref_struct,
        BuildStructFromRowFields(ref_struct_type, row_expr, type_factory_));
    GOOGLESQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> key_struct,
        BuildStructFromRowFields(key_struct_type, row_expr, type_factory_));

    std::vector<std::unique_ptr<const ResolvedExpr>> closure_struct_fields;
    closure_struct_fields.resize(2);
    closure_struct_fields[kReferencedColumnsFieldIndex] = std::move(ref_struct);
    closure_struct_fields[kKeyColumnsFieldIndex] = std::move(key_struct);

    std::unique_ptr<ResolvedMakeStruct> closure_struct = MakeResolvedMakeStruct(
        closure_struct_type, std::move(closure_struct_fields));
    GOOGLESQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
        nullptr, closure_struct.get()));

    // Since `row_expr` can be copied multiple times (here for the null check,
    // and later for field extraction), we use `RowTypeAllocatingCopier` to
    // ensure each copied `ResolvedGetRowField` gets a unique `RowType` pointer,
    // as required by the validator.
    RowTypeAllocatingCopier copier(type_factory_);
    GOOGLESQL_RETURN_IF_ERROR(row_expr->Accept(&copier));
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> row_expr_copy,
                     copier.ConsumeRootNode<ResolvedExpr>());
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> row_is_null,
                     function_call_builder_.IsNull(std::move(row_expr_copy)));

    std::unique_ptr<ResolvedLiteral> null_closure_struct = MakeResolvedLiteral(
        closure_struct_type, Value::Null(closure_struct_type));

    // Return NULL if `row_expr` is NULL. This is required to correctly handle
    // measures propagating past OUTER JOINs, where unmatched rows produce a
    // NULL row, and the closure struct must evaluate to NULL rather than a
    // non-NULL struct with NULL fields. This ensures that the unmatched rows
    // are discarded when evaluating the measure (due to `closure IS NOT NULL`
    // filtering injected by the rewriter).
    //
    // Example:
    //   SELECT key, AGG(t2.measure_sum_price)
    //   FROM t1 LEFT JOIN t2 ON t1.key = t2.key;
    //
    // For unmatched rows, the `t2` row variable is NULL.
    //
    // If we built the closure struct from `t2`'s columns directly, we would
    // get `STRUCT(t2.price) -> STRUCT(NULL)` which is NOT NULL.
    //
    // The rewriter injects `where_expr = closure IS NOT NULL` to filter out
    // unmatched rows. If the closure is `STRUCT(NULL)`, the filter
    // `STRUCT(NULL) IS NOT NULL` evaluates to TRUE, so the unmatched row is
    // NOT filtered out.
    //
    // By wrapping it in `IF(t2 IS NULL, NULL, STRUCT(t2.price))`, the closure
    // evaluates to `NULL` (struct), and `NULL IS NOT NULL` is FALSE, correctly
    // filtering out the unmatched row.
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedFunctionCall> if_call,
                     function_call_builder_.If(std::move(row_is_null),
                                               std::move(null_closure_struct),
                                               std::move(closure_struct)));
    GOOGLESQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
        nullptr, if_call.get()));
    return if_call;
  }

  // Builds the closure struct expression for a measure `m` which comes from
  // the RowType expression `row_expr`.
  //
  // - `target_struct_type`: The closure struct type of `m`.
  //   The struct type contains two top-level fields:
  //   - `referenced_columns`: A struct containing the referenced columns, where
  //     each field name is the name of the reference column.
  //   - `key_columns`: A struct containing the key columns.
  //
  // - `row_expr`: The RowType expression that contains the measure `m`.
  absl::StatusOr<std::unique_ptr<const ResolvedExpr>> BuildStructFromRowFields(
      const StructType* target_struct_type, const ResolvedExpr* row_expr,
      TypeFactory& type_factory) {
    GOOGLESQL_RET_CHECK(row_expr->type()->IsRow());
    const Table* table = row_expr->type()->AsRowType()->table();
    GOOGLESQL_RET_CHECK(table != nullptr);

    std::vector<std::unique_ptr<const ResolvedExpr>> fields;
    fields.reserve(target_struct_type->num_fields());
    for (const auto& field : target_struct_type->fields()) {
      const Column* column = table->FindColumnByName(field.name);
      GOOGLESQL_RET_CHECK(column != nullptr)
          << "Column " << field.name << " not found in row type table";

      if (column->GetType()->IsMeasureType()) {
        // This dependency column is a measure column, build its closure
        // expression recursively.
        GOOGLESQL_RET_CHECK(field.type->IsStruct());
        GOOGLESQL_ASSIGN_OR_RETURN(
            std::unique_ptr<const ResolvedExpr> closure_expr,
            BuildClosureExprFromRow(field.type->AsStruct(), row_expr));
        fields.push_back(std::move(closure_expr));
      } else {
        // The dependency column is non-measure, get it from the row expression
        // directly.
        //
        // Since `row_expr` is copied multiple times (once for each non-measure
        // column), we use `RowTypeAllocatingCopier` to ensure each copied
        // `ResolvedGetRowField` gets a unique `RowType` pointer, as required by
        // the validator.
        RowTypeAllocatingCopier copier(type_factory);
        GOOGLESQL_RETURN_IF_ERROR(row_expr->Accept(&copier));
        GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> row_expr_copy,
                         copier.ConsumeRootNode<ResolvedExpr>());
        const Type* type = column->GetType();
        if (type->IsRow()) {
          GOOGLESQL_RETURN_IF_ERROR(
              type_factory.MakeRowType(type->AsRowType()->table(),
                                       type->AsRowType()->table_name(), &type));
        }
        fields.push_back(
            MakeResolvedGetRowField(type, std::move(row_expr_copy), column));
      }
    }
    std::unique_ptr<ResolvedMakeStruct> make_struct =
        MakeResolvedMakeStruct(target_struct_type, std::move(fields));
    GOOGLESQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
        nullptr, make_struct.get()));
    return make_struct;
  }

  MeasureCollector& measure_collector_;
  const Function* any_value_fn_;
  FunctionCallBuilder& function_call_builder_;
  const LanguageOptions& language_options_;
  ColumnFactory& column_factory_;
  TypeFactory& type_factory_;
  AnnotationPropagator& annotation_propagator_;
  MeasureTypeReplacer measure_type_replacer_;

  // The measure type of argument of the current AGG call being visited.
  const MeasureType* active_measure_type_ = nullptr;

  // Normalizes the input of AGG to a column reference. If `arg` is not a
  // ResolvedColumnRef, creates a ResolvedComputedColumn to wrap it and returns
  // a temporary ResolvedColumnRef to the computed column.
  absl::StatusOr<const ResolvedColumnRef*> ComputeClosureStructRef(
      const ResolvedExpr* arg,
      std::unique_ptr<const ResolvedColumnRef>& temp_closure_struct_ref) {
    if (!arg->Is<ResolvedColumnRef>()) {
      ResolvedColumn new_col = column_factory_.MakeCol("$aggregate", "agg_arg",
                                                       arg->annotated_type());
      GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> arg_copy,
                       ResolvedASTDeepCopyVisitor::Copy(arg));
      auto computed_col =
          MakeResolvedComputedColumn(new_col, std::move(arg_copy));
      pending_computed_columns_stack_.back().push_back(std::move(computed_col));

      temp_closure_struct_ref = MakeResolvedColumnRef(
          new_col.type(), new_col,
          // The created ResolvedComputedColumn will be added to a ProjectScan
          // that wraps the `input_scan` of the `AggregateScan`, so for the AGG
          // call that column is always local.
          /*is_correlated=*/false);
      return temp_closure_struct_ref.get();
    }
    return arg->GetAs<ResolvedColumnRef>();
  }

  // Holds information about rewritten AGG(m) calls under an AggregateScan.
  struct ConstituentAggregates {
    // Constituent aggregates from rewritten AGG(m) calls.
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        constituent_aggregate_list;
    // Columns that are outputs of AGG(m) calls.
    absl::flat_hash_set<ResolvedColumn> agg_m_columns;
  };
  // A stack to hold ConstituentAggregates for nested AggregateScans.
  std::vector<ConstituentAggregates> constituent_aggregates_stack_;

  // The top of the stack is the list of pending computed columns to be added to
  // the `input_scan` of the current `AggregateScan`. These computed columns are
  // created for each `AGG(expr)` calls of the current AggregateScan where
  // `expr` is not a column reference. If a computed column depends on another
  // column, the dependency column is guaranteed to appear before it.
  //
  // This allows us to rewrite `AGG(expr)` into `AGG(m)` where m is a single
  // column reference.
  std::vector<std::vector<std::unique_ptr<const ResolvedComputedColumn>>>
      pending_computed_columns_stack_;
};

}  // namespace

absl::StatusOr<std::unique_ptr<const ResolvedNode>> RewriteMeasureColumns(
    std::unique_ptr<const ResolvedNode> resolved_ast,
    MeasureCollector& measure_collector, const Function* any_value_fn,
    FunctionCallBuilder& function_call_builder,
    const LanguageOptions& language_options, ColumnFactory& column_factory,
    TypeFactory& type_factory, AnnotationPropagator& annotation_propagator) {
  MeasureColumnRewriter rewriter(
      measure_collector, any_value_fn, function_call_builder, language_options,
      column_factory, type_factory, annotation_propagator);
  return rewriter.VisitAll(std::move(resolved_ast));
}

}  // namespace googlesql
