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

#include "zetasql/analyzer/rewriters/variadic_function_signature_expander.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// SQL templates for rewriting variadic MAP functions.
constexpr char kMapInsertSql[] = R"sql(
IF(
  map_arg IS NULL,
  NULL,
  MAP_FROM_ARRAY(
    ARRAY(
      SELECT
        CASE
          -- If a key arg appears more than once
          WHEN SUM(entries.is_new) > 1 THEN
            ERROR(
              FORMAT(
                'Key provided more than once as argument: %T',
                 entries.key
               )
             )
          -- If a key to insert already exists in the map
          WHEN SUM(entries.is_new) = 1 AND COUNT(*) > 1 THEN
            ERROR(FORMAT('Key already exists in map: %T', entries.key))
          -- Otherwise, insert the new <key,value> pair.
          ELSE
            STRUCT(entries.key, ANY_VALUE(entries.value HAVING MAX entries.is_new))
        END
      FROM (
        -- Entries from the original map
        SELECT t.key, t.value, 0 AS is_new
        FROM UNNEST(MAP_ENTRIES_UNSORTED(map_arg)) AS t
        UNION ALL
        -- Entries to insert from the arguments
        SELECT key, value, 1 AS is_new
        FROM ( $NEW_ENTRIES_SUBQUERY )
      ) AS entries
      GROUP BY entries.key
    )
  )
)
)sql";

constexpr char kMapInsertOrReplaceSql[] = R"sql(
IF(
  map_arg IS NULL,
  NULL,
  MAP_FROM_ARRAY(
    ARRAY(
      SELECT
        CASE
          -- If a key arg appears more than once
          WHEN SUM(entries.is_new) > 1 THEN
            ERROR(
              FORMAT(
                'Key provided more than once as argument: %T',
                entries.key
              )
            )
          ELSE
            STRUCT(
              entries.key,
              -- Prioritise keeping new values
              ANY_VALUE(entries.value HAVING MAX entries.is_new)
            )
        END
      FROM (
        -- Entries from the original map
        SELECT t.key, t.value, 0 AS is_new
        FROM UNNEST(MAP_ENTRIES_UNSORTED(map_arg)) AS t
        UNION ALL
        -- Entries to insert or replace from the arguments
        SELECT key, value, 1 AS is_new
        FROM ( $NEW_ENTRIES_SUBQUERY )
      ) AS entries
      GROUP BY entries.key
    )
  )
)
)sql";

constexpr char kMapReplaceKvPairsSql[] = R"sql(
IF(
  map_arg IS NULL,
  NULL,
  MAP_FROM_ARRAY(
    ARRAY(
      SELECT
        CASE
          -- If a key arg appears more than once
          WHEN SUM(entries.is_new) > 1 THEN
            ERROR(
              FORMAT(
                'Key provided more than once as argument: %T',
                entries.key
              )
            )
          -- If a key to update is not in the map
          WHEN MIN(entries.is_new) = 1 THEN
            ERROR(FORMAT('Key does not exist in map: %T', entries.key))
          ELSE
            -- If the key's value is to be replaced, update it (i.e., MAX(is_new) = 1)
            -- Otherwise, keep the existing value (i.e., MAX(is_new) = 0)
            STRUCT(entries.key, ANY_VALUE(entries.value HAVING MAX entries.is_new))
        END
      FROM (
        -- Entries from the original map
        SELECT t.key, t.value, 0 AS is_new
        FROM UNNEST(MAP_ENTRIES_UNSORTED(map_arg)) AS t
        UNION ALL
        -- Entries to replace from the arguments
        SELECT key, value, 1 AS is_new
        FROM ( $NEW_ENTRIES_SUBQUERY )
      ) AS entries
      GROUP BY entries.key
    )
  )
)
)sql";

constexpr char kMapReplaceKRepeatedVLambdaSql[] = R"sql(
IF(
  map_arg IS NULL,
  NULL,
  MAP_FROM_ARRAY(
    ARRAY(
      SELECT
        CASE
          -- If a key arg appears more than once
          WHEN SUM(entries.is_new) > 1 THEN
            ERROR(
              FORMAT(
                'Key provided more than once as argument: %T',
                entries.key
              )
            )
          -- If a key to update is not in the map
          WHEN MIN(entries.is_new) = 1 THEN
            ERROR(FORMAT('Key does not exist in map: %T', entries.key))
          ELSE
            STRUCT(
              entries.key,
              IF(
                MAX(entries.is_new) = 1,
                -- If the key's value is to be replaced, update it with the lambda
                updater_lambda(ANY_VALUE(entries.value HAVING MIN entries.is_new)),
                -- Otherwise, all entries.is_new are 0, so keep the original value
                ANY_VALUE(entries.value)
              )
            )
        END
      FROM (
        -- Entries from the original map
        SELECT t.key, t.value, 0 AS is_new
        FROM UNNEST(MAP_ENTRIES_UNSORTED(map_arg)) AS t
        UNION ALL
        -- Keys to update from the arguments
        SELECT key, NULL AS value, 1 AS is_new
        FROM ($KEYS_TO_UPDATE_SUBQUERY)
      ) AS entries
      GROUP BY entries.key
    )
  )
)
)sql";

constexpr char kMapDeleteSql[] = R"sql(
IF(
  map_arg IS NULL,
  NULL,
  MAP_FROM_ARRAY(
    ARRAY(
      SELECT STRUCT(entries.key, ANY_VALUE(entries.value))
      FROM (
        SELECT t.key, t.value, 0 AS is_delete_key
        FROM UNNEST(MAP_ENTRIES_UNSORTED(map_arg)) AS t
        UNION ALL
        SELECT key, NULL AS value, 1 AS is_delete_key
        FROM ($KEYS_TO_DELETE_SUBQUERY)
      ) AS entries
      GROUP BY entries.key
      HAVING MAX(entries.is_delete_key) = 0
    )
  )
)
)sql";

class VariadicFunctionSignatureExpanderVisitor
    : public ResolvedASTRewriteVisitor {
 private:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedFunctionCall(
      std::unique_ptr<const ResolvedFunctionCall> node) override {
    const FunctionSignature& signature = node->signature();
    std::vector<std::pair<std::string, const ResolvedExpr*>> labeled_args;
    const auto& original_args = node->argument_list();
    std::string generated_sql;
    std::vector<FunctionArgumentType> new_arg_types;

    switch (signature.context_id()) {
      case FN_MAP_INSERT:
      case FN_MAP_INSERT_OR_REPLACE:
      case FN_MAP_REPLACE_KV_PAIRS: {
        ZETASQL_RET_CHECK_GE(node->argument_list_size(), 3)
            << node->function()->Name()
            << " requires at least 3 arguments: a map, and at least one "
               "key/value pair.";
        ZETASQL_RET_CHECK_EQ((node->argument_list_size() - 1) % 2, 0)
            << node->function()->Name()
            << " requires an even number of key/value pair arguments.";
        labeled_args.push_back({"map_arg", original_args[0].get()});
        for (size_t i = 1; i < original_args.size(); ++i) {
          size_t pair_idx = (i - 1) / 2 + 1;
          std::string arg_name = ((i - 1) % 2 == 0)
                                     ? absl::StrCat("key_", pair_idx)
                                     : absl::StrCat("val_", pair_idx);
          labeled_args.push_back({arg_name, original_args[i].get()});
        }

        std::vector<std::string> union_clauses;
        for (size_t i = 1; i < labeled_args.size(); i += 2) {
          const std::string& key_name = labeled_args[i].first;
          const std::string& val_name = labeled_args[i + 1].first;
          union_clauses.push_back(absl::Substitute(
              "SELECT $0 AS key, $1 AS value", key_name, val_name));
        }
        std::string new_entries_sql =
            absl::StrJoin(union_clauses, " UNION ALL ");

        const char* sql_template;
        switch (signature.context_id()) {
          case FN_MAP_INSERT:
            sql_template = kMapInsertSql;
            break;
          case FN_MAP_INSERT_OR_REPLACE:
            sql_template = kMapInsertOrReplaceSql;
            break;
          case FN_MAP_REPLACE_KV_PAIRS:
            sql_template = kMapReplaceKvPairsSql;
            break;
        }
        generated_sql = absl::StrReplaceAll(
            sql_template, {{"$NEW_ENTRIES_SUBQUERY", new_entries_sql}});
        break;
      }
      case FN_MAP_REPLACE_K_REPEATED_V_LAMBDA: {
        ZETASQL_RET_CHECK_GE(node->generic_argument_list_size(), 3)
            << node->function()->Name()
            << " requires at least 3 arguments: a map, one or more keys, and a "
               "lambda.";

        std::vector<std::pair<std::string, const ResolvedFunctionArgument*>>
            generic_labeled_args;
        const auto& generic_args = node->generic_argument_list();
        generic_labeled_args.push_back({"map_arg", generic_args[0].get()});
        for (size_t i = 1; i < generic_args.size() - 1; ++i) {
          std::string arg_name = absl::StrCat("key_", i);
          generic_labeled_args.push_back({arg_name, generic_args[i].get()});
        }
        generic_labeled_args.push_back(
            {"updater_lambda", generic_args.back().get()});

        std::vector<std::string> union_clauses;
        for (size_t i = 1; i < generic_labeled_args.size() - 1; ++i) {
          const std::string& key_name = generic_labeled_args[i].first;
          union_clauses.push_back(
              absl::Substitute("SELECT $0 AS key", key_name));
        }
        std::string keys_to_update_sql =
            absl::StrJoin(union_clauses, " UNION ALL ");

        generated_sql = absl::StrReplaceAll(
            kMapReplaceKRepeatedVLambdaSql,
            {{"$KEYS_TO_UPDATE_SUBQUERY", keys_to_update_sql}});

        for (const auto& pair : generic_labeled_args) {
          const ResolvedFunctionArgument* arg = pair.second;
          FunctionArgumentTypeOptions options;
          options.set_argument_name(pair.first, zetasql::kPositionalOrNamed);
          if (arg->expr() != nullptr) {
            new_arg_types.push_back(FunctionArgumentType(
                arg->expr()->type(), options, /*num_occurrences=*/1));
          } else if (arg->inline_lambda() != nullptr) {
            const zetasql::ResolvedInlineLambda* lambda =
                arg->inline_lambda();

            zetasql::FunctionArgumentTypeList lambda_arg_types;
            for (const zetasql::ResolvedColumn& arg_col :
                 lambda->argument_list()) {
              // Each argument of the lambda itself is a simple type.
              lambda_arg_types.push_back(
                  zetasql::FunctionArgumentType(arg_col.type(),
                                                  /*num_occurrences=*/1));
            }

            const zetasql::Type* body_type = lambda->body()->type();
            zetasql::FunctionArgumentType lambda_body_type(
                body_type, /*num_occurrences=*/1);

            new_arg_types.push_back(zetasql::FunctionArgumentType::Lambda(
                std::move(lambda_arg_types), std::move(lambda_body_type),
                options));
          } else {
            ZETASQL_RET_CHECK_FAIL()
                << "Unsupported argument type in " << node->function()->Name();
          }
        }
        break;
      }
      case FN_MAP_DELETE: {
        ZETASQL_RET_CHECK_GE(node->argument_list_size(), 2)
            << node->function()->Name()
            << " requires at least 2 arguments: a map, and one or "
               "more keys to delete.";
        labeled_args.push_back({"map_arg", original_args[0].get()});
        for (size_t i = 1; i < original_args.size(); ++i) {
          std::string arg_name = absl::StrCat("key_", i);
          labeled_args.push_back({arg_name, original_args[i].get()});
        }

        std::vector<std::string> union_clauses;
        for (size_t i = 1; i < labeled_args.size(); ++i) {
          const std::string& key_name = labeled_args[i].first;
          union_clauses.push_back(
              absl::Substitute("SELECT $0 AS key", key_name));
        }
        std::string keys_to_delete_sql =
            absl::StrJoin(union_clauses, " UNION ALL ");
        generated_sql = absl::StrReplaceAll(
            kMapDeleteSql, {{"$KEYS_TO_DELETE_SUBQUERY", keys_to_delete_sql}});
        break;
      }
      default:
        return node;
    }

    if (new_arg_types.empty()) {
      for (const auto& pair : labeled_args) {
        const std::string& arg_name = pair.first;
        const ResolvedExpr* expr = pair.second;
        FunctionArgumentTypeOptions options;
        options.set_argument_name(arg_name, zetasql::kPositionalOrNamed);
        new_arg_types.push_back(
            FunctionArgumentType(expr->type(), options, /*num_occurrences=*/1));
      }
    }

    FunctionSignature current_sig = node->signature();
    FunctionSignatureOptions sig_options = current_sig.options();

    FunctionSignatureRewriteOptions rewrite_options;
    rewrite_options.set_sql(generated_sql);
    rewrite_options.set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER);
    rewrite_options.set_enabled(true);
    sig_options.set_rewrite_options(rewrite_options);

    FunctionSignature new_sig(current_sig.result_type(), new_arg_types,
                              current_sig.context_id(), sig_options);

    ResolvedFunctionCallBuilder builder = ToBuilder(std::move(node));
    builder.set_signature(new_sig);
    ZETASQL_ASSIGN_OR_RETURN(auto new_node, std::move(builder).Build());
    return new_node;
  }
};

}  // namespace

absl::StatusOr<std::unique_ptr<const ResolvedNode>>
VariadicFunctionSignatureExpander::Rewrite(
    const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
    Catalog& catalog, TypeFactory& type_factory,
    AnalyzerOutputProperties& analyzer_output_properties) const {
  VariadicFunctionSignatureExpanderVisitor visitor;
  return visitor.VisitAll(std::move(input));
}

const Rewriter* GetVariadicFunctionSignatureExpander() {
  static const auto* const kRewriter = new VariadicFunctionSignatureExpander();
  return kRewriter;
}

}  // namespace zetasql
