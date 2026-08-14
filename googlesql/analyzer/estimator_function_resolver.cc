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

#include "googlesql/analyzer/estimator_function_resolver.h"

#include <memory>
#include <utility>
#include <vector>

#include "googlesql/analyzer/name_scope.h"
#include "googlesql/analyzer/query_resolver_helper.h"
#include "googlesql/analyzer/resolver.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_builder.h"
#include "googlesql/resolved_ast/resolved_ast_enums.pb.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/strings/str_cat.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

STATIC_IDSTRING(kEstimatorId, "$estimator");

EstimatorFunctionResolver::EstimatorFunctionResolver(Resolver* resolver)
    : resolver_(resolver) {}

absl::Status
EstimatorFunctionResolver::ResolveWithinBoundsAndCreateEstimatorColumn(
    const ASTEstimatorFunctionCall* ast_estimator_function_call,
    std::unique_ptr<const ResolvedFunctionCall> resolved_function_call,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr_out) {
  // We pass `expr_resolution_info->name_scope->previous_scope()` because the
  // current scope extends `previous_scope()` with PARTITION BY aliases and
  // `aligned_timestamp`, neither of which should be accessible within the
  // WITHIN clause.
  GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedWithinBounds> within_bounds,
                   resolver_->ResolveWithinBounds(
                       ast_estimator_function_call->within_clause(),
                       expr_resolution_info->name_scope->previous_scope()));

  GOOGLESQL_RET_CHECK(resolved_function_call->function() != nullptr);
  GOOGLESQL_RET_CHECK(resolved_function_call->function()->SupportsWithinClause());
  auto builder = ToBuilder(std::move(resolved_function_call));

  std::unique_ptr<ResolvedEstimatorFunctionCall>
      resolved_estimator_function_call = MakeResolvedEstimatorFunctionCall(
          builder.type(), builder.function(), builder.signature(),
          builder.release_argument_list(),
          builder.release_generic_argument_list(), builder.error_mode(),
          /*distinct=*/false,
          ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING,
          /*where_expr=*/nullptr, std::move(within_bounds));

  // Modifiers are not supported in estimator function call
  GOOGLESQL_RET_CHECK(!resolved_estimator_function_call->distinct());
  GOOGLESQL_RET_CHECK(resolved_estimator_function_call->null_handling_modifier() ==
            ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING);
  GOOGLESQL_RET_CHECK(resolved_estimator_function_call->where_expr() == nullptr);

  if (ast_estimator_function_call->function() != nullptr) {
    resolver_->MaybeRecordParseLocation(ast_estimator_function_call,
                                        resolved_estimator_function_call.get());
  }
  GOOGLESQL_RETURN_IF_ERROR(resolver_->MaybeResolveCollationForFunctionCallBase(
      /*error_location=*/ast_estimator_function_call,
      resolved_estimator_function_call.get()));
  GOOGLESQL_RETURN_IF_ERROR(resolver_->CheckAndPropagateAnnotations(
      /*error_node=*/ast_estimator_function_call,
      resolved_estimator_function_call.get()));

  IdString alias = resolver_->MakeIdString(
      absl::StrCat("$estimator", 1 + estimator_computed_columns_.size()));

  const ResolvedColumn resolved_column(
      resolver_->AllocateColumnId(), kEstimatorId, alias,
      resolved_estimator_function_call->annotated_type());

  estimator_computed_columns_.emplace_back(MakeResolvedComputedColumn(
      resolved_column, std::move(resolved_estimator_function_call)));

  *resolved_expr_out = resolver_->MakeColumnRef(resolved_column);

  return absl::OkStatus();
}

}  // namespace googlesql
