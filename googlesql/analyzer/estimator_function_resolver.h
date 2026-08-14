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

#ifndef GOOGLESQL_ANALYZER_ESTIMATOR_FUNCTION_RESOLVER_H_
#define GOOGLESQL_ANALYZER_ESTIMATOR_FUNCTION_RESOLVER_H_

#include <memory>
#include <utility>
#include <vector>

#include "googlesql/analyzer/name_scope.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"

namespace googlesql {

class Resolver;
struct ExprResolutionInfo;

// EstimatorFunctionResolver is a query-scoped stateful resolver that resolves
// WITHIN clauses and manages the creation of estimator function columns during
// analysis of ALIGN operator.
//
// A ResolvedColumn is created to store the output of each estimator function
// call. This is done because the ALIGN scan computes only the estimator
// function, producing the result as a column. By replacing the function call at
// the expression call site with a ResolvedColumnRef to this column, any
// surrounding scalar expressions (e.g., arithmetic or logical operations
// wrapping the estimator) can be cleanly resolved as a post-projection on top
// of the ALIGN scan.
class EstimatorFunctionResolver {
 public:
  explicit EstimatorFunctionResolver(Resolver* resolver);
  EstimatorFunctionResolver(const EstimatorFunctionResolver&) = delete;
  EstimatorFunctionResolver& operator=(const EstimatorFunctionResolver&) =
      delete;

  ~EstimatorFunctionResolver() = default;

  absl::Status ResolveWithinBoundsAndCreateEstimatorColumn(
      const ASTEstimatorFunctionCall* ast_estimator_function_call,
      std::unique_ptr<const ResolvedFunctionCall> resolved_function_call,
      ExprResolutionInfo* expr_resolution_info,
      std::unique_ptr<const ResolvedExpr>* resolved_expr_out);

  const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
  estimator_computed_columns() const {
    return estimator_computed_columns_;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  ReleaseEstimatorColumns() {
    return std::move(estimator_computed_columns_);
  }

 private:
  Resolver* resolver_;  // Not owned.

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      estimator_computed_columns_;
};

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_ESTIMATOR_FUNCTION_RESOLVER_H_
