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

#ifndef ZETASQL_COMMON_MEASURE_UTILS_H_
#define ZETASQL_COMMON_MEASURE_UTILS_H_

#include <memory>
#include <vector>

#include "zetasql/analyzer/name_scope.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {

// Helper function that is used to determine a type is or contains in its
// nesting structure a MEASURE type.
bool IsOrContainsMeasure(const Type* type);

// Returns a user facing error if `name_list` contains a column or pseudo-column
// with a type that is or contains a MEASURE. `location`, `operation_name`, and
// `product_mode` are used to construct the error message. This function is
// called by the resolver.
absl::Status EnsureNoMeasuresInNameList(NameListPtr name_list,
                                        const ASTNode* location,
                                        absl::string_view operation_name,
                                        ProductMode product_mode);

// Extract top-level aggregates from `measure_expr` and add them to
// `extracted_aggregates`.
//
// For instance, given a measure expression like: SUM(value) / MIN(value), the
// the original ResolvedAST will look like:
//
// FunctionCall(ZetaSQL:$divide(DOUBLE, DOUBLE) -> DOUBLE)
// +-Cast(INT64 -> DOUBLE)
// | +-AggregateFunctionCall(ZetaSQL:sum(INT64) -> INT64)
// |   +-ExpressionColumn(type=INT64, name="value")
// +-AggregateFunctionCall(ZetaSQL:avg(INT64) -> DOUBLE)
//   +-ExpressionColumn(type=INT64, name="value")

// Post-rewrite, the ResolvedAST will look like:
//
// FunctionCall(ZetaSQL:$divide(DOUBLE, DOUBLE) -> DOUBLE)
// +-Cast(INT64 -> DOUBLE)
// | +-ColumnRef(type=INT64, column=<sum_computed_column_id>)
// +-ColumnRef(type=INT64, column=<avg_computed_column_id>)
//
// , where <sum_computed_column_id> and <avg_computed_column_id> are column ids
// for the ResolvedComputedColumns populated in `extracted_aggregates`.
absl::StatusOr<std::unique_ptr<const ResolvedExpr>> ExtractTopLevelAggregates(
    std::unique_ptr<const ResolvedExpr> measure_expr,
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
        extracted_aggregates,
    ColumnFactory& column_factory);

// Certain scans (e.g. ResolvedAggregateScan) are not allowed to emit MEASURE
// typed columns. Return an internal error if `scan` is not allowed to emit
// MEASURE types and a MEASURE typed column is found in its `column_list`.
absl::Status ValidateScanCanEmitMeasureColumns(const ResolvedScan* scan);

// Validates that `resolved_expr` is a valid measure expression.
absl::Status ValidateMeasureExpression(absl::string_view measure_expr,
                                       const ResolvedExpr& resolved_expr);

}  // namespace zetasql

#endif  // ZETASQL_COMMON_MEASURE_UTILS_H_
