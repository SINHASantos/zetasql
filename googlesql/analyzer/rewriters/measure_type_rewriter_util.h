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

#ifndef GOOGLESQL_ANALYZER_REWRITERS_MEASURE_TYPE_REWRITER_UTIL_H_
#define GOOGLESQL_ANALYZER_REWRITERS_MEASURE_TYPE_REWRITER_UTIL_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/analyzer/rewriters/measure_dependency_graph.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/resolved_ast/rewrite_utils.h"
#include "googlesql/base/case.h"
#include "absl/container/btree_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace googlesql {

// Returns whether `expr` is a builtin function `AGG(MEASURE<T>) => T`.
bool IsMeasureAggFunction(const ResolvedExpr* expr);

// Returns the measure-typed ResolvedColumn m in the given AGG(m) call
// represented by `aggregate_fn`.
//
// Requires: IsMeasureAggFunction(aggregate_fn) is true.
absl::StatusOr<ResolvedColumn> GetInvokedMeasureColumn(
    const ResolvedAggregateFunctionCall* aggregate_fn);

// Returns an error if `input` contains a query shape that is unsupported by
// the measure type rewriter.
absl::Status HasUnsupportedQueryShape(const ResolvedNode* input,
                                      const LanguageOptions& language_options);
class MeasureCollector;
class MeasureType;

struct RewriteMeasureExprResult {
  // A scalar expression over constituent aggregates in
  // `constituent_aggregate_list` that evaluates to the result of AGG(m).
  std::unique_ptr<const ResolvedExpr> rewritten_measure_expr;

  // Aggregates that must be computed by the AggregateScan.
  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
      constituent_aggregate_list;

  // Computed columns representing the closure expressions of the transitive
  // measure dependencies of the measure being rewritten.
  //
  // It is guaranteed that if a ResolvedComputedColumn `c` references another
  // ResolvedComputedColumn `d` in this list, then `d` appears before `c` in
  // this list.
  //
  // These columns will be added to the input_scan of the AggregateScan by
  // wrapping the input_scan with ProjectScans.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      closure_computed_columns;
};

// Rewrites a measure expression.
//
// A measure expression is written as a scalar expression over zero or more
// constituent aggregate functions (e.g. SUM(X) / SUM(Y) + (<scalar_subquery>)),
// and so the resulting rewritten expression has 2 components to it:
//
// 1. A list of constituent aggregate functions that are rewritten to use
//    multi-level aggregation to grain-lock, stored in
//    `constituent_aggregate_list`. These aggregate functions need to be
//    computed by the AggregateScan.
//
// 2. A scalar expression over the constituent aggregate functions, stored in
//    RewriteMeasureExprResult::rewritten_measure_expr. This expression needs to
//    be computed by a ProjectScan over the AggregateScan.
//
// The scalar expression is rewritten to use column references to the
// constituent aggregate functions. The constituent aggregate functions are
// themselves rewritten to use multi-level aggregation to grain-lock and avoid
// overcounting.
//
// Input:
// - `measure_type`: The measure type to rewrite. Must not be null.
// - `closure_struct_ref`: A column reference to the closure struct column
//     corresponding to the measure being rewritten. Must not be null.
// - `measure_collector`: Used to look up info for the measure dependencies of
//   `measure_type`.
// - `any_value_fn`: The ANY_VALUE function definition. Must not be null.
//
// Returns the rewritten measure expression. See the comments for
// `RewriteMeasureExprResult` for more details.
absl::StatusOr<RewriteMeasureExprResult> RewriteMeasureExpr(
    const MeasureType* measure_type,
    const ResolvedColumnRef* closure_struct_ref,
    const MeasureCollector& measure_collector, const Function* any_value_fn,
    FunctionCallBuilder& function_call_builder,
    const LanguageOptions& language_options, ColumnFactory& column_factory,
    TypeFactory& type_factory);

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_REWRITERS_MEASURE_TYPE_REWRITER_UTIL_H_
