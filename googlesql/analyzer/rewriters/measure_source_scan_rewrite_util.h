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

#ifndef GOOGLESQL_ANALYZER_REWRITERS_MEASURE_SOURCE_SCAN_REWRITE_UTIL_H_
#define GOOGLESQL_ANALYZER_REWRITERS_MEASURE_SOURCE_SCAN_REWRITE_UTIL_H_

#include <memory>

#include "googlesql/analyzer/rewriters/measure_collector.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"

namespace googlesql {

// Rewrites `resolved_ast` to replace the AGG'ed measure columns with their
// corresponding closure struct columns and registers measure metadata in
// `measure_collector`.
//
// Each measure source scan that defines an AGG'ed measure column is:
//
// 1. Rebuilt to include the required non-measure columns.
// 2. Wrapped in a chain of `ResolvedProjectScan` layers that evaluate closure
//    struct columns.
//
// Example scan transformation:
//
// Before:
//   +-Scan(col_list=[c1, m1, m2])
//
// where m1 and m2 are measures, m1 := MEASURE(SUM(c1)),
// and m2 := MEASURE(AGG(m1) + 1).
//
// After:
//   +-ProjectScan(col_list=[c1, closure_m1, closure_m2])
//   |   # Computes the closure struct for m2.
//   |   expr_list=[
//   |     closure_m2 := STRUCT(
//   |       referenced_columns: STRUCT(m1: closure_m1),
//   |       key_columns:        STRUCT(key: ...)
//   |     )
//   |   ]
//   |   +-ProjectScan(col_list=[c1, key, closure_m1])
//   |     # Computes the closure struct for m1.
//   |     expr_list=[
//   |       closure_m1 := STRUCT(
//   |         referenced_columns: STRUCT(c1: c1),
//   |         key_columns:        STRUCT(key: ...)
//   |       )
//   |     ]
//   |     # Measures removed; keys and dependencies added.
//   |     +-RebuiltScan(col_list=[c1, key])
absl::StatusOr<std::unique_ptr<const ResolvedNode>> AddClosures(
    MeasureCollector& measure_collector,
    std::unique_ptr<const ResolvedNode> resolved_ast, TypeFactory& type_factory,
    ColumnFactory& column_factory);

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_REWRITERS_MEASURE_SOURCE_SCAN_REWRITE_UTIL_H_
