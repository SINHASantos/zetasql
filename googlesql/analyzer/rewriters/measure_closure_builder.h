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

#ifndef GOOGLESQL_ANALYZER_REWRITERS_MEASURE_CLOSURE_BUILDER_H_
#define GOOGLESQL_ANALYZER_REWRITERS_MEASURE_CLOSURE_BUILDER_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/analyzer/rewriters/measure_collector.h"
#include "googlesql/analyzer/rewriters/measure_dependency_graph.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"

namespace googlesql {

// Interface for providing or projecting columns for a catalog column out of the
// measure source scan during closure column construction.
class ColumnProvider {
 public:
  virtual ~ColumnProvider() = default;

  // Returns the projected ResolvedColumn for `column`. If the column is not
  // already projected, projects it and returns the new ResolvedColumn.
  //
  // Input:
  // - `column`: The catalog Column to retrieve or project.
  //
  // Returns:
  // - The ResolvedColumn representing the projected column on the measure
  //   source scan.
  virtual absl::StatusOr<ResolvedColumn> GetOrProjectColumn(
      const Column* column) = 0;
};

// Computes the closure struct types for all measures in `graph` from a scan.
//
// As an optimization, all base measures (measures with no dependencies on other
// measures) in the graph share a single base closure type.
//
// The shared base closure struct type has the following structure:
// STRUCT<
//   referenced_columns STRUCT<...>,  -- Union of all columns referenced by
//                                    -- any of the base measures.
//   key_columns STRUCT<...>          -- Union of all row identity columns
//                                    -- of all base measures.
// >
//
// For derived measures, the closure struct type is built hierarchically:
// STRUCT<
//   referenced_columns STRUCT<
//     non_measure_col TYPE,          -- For referenced non-measure columns.
//     dep_measure_1 CLOSURE_TYPE_1,  -- The closure struct type of the
//     dep_measure_2 CLOSURE_TYPE_2,  -- dependency measures.
//     ...
//   >,
//   key_columns STRUCT<...>          -- Row identity columns for this measure.
// >
//
// Input:
// - `graph`: The measure dependency graph containing the measures from the
//   scan.
// - `table`: The table containing the measures.
//
// Returns:
// - A map from measure name to its computed closure struct type.
absl::StatusOr<CaseInsensitiveMap<const StructType*>>
ComputeClosureTypesForMeasuresFromScan(const MeasureGraph& graph,
                                       const Table& table,
                                       TypeFactory& type_factory);

// Computes the closure struct types for measures in `graph` from a row type.
//
// Unlike ComputeClosureTypesForMeasuresFromScan, this does not optimize base
// measures to share a single closure type. Each measure has its own closure
// struct type built hierarchically:
//
// STRUCT<
//   referenced_columns STRUCT<
//     non_measure_col TYPE,          -- For referenced non-measure columns.
//     dep_measure_1 CLOSURE_TYPE_1,  -- The closure struct type of the
//     dep_measure_2 CLOSURE_TYPE_2,  -- dependency measures.
//     ...
//   >,
//   key_columns STRUCT<...>          -- Row identity columns for this measure.
// >
//
// Input:
// - `graph`: The measure dependency graph.
// - `table`: The table associated with the row type.
//
// Returns:
// - A map from measure name to its computed closure struct type.
absl::StatusOr<CaseInsensitiveMap<const StructType*>>
BuildClosureTypesForTableRow(const MeasureGraph& graph, const Table& table,
                             TypeFactory& type_factory);

// Represents the computed closure columns and their required inputs at a
// specific topological level of the measure dependency graph.
struct ClosureLayer {
  // The computed closure columns created at this level.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> computed_columns;

  // The input columns required by the computed columns at this level.
  absl::flat_hash_set<ResolvedColumn> required_input_columns;
};

// Holds the results of computing closure columns.
struct ComputeClosureColumnsResult {
  // Contains the computed columns and their required inputs, organized by
  // topological level.
  std::vector<ClosureLayer> closure_structs;

  // Map from measure name to its closure column.
  CaseInsensitiveMap<ResolvedColumn> measure_to_closure_col;
};

// Builds the closure struct columns for all measures in the graph and their
// dependencies, level by level from the topologically sorted nodes of the
// measure dependency graph.
//
// Input:
// - `graph`: The measure dependency graph. Must not be empty.
// - `table`: The table containing the measures.
// - `closure_types`: Map of pre-computed closure struct types for all measures.
// - `type_factory`: The TypeFactory.
// - `column_factory`: The ColumnFactory.
// - `column_provider`: Provider to fetch or project a column.
//
// Returns:
// - The computed closure columns, and map from measure name to closure column.
absl::StatusOr<ComputeClosureColumnsResult>
ComputeClosureColumnsForMeasuresFromScan(
    const MeasureGraph& graph, const Table& table,
    const CaseInsensitiveMap<const StructType*>& closure_types,
    TypeFactory& type_factory, ColumnFactory& column_factory,
    ColumnProvider& column_provider);

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_REWRITERS_MEASURE_CLOSURE_BUILDER_H_
