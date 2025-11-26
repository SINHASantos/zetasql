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

#ifndef ZETASQL_ANALYZER_REWRITERS_MEASURE_COLLECTOR_H_
#define ZETASQL_ANALYZER_REWRITERS_MEASURE_COLLECTOR_H_

#include <vector>

#include "zetasql/public/types/measure_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {

struct MeasureInfo {
  // The expression defining measure logic, from measure definition in catalog
  // containing ResolvedExpressionColumns.
  const ResolvedExpr* measure_expr;

  // A struct-typed column containing values needed for measure expansion into
  // multi-level aggregation. The struct contains columns referenced in
  // `measure_expr` and row identity columns from the source table.
  ResolvedColumn closure_struct;

  // Indices of row identity columns in source table, w.r.t. `Table::Columns()`.
  std::vector<int> row_identity_column_indices;

  // The measure-typed column from whence the measure originates,
  // e.g., a measure-typed ResolvedColumn on a ResolvedTableScan::column_list().
  ResolvedColumn measure_source_column;
};

class MeasureCollector {
 public:
  explicit MeasureCollector(ColumnFactory& column_factory)
      : column_factory_(column_factory) {}

  // Each `MeasureType` pointer uniquely identifies a measure because
  // we instantiate a new `MeasureType` for each measure source column.
  using Key = const MeasureType*;

  // Adds the given measure `info` keyed by `key`.
  //
  // Returns an error if `key` already exists.
  absl::Status AddMeasureInfo(Key key, MeasureInfo info);

  // Returns the measure info keyed by `key`.
  //
  // Returns an error if no info found for `key`.
  absl::StatusOr<MeasureInfo> GetMeasureInfo(Key key) const;

  // Returns the closure struct column that contains the required information to
  // evaluate the measure-typed column `m`.
  //
  // Note: This function is needed because multiple measure-typed columns can
  // map to the same closure struct column (because we build one closure struct
  // per source scan). If each measure-typed column mapped to a unique struct
  // closure column, we wouldn't need this function.
  absl::StatusOr<ResolvedColumn> GetClosureColumn(const ResolvedColumn& m);

  // Marks as aggregated the measure info corresponding to `key`.
  // This means at least one measure-typed ResolvedColumn `m` associated with
  // the `key` is AGG'ed, i.e., there is an AGG(m) call.
  void MarkAgged(Key key);

  // Returns whether the measure columns associated with `key` are AGG'ed.
  bool IsAgged(Key key) const;

  // Performs the following validation:
  // - Every key in `agged_measure_keys_` is present in `measure_infos_`.
  absl::Status Validate() const;

 private:
  ColumnFactory& column_factory_;
  absl::flat_hash_map<Key, MeasureInfo> measure_infos_;
  absl::flat_hash_map<ResolvedColumn, ResolvedColumn>
      propagated_closure_columns_;
  // Contains the key of the measure columns that are AGG'ed.
  absl::flat_hash_set<Key> agged_measure_keys_;
};

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_MEASURE_COLLECTOR_H_
