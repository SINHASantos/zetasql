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

#include "googlesql/reference_impl/measure_evaluation.h"

#include <utility>

#include "googlesql/public/catalog.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/types/measure_type.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_builder.h"

namespace googlesql {

namespace {
// Provides unified access to the table schema for scans that can return
// measure columns.
template <typename T>
struct ScanTraits;

template <>
struct ScanTraits<ResolvedTableScan> {
  static const Table* GetTable(const ResolvedTableScan& scan) {
    return scan.table();
  }
};

template <>
struct ScanTraits<ResolvedTVFScan> {
  static const Table* GetTable(const ResolvedTVFScan& scan) {
    return scan.signature()->result_table_schema();
  }
};

// Returns the resolved measure expression for the given `column` if it is a
// measure column with a resolved expression. Returns nullptr otherwise.
const ResolvedExpr* GetMeasureExpression(const Column& column) {
  if (!column.GetType()->IsMeasureType()) {
    return nullptr;
  }
  auto expr_attr = column.GetExpression();
  if (!expr_attr.has_value()) {
    return nullptr;
  }
  if (expr_attr->GetExpressionKind() !=
      Column::ExpressionAttributes::ExpressionKind::MEASURE_EXPRESSION) {
    return nullptr;
  }
  if (!expr_attr->HasResolvedExpression()) {
    return nullptr;
  }
  return expr_attr->GetResolvedExpression();
}

}  // namespace

template <typename ScanType>
absl::Status MeasureColumnToExprMapping::TrackMeasureColumnsEmittedByScan(
    const ScanType& scan) {
  // If there are no measure columns emitted by the table scan, we can skip this
  // method. We deliberately do not check `column_index_list` at this point
  // because there are many legacy cases where it is not populated.
  if (!absl::c_any_of(scan.column_list(), [](const ResolvedColumn& column) {
        return column.type()->IsMeasureType();
      })) {
    return absl::OkStatus();
  }
  const Table* table = ScanTraits<ScanType>::GetTable(scan);
  GOOGLESQL_RET_CHECK(table != nullptr);

  // Step 1: Add all catalog measures to the map.
  //
  // This is needed for measures referenced in the definition expressions of
  // other measures. For example, if we have a base measure `m1 := SUM(x)`
  // and a derived measure `m2 := AGG(m1) + 1`, when evaluating `m2` we need to
  // resolve `m1` which is a catalog measure.
  for (int i = 0; i < table->NumColumns(); ++i) {
    const Column* column = table->GetColumn(i);
    if (const ResolvedExpr* measure_expr = GetMeasureExpression(*column);
        measure_expr != nullptr) {
      GOOGLESQL_RETURN_IF_ERROR(AddMeasureColumnWithExpr(column->GetType()->AsMeasure(),
                                               measure_expr));
    }
  }

  // Step 2: Add projected scan measures to the map.
  //
  // This is needed for measure references in the query, e.g., AGG(m).
  //
  // The measure expressions in Step 1 and Step 2 are identical (both fetch from
  // `catalog_column`), but both entries are needed in the map because
  // `ResolvedColumn::type()` has a distinct `MeasureType*` pointer from
  // `catalog_column->GetType()` due to measure type uniqueness.
  for (int idx = 0; idx < scan.column_list_size(); ++idx) {
    const ResolvedColumn& column = scan.column_list(idx);
    if (column.type()->IsMeasureType()) {
      const Column* catalog_column = nullptr;
      if (idx < scan.column_index_list_size()) {
        catalog_column = table->GetColumn(scan.column_index_list(idx));
      } else {
        catalog_column = table->FindColumnByName(column.name());
      }
      GOOGLESQL_RET_CHECK(catalog_column != nullptr);
      const ResolvedExpr* measure_expr = GetMeasureExpression(*catalog_column);
      GOOGLESQL_RET_CHECK(measure_expr != nullptr);
      GOOGLESQL_RETURN_IF_ERROR(
          AddMeasureColumnWithExpr(column.type()->AsMeasure(), measure_expr));
    }
  }
  return absl::OkStatus();
}

absl::Status MeasureColumnToExprMapping::TrackMeasureColumnsEmittedByTableScan(
    const ResolvedTableScan& table_scan) {
  return TrackMeasureColumnsEmittedByScan(table_scan);
}

absl::Status MeasureColumnToExprMapping::TrackMeasureColumnsEmittedByTVFScan(
    const ResolvedTVFScan& tvf_scan) {
  return TrackMeasureColumnsEmittedByScan(tvf_scan);
}

absl::StatusOr<const ResolvedExpr*> MeasureColumnToExprMapping::GetMeasureExpr(
    const MeasureType* measure_type) const {
  if (auto it = measure_column_to_expr_.find(measure_type);
      it != measure_column_to_expr_.end()) {
    return it->second;
  }
  return absl::NotFoundError(
      absl::StrCat("MeasureType not found: ", measure_type->DebugString()));
}

absl::Status MeasureColumnToExprMapping::AddMeasureColumnWithExpr(
    const MeasureType* measure_type, const ResolvedExpr* expr) {
  GOOGLESQL_RET_CHECK(measure_type != nullptr);
  GOOGLESQL_RET_CHECK(expr != nullptr);
  auto [it, inserted] = measure_column_to_expr_.insert({measure_type, expr});
  if (inserted) {
    return absl::OkStatus();
  }
  // If inserting the same column twice, we must be tracking the same
  // expression.
  GOOGLESQL_RET_CHECK_EQ(it->second, expr);
  return absl::OkStatus();
}

}  // namespace googlesql
