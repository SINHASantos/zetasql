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

#include "googlesql/analyzer/rewriters/measure_source_scan_rewrite_util.h"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/analyzer/annotation_propagator.h"
#include "googlesql/analyzer/rewriters/measure_closure_builder.h"
#include "googlesql/analyzer/rewriters/measure_collector.h"
#include "googlesql/analyzer/rewriters/measure_dependency_graph.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/measure_type.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_builder.h"
#include "googlesql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "googlesql/resolved_ast/resolved_ast_visitor.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "googlesql/base/case.h"
#include "absl/algorithm/container.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "googlesql/base/check.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

namespace {

/* Measure source column helper structures */

// Traits to extract the catalog Table from different scan types.
template <typename ScanType>
struct MeasureSourceTraits {};

template <>
struct MeasureSourceTraits<ResolvedTableScan> {
  static const Table* GetTable(const ResolvedTableScan* scan) {
    return scan->table();
  }
};

template <>
struct MeasureSourceTraits<ResolvedTVFScan> {
  static const Table* GetTable(const ResolvedTVFScan* scan) {
    return scan->signature()->result_table_schema();
  }
};

// Registers the catalog metadata of all measures in `graph` with
// `measure_collector`.
//
// Input:
// - `graph`: The measure dependency graph.
// - `table`: The catalog table.
// - `closure_types`: Map of measure name to its closure struct type.
// - `measure_collector`: The collector to register the metadata with.
//   The collector is mutated to register the `MeasureInfo`s.
absl::Status RegisterCatalogMeasureInfos(
    const MeasureGraph& graph, const Table& table,
    const CaseInsensitiveMap<AnnotatedType>& closure_types,
    MeasureCollector& measure_collector) {
  for (const MeasureGraph::Node* node : graph.nodes()) {
    absl::btree_set<std::string, googlesql_base::CaseLess>
        row_identity_column_names;
    for (int idx : node->row_identity_column_indices) {
      row_identity_column_names.insert(table.GetColumn(idx)->Name());
    }

    auto closure_type_it = closure_types.find(node->name);
    GOOGLESQL_RET_CHECK(closure_type_it != closure_types.end());
    AnnotatedType closure_annotated_type = closure_type_it->second;
    GOOGLESQL_RET_CHECK(closure_annotated_type.type != nullptr);
    MeasureInfo measure_info = {
        .measure_expr = node->def_expr,
        .row_identity_column_names = std::move(row_identity_column_names),
        .closure_struct_annotated_type = closure_annotated_type,
    };
    GOOGLESQL_RETURN_IF_ERROR(
        measure_collector.AddMeasureInfo(node->measure_type, measure_info));
    measure_collector.MarkAgged(node->measure_type);
  }
  return absl::OkStatus();
}

// Represents a measure column projected by a source scan.
struct ProjectedMeasure {
  // The projected ResolvedColumn for the measure.
  ResolvedColumn resolved_column;
  // The catalog column for the measure.
  const Column* catalog_column;
  // The node in the measure dependency graph.
  const MeasureGraph::Node* node;
};

// Registers the resolved metadata of the projected measures with
// `measure_collector`.
//
// Input:
// - `projected_measures`: The list of projected measures.
// - `measure_to_closure_col`: Map of measure name to its closure column.
// - `measure_collector`: The collector to register the metadata with.
//   The collector is mutated to add projected measure infos.
absl::Status RegisterProjectedMeasureInfos(
    absl::Span<const ProjectedMeasure> projected_measures,
    const CaseInsensitiveMap<ResolvedColumn>& measure_to_closure_col,
    MeasureCollector& measure_collector) {
  for (const auto& pm : projected_measures) {
    GOOGLESQL_ASSIGN_OR_RETURN(MeasureInfo catalog_info,
                     measure_collector.GetMeasureInfo(pm.node->measure_type));

    auto it = measure_to_closure_col.find(pm.node->name);
    GOOGLESQL_RET_CHECK(it != measure_to_closure_col.end());
    ResolvedColumn closure_col = it->second;

    MeasureInfo projected_info = catalog_info;
    projected_info.closure_column = MeasureInfo::ClosureColumn{
        .closure_struct = closure_col,
        .measure_source_column = pm.resolved_column,
    };
    projected_info.closure_struct_annotated_type = closure_col.annotated_type();

    GOOGLESQL_RETURN_IF_ERROR(measure_collector.AddMeasureInfo(
        pm.resolved_column.type()->AsMeasure(), projected_info));
    measure_collector.MarkAgged(pm.resolved_column.type()->AsMeasure());
  }
  return absl::OkStatus();
}

bool HasAggedMeasure(const ResolvedScan& scan,
                     const MeasureCollector& measure_collector) {
  for (const ResolvedColumn& col : scan.column_list()) {
    if (col.type()->IsMeasureType() &&
        measure_collector.IsAgged(col.type()->AsMeasure())) {
      return true;
    }
  }
  return false;
}

// Rebuilds the measure source `scan` by
// - removing AGG'ed measure columns, and
// - adding any missing non-measure columns that are transitively required by
//   the measure expressions.
//
// Input:
// - `scan`: The measure source scan to rebuild.
// - `table`: The catalog table.
// - `measure_collector`: Used to identify which measure columns are AGG'ed.
// - `missing_non_measure_columns`: Contains the non-measure columns to be
//   added.
template <typename ScanType>
absl::StatusOr<std::unique_ptr<const ScanType>> RebuildScan(
    std::unique_ptr<const ScanType> scan, const Table& table,
    const MeasureCollector& measure_collector,
    const CaseInsensitiveMap<ResolvedColumn>& missing_non_measure_columns) {
  GOOGLESQL_RET_CHECK(scan != nullptr);
  struct IndexedColumn {
    int index;
    ResolvedColumn column;
  };
  std::vector<IndexedColumn> indexed_columns;
  for (int i = 0; i < scan->column_list_size(); ++i) {
    const ResolvedColumn& col = scan->column_list(i);
    if (col.type()->IsMeasureType() &&
        measure_collector.IsAgged(col.type()->AsMeasure())) {
      continue;
    }
    indexed_columns.push_back(
        IndexedColumn{.index = scan->column_index_list(i), .column = col});
  }

  for (int i = 0; i < table.NumColumns(); ++i) {
    const Column* column = table.GetColumn(i);
    if (missing_non_measure_columns.contains(column->Name())) {
      indexed_columns.push_back(IndexedColumn{
          .index = i,
          .column = missing_non_measure_columns.at(column->Name())});
    }
  }

  std::sort(indexed_columns.begin(), indexed_columns.end(),
            [](const auto& a, const auto& b) { return a.index < b.index; });

  ResolvedColumnList project_column_list;
  project_column_list.reserve(indexed_columns.size());
  std::vector<int> project_column_index_list;
  project_column_index_list.reserve(indexed_columns.size());
  for (const auto& item : indexed_columns) {
    project_column_list.push_back(item.column);
    project_column_index_list.push_back(item.index);
  }

  GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<const ScanType> rebuilt_scan,
                   ToBuilder(std::move(scan))
                       .set_column_list(project_column_list)
                       .set_column_index_list(project_column_index_list)
                       .Build());
  return rebuilt_scan;
}

// Returns the set of columns that must be projected at each level of
// ProjectScan.
//
// Input:
// - `scan`: The measure source scan.
// - `final_output_columns`: The set of columns that the final output scan
//   should project.
// - `closure_structs`: Lists the computed closure columns for each ProjectScan
//   layer. This argument is consumed by the function.
absl::StatusOr<std::vector<absl::flat_hash_set<ResolvedColumn>>>
ComputeProjectScanOutputColumns(
    const ResolvedScan& scan,
    const absl::flat_hash_set<ResolvedColumn>& final_output_columns,
    absl::Span<const ClosureLayer> closure_structs) {
  std::vector<absl::flat_hash_set<ResolvedColumn>> alive_sets(
      closure_structs.size());
  absl::flat_hash_set<ResolvedColumn> current_alive_columns =
      final_output_columns;

  // Standard liveness analysis
  // (https://en.wikipedia.org/wiki/Live-variable_analysis).
  for (int i = static_cast<int>(closure_structs.size()) - 1; i >= 0; --i) {
    const auto& level = closure_structs[i];
    alive_sets[i] = current_alive_columns;
    // Columns computed at this level are produced here, so they weren't alive
    // before this level.
    for (const auto& closure : level.computed_columns) {
      current_alive_columns.erase(closure->column());
    }
    // Columns required as input for this level must be alive before this level.
    for (const ResolvedColumn& dep : level.required_input_columns) {
      current_alive_columns.insert(dep);
    }
  }

  // The columns remaining after backpropagation must match the columns
  // produced by the input scan.
  absl::flat_hash_set<ResolvedColumn> rebuilt_scan_columns(
      scan.column_list().begin(), scan.column_list().end());
  GOOGLESQL_RET_CHECK(rebuilt_scan_columns == current_alive_columns)
      << "Columns in rebuilt source scan do not match alive columns. "
      << "Alive columns: " << current_alive_columns.size()
      << ", Rebuilt scan columns: " << rebuilt_scan_columns.size();

  return alive_sets;
}

// Projects closure struct columns on top of `scan` by wrapping it with layers
// of `ResolvedProjectScan`s.
//
// The final ProjectScan will output columns in `final_output_columns`.
//
// Input:
// - `scan`: The input scan to wrap. Must not be null.
// - `final_output_columns`: The set of columns that the final output scan
//   should project.
// - `closure_structs`: Lists the computed closure columns for each topological
//   level. This parameter is consumed by the function.
absl::StatusOr<std::unique_ptr<const ResolvedScan>> ProjectClosureExpressions(
    std::unique_ptr<const ResolvedScan> scan,
    const absl::flat_hash_set<ResolvedColumn>& final_output_columns,
    std::vector<ClosureLayer> closure_structs) {
  GOOGLESQL_RET_CHECK(scan != nullptr);

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::vector<absl::flat_hash_set<ResolvedColumn>> leveled_output_columns,
      ComputeProjectScanOutputColumns(*scan, final_output_columns,
                                      closure_structs));

  // Construct the ProjectScans from bottom to top. Each ProjectScan only
  // outputs the columns in `leveled_output_columns` at that level.
  std::unique_ptr<const ResolvedScan> prev = std::move(scan);
  for (size_t i = 0; i < closure_structs.size(); ++i) {
    auto& level = closure_structs[i];
    const auto& output_columns = leveled_output_columns[i];

    ResolvedColumnList new_column_list;
    absl::c_copy_if(prev->column_list(), std::back_inserter(new_column_list),
                    [&output_columns](const ResolvedColumn& prev_col) {
                      return output_columns.contains(prev_col);
                    });
    for (const auto& closure : level.computed_columns) {
      // All computed closure columns are transitively needed, so they must be
      // present in `output_columns`.
      GOOGLESQL_RET_CHECK(output_columns.contains(closure->column()));
      new_column_list.push_back(closure->column());
    }

    prev = MakeResolvedProjectScan(
        new_column_list, std::move(level.computed_columns), std::move(prev));
  }
  return prev;
}

// Rewrites a ResolvedTableScan or ResolvedTVFScan if it contains AGG'ed
// measure source columns.
//
// If measure columns are present on the scan, this class:
// 1. Builds closure columns. Each closure column is a STRUCT containing:
//    - referenced_columns: a STRUCT of columns referenced by the measure
//      expression and its dependencies on the scan.
//    - key_columns: a STRUCT of row identity columns of the table.
// 2. Creates a chain of `ProjectScan`s on top of the input scan to evaluate
//    and project these closure columns.
// 3. Replaces the AGG'ed measure columns with their corresponding closure
//    columns, and adds any columns referenced by their definition expressions
//    but not present in scan's column list to the scan.
// 4. Registers measure metadata with `measure_collector_` for later rewrite
//    of the measure references.
template <typename ScanType>
class MeasureSourceColumnReplacer {
 public:
  MeasureSourceColumnReplacer(std::unique_ptr<const ScanType> scan,
                              MeasureCollector& measure_collector,
                              TypeFactory& type_factory,
                              ColumnFactory& column_factory,
                              AnnotationPropagator& annotation_propagator)
      : scan_(std::move(scan)),
        measure_collector_(measure_collector),
        type_factory_(type_factory),
        column_factory_(column_factory),
        annotation_propagator_(annotation_propagator) {}

  // Rewrites the input scan to replace AGG'ed measure columns with closure
  // struct columns.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Replace() {
    GOOGLESQL_RET_CHECK(scan_ != nullptr);
    if (!HasAggedMeasure(*scan_, measure_collector_)) {
      return std::move(scan_);
    }

    GOOGLESQL_ASSIGN_OR_RETURN(const Table* table, GetTable());

    // Step 1: Build the measure dependency graph to collect all the
    // transitively AGG'ed measures.
    GOOGLESQL_ASSIGN_OR_RETURN(MeasureGraph graph, BuildMeasureGraphFromScan());

    GOOGLESQL_ASSIGN_OR_RETURN(
        CaseInsensitiveMap<AnnotatedType> closure_types,
        ComputeClosureTypesForMeasuresFromScan(graph, *table, type_factory_));

    // Step 2: Register these measures with `measure_collector` under their
    // catalog Column::GetType().
    //
    // These MeasureInfo's will be used to rewrite the AGG calls over dependency
    // measures, e.g., the `AGG(b)` for `m := AGG(b) + 1`.
    GOOGLESQL_RETURN_IF_ERROR(RegisterCatalogMeasureInfos(graph, *table, closure_types,
                                                measure_collector_));

    // Step 3: Build the closure columns for all the measures in the graph.
    ReplacerColumnProvider column_provider(*this, *table);
    GOOGLESQL_ASSIGN_OR_RETURN(ComputeClosureColumnsResult closure_result,
                     ComputeClosureColumnsForMeasuresFromScan(
                         graph, *table, closure_types, column_factory_,
                         column_provider, annotation_propagator_));

    // Step 4: Register the MeasureInfo for the projected measures. They are
    // different from the catalog measure info because ResolvedColumn::type()
    // is different from Column::type().
    //
    // These MeasureInfo's will be used to rewrite the explicit AGG calls users
    // write in the query.
    GOOGLESQL_ASSIGN_OR_RETURN(std::vector<ProjectedMeasure> projected_measures,
                     GetProjectedMeasures(graph));
    GOOGLESQL_RETURN_IF_ERROR(RegisterProjectedMeasureInfos(
        projected_measures, closure_result.measure_to_closure_col,
        measure_collector_));

    // Step 5: Rebuild the scan to project the closure struct columns and the
    // required non-measure columns, and remove the AGG'ed measure columns.
    GOOGLESQL_ASSIGN_OR_RETURN(
        absl::flat_hash_set<ResolvedColumn> final_output_columns,
        ComputeFinalOutputColumn(projected_measures,
                                 closure_result.measure_to_closure_col));

    GOOGLESQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ScanType> rebuilt_scan,
        RebuildScan(std::move(scan_), *table, measure_collector_,
                    column_provider.missing_non_measure_columns()));

    GOOGLESQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedScan> final_scan,
        ProjectClosureExpressions(std::move(rebuilt_scan), final_output_columns,
                                  std::move(closure_result.closure_structs)));

    return final_scan;
  }

 private:
  // Helper class that implements ColumnProvider to collect missing non-measure
  // columns while building closure columns.
  class ReplacerColumnProvider : public ColumnProvider {
   public:
    ReplacerColumnProvider(MeasureSourceColumnReplacer& replacer,
                           const Table& table)
        : replacer_(replacer), table_(table) {}

    absl::StatusOr<ResolvedColumn> GetOrProjectColumn(
        const Column* column) override {
      GOOGLESQL_RET_CHECK(column != nullptr);

      int table_col_idx = -1;
      for (int i = 0; i < table_.NumColumns(); ++i) {
        if (table_.GetColumn(i) == column) {
          table_col_idx = i;
          break;
        }
      }
      GOOGLESQL_RET_CHECK_GE(table_col_idx, 0);

      for (int i = 0; i < replacer_.scan_->column_index_list_size(); ++i) {
        if (replacer_.scan_->column_index_list(i) == table_col_idx) {
          return replacer_.scan_->column_list(i);
        }
      }
      auto it = missing_non_measure_columns_.find(column->Name());
      if (it != missing_non_measure_columns_.end()) {
        return it->second;
      }
      ResolvedColumn new_col = replacer_.column_factory_.MakeCol(
          table_.Name(), column->Name(),
          AnnotatedType(column->GetType(), column->GetTypeAnnotationMap()));
      missing_non_measure_columns_[column->Name()] = new_col;
      return new_col;
    }

    const CaseInsensitiveMap<ResolvedColumn>& missing_non_measure_columns()
        const {
      return missing_non_measure_columns_;
    }

   private:
    // Reference to the parent replacer.
    MeasureSourceColumnReplacer& replacer_;
    // The catalog table.
    const Table& table_;
    // Map to accumulate non-measure columns that were missing from the scan.
    CaseInsensitiveMap<ResolvedColumn> missing_non_measure_columns_;
  };

  // Returns the catalog table associated with the scan.
  absl::StatusOr<const Table*> GetTable() const {
    const Table* table = MeasureSourceTraits<ScanType>::GetTable(scan_.get());
    GOOGLESQL_RET_CHECK(table != nullptr);
    return table;
  }

  // Builds the measure dependency graph from `scan_`.
  absl::StatusOr<MeasureGraph> BuildMeasureGraphFromScan() {
    GOOGLESQL_ASSIGN_OR_RETURN(const Table* table, GetTable());
    MeasureGraph graph;
    for (int i = 0; i < scan_->column_list_size(); ++i) {
      const ResolvedColumn& col = scan_->column_list(i);
      if (!col.type()->IsMeasureType()) {
        continue;
      }
      if (!measure_collector_.IsAgged(col.type()->AsMeasure())) {
        continue;
      }
      int col_idx_in_table = scan_->column_index_list(i);
      const Column* catalog_column = table->GetColumn(col_idx_in_table);

      GOOGLESQL_RETURN_IF_ERROR(graph.AddIfNotPresent(*catalog_column, *table).status());
    }
    return graph;
  }

  // Returns the projected measures in `graph` that are projected by `scan_`.
  absl::StatusOr<std::vector<ProjectedMeasure>> GetProjectedMeasures(
      const MeasureGraph& graph) const {
    GOOGLESQL_ASSIGN_OR_RETURN(const Table* table, GetTable());
    std::vector<ProjectedMeasure> projected_measures;
    for (int i = 0; i < scan_->column_list_size(); ++i) {
      const ResolvedColumn& c = scan_->column_list(i);
      if (!c.type()->IsMeasureType() ||
          !measure_collector_.IsAgged(c.type()->AsMeasure())) {
        continue;
      }
      int col_idx = scan_->column_index_list(i);
      const Column* catalog_col = table->GetColumn(col_idx);

      const MeasureGraph::Node* node = graph.FindNode(catalog_col->Name());
      GOOGLESQL_RET_CHECK(node != nullptr);
      projected_measures.push_back(ProjectedMeasure{
          .resolved_column = c,
          .catalog_column = catalog_col,
          .node = node,
      });
    }
    return projected_measures;
  }

  // Computes the set of columns that the final rewritten scan (the outermost
  // ProjectScan) must output.
  //
  // Input:
  // - `projected_measures`: The list of projected measures.
  // - `measure_to_closure_col`: Map of measure name to its closure column.
  //
  // Returns:
  // - The set of columns containing:
  //   - Columns from the original `scan_`, excluding AGG'ed measure columns.
  //   - The new closure columns that replace the AGG'ed measure columns.
  absl::StatusOr<absl::flat_hash_set<ResolvedColumn>> ComputeFinalOutputColumn(
      absl::Span<const ProjectedMeasure> projected_measures,
      const CaseInsensitiveMap<ResolvedColumn>& measure_to_closure_col) const {
    absl::flat_hash_set<ResolvedColumn> alive_columns;
    for (const ResolvedColumn& col : scan_->column_list()) {
      if (col.type()->IsMeasureType() &&
          measure_collector_.IsAgged(col.type()->AsMeasure())) {
        continue;
      }
      alive_columns.insert(col);
    }
    for (const auto& pm : projected_measures) {
      auto it = measure_to_closure_col.find(pm.node->name);
      GOOGLESQL_RET_CHECK(it != measure_to_closure_col.end());
      alive_columns.insert(it->second);
    }
    return alive_columns;
  }

  // The scan to be rewritten.
  std::unique_ptr<const ScanType> scan_;
  // Used to collect and query measure metadata.
  MeasureCollector& measure_collector_;
  // Used to create closure struct types.
  TypeFactory& type_factory_;
  // Used to generate new resolved columns.
  ColumnFactory& column_factory_;
  // Annotation propagator for attaching annotations bottom-up.
  AnnotationPropagator& annotation_propagator_;
};

class MeasureSourceCollector : public ResolvedASTRewriteVisitor {
 public:
  MeasureSourceCollector(MeasureCollector& measure_collector,
                         TypeFactory& type_factory,
                         ColumnFactory& column_factory,
                         AnnotationPropagator& annotation_propagator)
      : measure_collector_(measure_collector),
        type_factory_(type_factory),
        column_factory_(column_factory),
        annotation_propagator_(annotation_propagator) {}

 protected:
  // Row field access of a measure-typed column is a source of a measure.
  //
  // Here we only collect the measure info and defer replacing the measure
  // source with the closure struct to `MeasureColumnRewriter` to avoid having
  // type inconsistencies in the Resolved AST, e.g., between a
  // `ResolvedGetRowField` and the `ResolvedComputedColumn` that contains
  // it.
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedGetRowField(
      std::unique_ptr<const ResolvedGetRowField> node) override {
    if (!node->type()->IsMeasureType()) {
      return node;
    }

    const MeasureType* measure_type = node->type()->AsMeasure();
    if (!measure_collector_.IsAgged(measure_type)) {
      return node;
    }

    {
      absl::Status status =
          measure_collector_.GetMeasureInfo(measure_type).status();
      if (status.ok()) {
        // Already registered, skip the collection. This can happen because
        // the type of a `ResolvedGetRowField` comes from catalog
        // `Column::type()`.
        return node;
      }
      GOOGLESQL_RET_CHECK(absl::IsNotFound(status))
          << "Unexpected error getting measure info for measure type: "
          << measure_type->DebugString() << " error: " << status;
    }

    const Column* measure_column = node->column();
    const Table* table = node->expr()->type()->AsRowType()->table();
    // We currently only support measure columns on tables with DEFAULT column
    // list mode, i.e., tables that have a column list.
    GOOGLESQL_RET_CHECK(table->HasColumnList());

    // Register all the measures transitively depending on this measure from
    // row type.
    MeasureGraph graph;
    GOOGLESQL_RETURN_IF_ERROR(graph.AddIfNotPresent(*measure_column, *table).status());
    GOOGLESQL_ASSIGN_OR_RETURN(
        CaseInsensitiveMap<AnnotatedType> closure_types,
        BuildClosureTypesForTableRow(graph, *table, type_factory_));
    GOOGLESQL_RETURN_IF_ERROR(RegisterCatalogMeasureInfos(graph, *table, closure_types,
                                                measure_collector_));

    return node;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedTableScan(
      std::unique_ptr<const ResolvedTableScan> scan) override {
    return MeasureSourceColumnReplacer<ResolvedTableScan>(
               std::move(scan), measure_collector_, type_factory_,
               column_factory_, annotation_propagator_)
        .Replace();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> PostVisitResolvedTVFScan(
      std::unique_ptr<const ResolvedTVFScan> scan) override {
    return MeasureSourceColumnReplacer<ResolvedTVFScan>(
               std::move(scan), measure_collector_, type_factory_,
               column_factory_, annotation_propagator_)
        .Replace();
  }

 private:
  MeasureCollector& measure_collector_;
  TypeFactory& type_factory_;
  ColumnFactory& column_factory_;
  AnnotationPropagator& annotation_propagator_;
};

}  // namespace

/* Public API Entry Point */

absl::StatusOr<std::unique_ptr<const ResolvedNode>> AddClosures(
    MeasureCollector& measure_collector,
    std::unique_ptr<const ResolvedNode> resolved_ast, TypeFactory& type_factory,
    ColumnFactory& column_factory,
    AnnotationPropagator& annotation_propagator) {
  MeasureSourceCollector visitor(measure_collector, type_factory,
                                 column_factory, annotation_propagator);
  return visitor.VisitAll(std::move(resolved_ast));
}

}  // namespace googlesql
