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

#include "googlesql/analyzer/rewriters/measure_closure_builder.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/analyzer/annotation_propagator.h"
#include "googlesql/analyzer/rewriters/measure_dependency_graph.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/column_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/base/case.h"
#include "absl/algorithm/container.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "googlesql/base/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

namespace {

/* Helper functions for building closure types */

absl::StatusOr<AnnotatedType> MakeAnnotatedStructType(
    absl::Span<const StructType::StructField> fields,
    absl::Span<const AnnotationMap* const> field_annotations,
    TypeFactory& type_factory) {
  GOOGLESQL_RET_CHECK_EQ(fields.size(), field_annotations.size());
  const StructType* struct_type = nullptr;
  GOOGLESQL_RETURN_IF_ERROR(type_factory.MakeStructType(fields, &struct_type));

  std::unique_ptr<AnnotationMap> map = AnnotationMap::Create(struct_type);
  StructAnnotationMap* struct_map = map->AsStructMap();
  for (int i = 0; i < fields.size(); ++i) {
    if (field_annotations[i] != nullptr) {
      GOOGLESQL_RETURN_IF_ERROR(struct_map->CloneIntoField(i, field_annotations[i]));
    }
  }
  map->Normalize();
  const AnnotationMap* owned_map = nullptr;
  if (!map->Empty()) {
    GOOGLESQL_ASSIGN_OR_RETURN(owned_map, type_factory.TakeOwnership(std::move(map)));
  }
  return AnnotatedType(struct_type, owned_map);
}

// Builds a shared closure struct type for all the given `base_measures`.
//
// The shared type contains all columns referenced by any of the base measures,
// and all row identity columns of the table.
//
// Input:
// - `base_measures`: List of base measure nodes that share the closure type.
// - `table`: The table containing the measures.
// - `type_factory`: The TypeFactory.
//
// Returns:
// - The shared closure struct type.
absl::StatusOr<AnnotatedType> BuildSharedBaseMeasureClosureType(
    absl::Span<const MeasureGraph::Node* const> base_measures,
    const Table& table, TypeFactory& type_factory) {
  absl::btree_set<std::string, googlesql_base::CaseLess>
      all_referenced_column_names;
  absl::btree_set<int> all_row_identity_column_indices;

  for (const MeasureGraph::Node* node : base_measures) {
    GOOGLESQL_ASSIGN_OR_RETURN(CaseInsensitiveStringSet referenced_column_names,
                     GetExpressionColumnNames(*node->def_expr));
    all_referenced_column_names.insert(referenced_column_names.begin(),
                                       referenced_column_names.end());
    all_row_identity_column_indices.insert(
        node->row_identity_column_indices.begin(),
        node->row_identity_column_indices.end());
  }

  // Build referenced_columns struct type
  std::vector<StructType::StructField> ref_fields;
  std::vector<const AnnotationMap*> ref_annotations;
  for (int table_col_idx = 0; table_col_idx < table.NumColumns();
       ++table_col_idx) {
    const Column* column = table.GetColumn(table_col_idx);
    GOOGLESQL_RET_CHECK(column != nullptr);
    if (all_referenced_column_names.contains(column->Name())) {
      // Base measures cannot reference other measure columns
      GOOGLESQL_RET_CHECK(!column->GetType()->IsMeasureType());
      ref_fields.push_back(
          StructType::StructField(column->Name(), column->GetType()));
      ref_annotations.push_back(column->GetTypeAnnotationMap());
    }
  }
  GOOGLESQL_ASSIGN_OR_RETURN(
      AnnotatedType referenced_columns_annotated_type,
      MakeAnnotatedStructType(ref_fields, ref_annotations, type_factory));

  // Build key_columns struct type
  std::vector<StructType::StructField> key_fields;
  std::vector<const AnnotationMap*> key_annotations;
  key_fields.reserve(all_row_identity_column_indices.size());
  key_annotations.reserve(all_row_identity_column_indices.size());
  for (int row_id_col_idx : all_row_identity_column_indices) {
    const Column* column = table.GetColumn(row_id_col_idx);
    GOOGLESQL_RET_CHECK(column != nullptr);
    key_fields.push_back(
        StructType::StructField(column->Name(), column->GetType()));
    key_annotations.push_back(column->GetTypeAnnotationMap());
  }
  GOOGLESQL_ASSIGN_OR_RETURN(
      AnnotatedType key_columns_annotated_type,
      MakeAnnotatedStructType(key_fields, key_annotations, type_factory));

  // Build closure struct type
  std::vector<StructType::StructField> closure_fields = {
      {kReferencedColumnsFieldName, referenced_columns_annotated_type.type},
      {kKeyColumnsFieldName, key_columns_annotated_type.type}};
  std::vector<const AnnotationMap*> closure_annotations = {
      referenced_columns_annotated_type.annotation_map,
      key_columns_annotated_type.annotation_map};

  return MakeAnnotatedStructType(closure_fields, closure_annotations,
                                 type_factory);
}

// Computes the closure struct type for a specific measure `node`.
//
// The closure type consists of:
// - `referenced_columns`: A struct containing all the columns referenced by
//    this measure. If a dependency measure `dep_m` is referenced, the closure
//    struct type of `dep_m` is used as the field type.
// - `key_columns`: A struct containing the row identity columns of the measure.
//
// Input:
// - `node`: The measure node to compute the type for.
// - `computed_dependencies`: Pre-computed closure types of its dependencies.
// - `table`: The table containing the measures.
//
// Returns:
// - The computed closure struct type.
absl::StatusOr<AnnotatedType> BuildMeasureClosureType(
    const MeasureGraph::Node& node,
    const CaseInsensitiveMap<const AnnotatedType*>& computed_dependencies,
    const Table& table, TypeFactory& type_factory) {
  GOOGLESQL_ASSIGN_OR_RETURN(CaseInsensitiveStringSet referenced_column_names,
                   GetExpressionColumnNames(*node.def_expr));

  // Build referenced_columns struct type.
  std::vector<StructType::StructField> ref_fields;
  std::vector<const AnnotationMap*> ref_annotations;
  std::vector<std::string> sorted_names(referenced_column_names.begin(),
                                        referenced_column_names.end());
  std::sort(sorted_names.begin(), sorted_names.end(),
            googlesql_base::CaseLess());
  ref_fields.reserve(sorted_names.size());
  ref_annotations.reserve(sorted_names.size());

  for (const std::string& name : sorted_names) {
    const Column* column = table.FindColumnByName(name);
    GOOGLESQL_RET_CHECK(column != nullptr);
    if (column->GetType()->IsMeasureType()) {
      auto it = computed_dependencies.find(name);
      GOOGLESQL_RET_CHECK(it != computed_dependencies.end())
          << "Cannot find dependency type for: " << name;
      GOOGLESQL_RET_CHECK(it->second != nullptr);
      const AnnotatedType& dep_closure_type = *it->second;
      ref_fields.push_back(
          StructType::StructField(column->Name(), dep_closure_type.type));
      ref_annotations.push_back(dep_closure_type.annotation_map);
    } else {
      ref_fields.push_back(
          StructType::StructField(column->Name(), column->GetType()));
      ref_annotations.push_back(column->GetTypeAnnotationMap());
    }
  }
  GOOGLESQL_ASSIGN_OR_RETURN(
      AnnotatedType referenced_columns_annotated_type,
      MakeAnnotatedStructType(ref_fields, ref_annotations, type_factory));

  // Build key_columns struct type.
  std::vector<StructType::StructField> key_fields;
  std::vector<const AnnotationMap*> key_annotations;
  key_fields.reserve(node.row_identity_column_indices.size());
  key_annotations.reserve(node.row_identity_column_indices.size());
  for (int idx : node.row_identity_column_indices) {
    const Column* column = table.GetColumn(idx);
    GOOGLESQL_RET_CHECK(column != nullptr);
    key_fields.push_back(
        StructType::StructField(column->Name(), column->GetType()));
    key_annotations.push_back(column->GetTypeAnnotationMap());
  }
  GOOGLESQL_ASSIGN_OR_RETURN(
      AnnotatedType key_columns_annotated_type,
      MakeAnnotatedStructType(key_fields, key_annotations, type_factory));

  // Build closure struct type.
  std::vector<StructType::StructField> closure_fields = {
      {kReferencedColumnsFieldName, referenced_columns_annotated_type.type},
      {kKeyColumnsFieldName, key_columns_annotated_type.type}};
  std::vector<const AnnotationMap*> closure_annotations = {
      referenced_columns_annotated_type.annotation_map,
      key_columns_annotated_type.annotation_map};

  return MakeAnnotatedStructType(closure_fields, closure_annotations,
                                 type_factory);
}

// Visitor that computes the closure struct types for measures from a scan.
//
// All the base measures will share a single closure struct type.
class ScanClosureTypeVisitor : public MeasureGraphVisitor<AnnotatedType> {
 public:
  static absl::StatusOr<std::unique_ptr<ScanClosureTypeVisitor>> Create(
      const MeasureGraph& graph, const Table& table, TypeFactory& type_factory);

  ScanClosureTypeVisitor(const ScanClosureTypeVisitor&) = delete;
  ScanClosureTypeVisitor& operator=(const ScanClosureTypeVisitor&) = delete;
  ScanClosureTypeVisitor(ScanClosureTypeVisitor&&) = default;
  ScanClosureTypeVisitor& operator=(ScanClosureTypeVisitor&&) = default;

  absl::StatusOr<AnnotatedType> ComputeBase(
      const MeasureGraph::Node& base_node) override {
    GOOGLESQL_RET_CHECK(shared_base_type_.type != nullptr);
    return shared_base_type_;
  }

  absl::StatusOr<AnnotatedType> ComputeDerived(
      const MeasureGraph::Node& node,
      const CaseInsensitiveMap<const AnnotatedType*>& computed_dependencies)
      override {
    return BuildMeasureClosureType(node, computed_dependencies, table_,
                                   type_factory_);
  }

 private:
  ScanClosureTypeVisitor(const Table& table, TypeFactory& type_factory)
      : table_(table),
        type_factory_(type_factory),
        shared_base_type_(/*type=*/nullptr) {}

  // Initializes the visitor by pre-computing the shared base closure type.
  absl::Status Init(const MeasureGraph& graph);

  // The table containing the measures.
  const Table& table_;

  // The TypeFactory used to create closure struct types.
  TypeFactory& type_factory_;

  // The pre-computed shared closure struct type for all base measures.
  AnnotatedType shared_base_type_;
};

absl::StatusOr<std::unique_ptr<ScanClosureTypeVisitor>>
ScanClosureTypeVisitor::Create(const MeasureGraph& graph, const Table& table,
                               TypeFactory& type_factory) {
  std::unique_ptr<ScanClosureTypeVisitor> visitor(
      new ScanClosureTypeVisitor(table, type_factory));
  GOOGLESQL_RETURN_IF_ERROR(visitor->Init(graph));
  return visitor;
}

absl::Status ScanClosureTypeVisitor::Init(const MeasureGraph& graph) {
  if (graph.nodes().empty()) {
    return absl::OkStatus();
  }

  std::vector<const MeasureGraph::Node*> base_measures;
  for (const MeasureGraph::Node* node : graph.nodes()) {
    if (node->dependencies.empty()) {
      base_measures.push_back(node);
    }
  }

  GOOGLESQL_RET_CHECK(!base_measures.empty());

  GOOGLESQL_ASSIGN_OR_RETURN(
      shared_base_type_,
      BuildSharedBaseMeasureClosureType(base_measures, table_, type_factory_));
  return absl::OkStatus();
}

/* Helper functions and structures for building closure expressions */

struct ClosureExprResult {
  // The closure column for this measure.
  ResolvedColumn closure_column;

  // The columns that this closure struct expression directly depends on.
  absl::flat_hash_set<ResolvedColumn> required_dependencies;
};

// Visitor that computes the closure columns for measures from a scan.
//
// All the base measures will share a single closure struct column.
class ScanClosureExprVisitor : public MeasureGraphVisitor<ClosureExprResult> {
 public:
  static absl::StatusOr<std::unique_ptr<ScanClosureExprVisitor>> Create(
      const MeasureGraph& graph, const Table& table,
      ColumnFactory& column_factory,
      const CaseInsensitiveMap<AnnotatedType>& closure_types,
      ColumnProvider& column_provider,
      AnnotationPropagator& annotation_propagator) {
    auto visitor = absl::WrapUnique(
        new ScanClosureExprVisitor(table, column_factory, closure_types,
                                   column_provider, annotation_propagator));
    GOOGLESQL_RETURN_IF_ERROR(visitor->Init(graph));
    return visitor;
  }

  ScanClosureExprVisitor(const ScanClosureExprVisitor&) = delete;
  ScanClosureExprVisitor& operator=(const ScanClosureExprVisitor&) = delete;
  ScanClosureExprVisitor(ScanClosureExprVisitor&&) = default;
  ScanClosureExprVisitor& operator=(ScanClosureExprVisitor&&) = default;

  absl::StatusOr<ClosureExprResult> ComputeBase(
      const MeasureGraph::Node& base_node) override {
    GOOGLESQL_RET_CHECK(shared_base_result_.closure_column.IsInitialized());
    return shared_base_result_;
  }

  absl::StatusOr<ClosureExprResult> ComputeDerived(
      const MeasureGraph::Node& node,
      const CaseInsensitiveMap<const ClosureExprResult*>& computed_dependencies)
      override {
    auto type_it = closure_types_.find(node.name);
    GOOGLESQL_RET_CHECK(type_it != closure_types_.end());
    return BuildClosureExpr(type_it->second, node.name, computed_dependencies);
  }

  // Releases the accumulated computed columns.
  CaseInsensitiveMap<std::unique_ptr<ResolvedComputedColumn>>
  ReleaseComputedColumns() {
    return std::move(computed_columns_);
  }

 private:
  ScanClosureExprVisitor(const Table& table, ColumnFactory& column_factory,
                         const CaseInsensitiveMap<AnnotatedType>& closure_types,
                         ColumnProvider& column_provider,
                         AnnotationPropagator& annotation_propagator)
      : table_(table),
        column_factory_(column_factory),
        closure_types_(closure_types),
        column_provider_(column_provider),
        annotation_propagator_(annotation_propagator) {}

  // Initializes the visitor by verifying all the base measures share the same
  // closure struct type.
  absl::Status Init(const MeasureGraph& graph);

  // Builds the closure expression result for a measure node, and stores the
  // generated computed column in `computed_columns_`.
  absl::StatusOr<ClosureExprResult> BuildClosureExpr(
      AnnotatedType closure_annotated_type, const std::string& measure_name,
      const CaseInsensitiveMap<const ClosureExprResult*>&
          computed_dependencies);

  // The table containing the measures.
  const Table& table_;

  // The ColumnFactory used to create closure columns.
  ColumnFactory& column_factory_;

  // Pre-computed closure struct types for all measures.
  const CaseInsensitiveMap<AnnotatedType>& closure_types_;

  // Provider to project or get columns from the source scan.
  ColumnProvider& column_provider_;

  // Annotation propagator for attaching annotations bottom-up.
  AnnotationPropagator& annotation_propagator_;

  // The pre-computed closure expression result for all base measures.
  // Initialized in Init().
  ClosureExprResult shared_base_result_;

  // Map from measure name to its computed column.
  CaseInsensitiveMap<std::unique_ptr<ResolvedComputedColumn>> computed_columns_;
};

absl::Status ScanClosureExprVisitor::Init(const MeasureGraph& graph) {
  GOOGLESQL_RET_CHECK(!graph.nodes().empty());

  std::vector<const MeasureGraph::Node*> base_measures;
  for (const MeasureGraph::Node* node : graph.nodes()) {
    if (node->dependencies.empty()) {
      base_measures.push_back(node);
    }
  }

  GOOGLESQL_RET_CHECK(!base_measures.empty());

  // Verify that all base measures have the same closure struct type.
  auto shared_type_it = closure_types_.find(base_measures[0]->name);
  GOOGLESQL_RET_CHECK(shared_type_it != closure_types_.end())
      << "Missing closure type for base measure: " << base_measures[0]->name;
  AnnotatedType shared_type = shared_type_it->second;

  for (size_t i = 1; i < base_measures.size(); ++i) {
    auto type_it = closure_types_.find(base_measures[i]->name);
    GOOGLESQL_RET_CHECK(type_it != closure_types_.end())
        << "Missing closure type for base measure: " << base_measures[i]->name;
    GOOGLESQL_RET_CHECK(type_it->second.type->Equals(shared_type.type))
        << "Base measures do not have the same closure type. "
        << base_measures[0]->name << " has type "
        << shared_type.type->DebugString() << ", but " << base_measures[i]->name
        << " has type " << type_it->second.type->DebugString();
    GOOGLESQL_RET_CHECK(AnnotationMap::Equals(type_it->second.annotation_map,
                                    shared_type.annotation_map))
        << "Base measures do not have the same closure type annotations. "
        << base_measures[0]->name << " vs " << base_measures[i]->name;
  }

  GOOGLESQL_ASSIGN_OR_RETURN(shared_base_result_,
                   BuildClosureExpr(shared_type, base_measures[0]->name,
                                    /*computed_dependencies=*/{}));

  return absl::OkStatus();
}

absl::StatusOr<ClosureExprResult> ScanClosureExprVisitor::BuildClosureExpr(
    AnnotatedType closure_annotated_type, const std::string& measure_name,
    const CaseInsensitiveMap<const ClosureExprResult*>& computed_dependencies) {
  const StructType* closure_type = closure_annotated_type.type->AsStruct();
  GOOGLESQL_RET_CHECK(closure_type != nullptr);
  GOOGLESQL_RET_CHECK(closure_type->fields().size() == 2);
  GOOGLESQL_RET_CHECK(closure_type->field(0).type->IsStruct());
  GOOGLESQL_RET_CHECK(closure_type->field(1).type->IsStruct());

  const StructType* ref_struct_type =
      closure_type->field(kReferencedColumnsFieldIndex).type->AsStruct();
  const StructType* key_struct_type =
      closure_type->field(kKeyColumnsFieldIndex).type->AsStruct();

  // Build the referenced columns struct expression.
  std::vector<std::unique_ptr<const ResolvedExpr>> ref_exprs;
  ref_exprs.reserve(ref_struct_type->fields().size());
  absl::flat_hash_set<ResolvedColumn> required_dependencies;
  required_dependencies.reserve(ref_struct_type->fields().size() +
                                key_struct_type->fields().size());

  for (const StructType::StructField& field : ref_struct_type->fields()) {
    const Column* column = table_.FindColumnByName(field.name);
    GOOGLESQL_RET_CHECK(column != nullptr);
    if (column->GetType()->IsMeasureType()) {
      // Measure dependencies should be provided by `computed_dependencies`.
      auto it = computed_dependencies.find(field.name);
      GOOGLESQL_RET_CHECK(it != computed_dependencies.end())
          << "Cannot find dependency closure expr result for: " << field.name;
      const ClosureExprResult* dep_result = it->second;
      GOOGLESQL_RET_CHECK(dep_result != nullptr);
      const ResolvedColumn& dep_closure_col = dep_result->closure_column;

      ref_exprs.push_back(MakeResolvedColumnRef(
          dep_closure_col,
          // These columns will be added to the expr_list of the ProjectScans,
          // so they are not correlated.
          /*is_correlated=*/false));
      required_dependencies.insert(dep_closure_col);
    } else {
      // Else, it is a non-measure column that we need to project from the
      // provider.
      GOOGLESQL_ASSIGN_OR_RETURN(ResolvedColumn non_measure_col,
                       column_provider_.GetOrProjectColumn(column));
      ref_exprs.push_back(MakeResolvedColumnRef(
          non_measure_col,
          // These columns are from or will be projected by the measure source
          // scan, so they are not correlated.
          /*is_correlated=*/false));
      required_dependencies.insert(non_measure_col);
    }
  }
  std::unique_ptr<ResolvedMakeStruct> ref_struct_expr =
      MakeResolvedMakeStruct(ref_struct_type, std::move(ref_exprs));
  GOOGLESQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      nullptr, ref_struct_expr.get()));

  // Build the key columns struct expression.
  std::vector<std::unique_ptr<const ResolvedExpr>> key_exprs;
  for (const StructType::StructField& field : key_struct_type->fields()) {
    const Column* column = table_.FindColumnByName(field.name);
    GOOGLESQL_RET_CHECK(column != nullptr);
    GOOGLESQL_ASSIGN_OR_RETURN(ResolvedColumn key_col,
                     column_provider_.GetOrProjectColumn(column));
    key_exprs.push_back(MakeResolvedColumnRef(key_col,
                                              /*is_correlated=*/false));
    required_dependencies.insert(key_col);
  }
  std::unique_ptr<ResolvedMakeStruct> key_struct_expr =
      MakeResolvedMakeStruct(key_struct_type, std::move(key_exprs));
  GOOGLESQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      nullptr, key_struct_expr.get()));

  // Build the closure struct expression.
  std::vector<std::unique_ptr<const ResolvedExpr>> wrapping_exprs;
  wrapping_exprs.push_back(std::move(ref_struct_expr));
  wrapping_exprs.push_back(std::move(key_struct_expr));
  std::unique_ptr<ResolvedMakeStruct> closure_expr =
      MakeResolvedMakeStruct(closure_type, std::move(wrapping_exprs));
  GOOGLESQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      nullptr, closure_expr.get()));

  const std::string closure_column_name =
      absl::StrCat("struct_for_measures_from_table_", table_.Name());
  ResolvedColumn closure_column = column_factory_.MakeCol(
      table_.Name(), closure_column_name, closure_expr->annotated_type());

  computed_columns_[measure_name] =
      MakeResolvedComputedColumn(closure_column, std::move(closure_expr));

  return ClosureExprResult{
      .closure_column = closure_column,
      .required_dependencies = std::move(required_dependencies)};
}

// Visitor that computes the closure struct types for measures from a row type.
//
// Each measure type has its own closure struct type.
class RowTypeClosureTypeVisitor : public MeasureGraphVisitor<AnnotatedType> {
 public:
  RowTypeClosureTypeVisitor(const Table& table, TypeFactory& type_factory)
      : table_(table), type_factory_(type_factory) {}

  RowTypeClosureTypeVisitor(const RowTypeClosureTypeVisitor&) = delete;
  RowTypeClosureTypeVisitor& operator=(const RowTypeClosureTypeVisitor&) =
      delete;
  RowTypeClosureTypeVisitor(RowTypeClosureTypeVisitor&&) = default;
  RowTypeClosureTypeVisitor& operator=(RowTypeClosureTypeVisitor&&) = default;

  absl::StatusOr<AnnotatedType> ComputeBase(
      const MeasureGraph::Node& base_node) override;

  absl::StatusOr<AnnotatedType> ComputeDerived(
      const MeasureGraph::Node& node,
      const CaseInsensitiveMap<const AnnotatedType*>& computed_dependencies)
      override;

 private:
  const Table& table_;
  TypeFactory& type_factory_;
};

absl::StatusOr<AnnotatedType> RowTypeClosureTypeVisitor::ComputeBase(
    const MeasureGraph::Node& base_node) {
  return BuildMeasureClosureType(base_node, /*computed_dependencies=*/{},
                                 table_, type_factory_);
}

absl::StatusOr<AnnotatedType> RowTypeClosureTypeVisitor::ComputeDerived(
    const MeasureGraph::Node& node,
    const CaseInsensitiveMap<const AnnotatedType*>& computed_dependencies) {
  return BuildMeasureClosureType(node, computed_dependencies, table_,
                                 type_factory_);
}

}  // namespace

absl::StatusOr<CaseInsensitiveMap<AnnotatedType>>
ComputeClosureTypesForMeasuresFromScan(const MeasureGraph& graph,
                                       const Table& table,
                                       TypeFactory& type_factory) {
  GOOGLESQL_ASSIGN_OR_RETURN(auto type_visitor,
                   ScanClosureTypeVisitor::Create(graph, table, type_factory));
  GOOGLESQL_ASSIGN_OR_RETURN(auto traversal_results,
                   graph.TopologicalTraversal(*type_visitor));

  CaseInsensitiveMap<AnnotatedType> closure_types;
  for (const auto& level_results : traversal_results) {
    for (const auto& [node, type] : level_results) {
      GOOGLESQL_RET_CHECK(closure_types.insert({node->name, type}).second)
          << "Duplicate measure: " << node->name;
    }
  }
  return closure_types;
}

absl::StatusOr<CaseInsensitiveMap<AnnotatedType>> BuildClosureTypesForTableRow(
    const MeasureGraph& graph, const Table& table, TypeFactory& type_factory) {
  RowTypeClosureTypeVisitor visitor(table, type_factory);
  GOOGLESQL_ASSIGN_OR_RETURN(auto traversal_results, graph.TopologicalTraversal(visitor));

  CaseInsensitiveMap<AnnotatedType> closure_types;
  for (const auto& level_results : traversal_results) {
    for (const auto& [node, type] : level_results) {
      GOOGLESQL_RET_CHECK(closure_types.insert({node->name, type}).second)
          << "Duplicate measure: " << node->name;
    }
  }
  return closure_types;
}

absl::StatusOr<ComputeClosureColumnsResult>
ComputeClosureColumnsForMeasuresFromScan(
    const MeasureGraph& graph, const Table& table,
    const CaseInsensitiveMap<AnnotatedType>& closure_types,
    ColumnFactory& column_factory, ColumnProvider& column_provider,
    AnnotationPropagator& annotation_propagator) {
  ComputeClosureColumnsResult result;

  GOOGLESQL_ASSIGN_OR_RETURN(auto expr_visitor,
                   ScanClosureExprVisitor::Create(
                       graph, table, column_factory, closure_types,
                       column_provider, annotation_propagator));
  GOOGLESQL_ASSIGN_OR_RETURN(auto traversal_exprs,
                   graph.TopologicalTraversal(*expr_visitor));

  auto computed_columns_map = expr_visitor->ReleaseComputedColumns();

  for (const auto& level_exprs : traversal_exprs) {
    ClosureLayer layer;
    for (const auto& [node, expr_res] : level_exprs) {
      auto it = computed_columns_map.find(node->name);
      // All base measures share the same closure computed column, and the map
      // `computed_columns_map` only contains one such base measure that maps to
      // the shared closure column.
      //
      // Therefore, if a node is not in the map, it is a base measure that
      // doesn't need its own computed column, so it is safe to skip it.
      if (it != computed_columns_map.end()) {
        layer.computed_columns.push_back(std::move(it->second));
      }
      layer.required_input_columns.insert(
          expr_res.required_dependencies.begin(),
          expr_res.required_dependencies.end());

      GOOGLESQL_RET_CHECK(result.measure_to_closure_col
                    .insert({node->name, expr_res.closure_column})
                    .second)
          << "Duplicate measure: " << node->name;
    }
    result.closure_structs.push_back(std::move(layer));
  }

  return result;
}

}  // namespace googlesql
