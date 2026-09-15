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

#include "googlesql/analyzer/graph_dml_resolver.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/analyzer/expr_resolver_helper.h"
#include "googlesql/analyzer/graph_query_resolver_helper.h"
#include "googlesql/analyzer/name_scope.h"
#include "googlesql/analyzer/resolver.h"
#include "googlesql/parser/ast_node.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/parser/parse_tree_errors.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/property_graph.h"
#include "googlesql/public/types/graph_element_type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/types/type_modifiers.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_builder.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

STATIC_IDSTRING(kElementTableName, "$element_table");
STATIC_IDSTRING(kInsertNodeName, "$insert_node");
STATIC_IDSTRING(kInsertEdgeName, "$insert_edge");

namespace {

// Returns a list of requested label names in the label filter.
// Returns an error if the label filter is not a simple label or a
// conjunction (&) of simple labels.
absl::StatusOr<std::vector<IdString>> ExtractInsertLabelNames(
    const ASTGraphLabelFilter* ast_label_filter) {
  GOOGLESQL_RET_CHECK_NE(ast_label_filter, nullptr);
  GOOGLESQL_RET_CHECK_NE(ast_label_filter->label_expression(), nullptr);

  std::vector<IdString> labels;
  const auto* expr = ast_label_filter->label_expression();

  // This is a simple label.
  if (expr->Is<ASTGraphElementLabel>()) {
    labels.push_back(
        expr->GetAsOrDie<ASTGraphElementLabel>()->name()->GetAsIdString());
    return labels;
  }

  auto make_invalid_label_sql_error = [&]() {
    return MakeSqlErrorAt(ast_label_filter)
           << "Only label identifiers and label conjunction (&) are "
              "supported in INSERT";
  };

  // This is a conjunction of simple labels.
  if (expr->Is<ASTGraphLabelOperation>()) {
    const auto* op = expr->GetAsOrDie<ASTGraphLabelOperation>();
    if (op->op_type() != ASTGraphLabelOperation::AND) {
      return make_invalid_label_sql_error();
    }

    for (const auto* input : op->inputs()) {
      if (!input->Is<ASTGraphElementLabel>()) {
        return make_invalid_label_sql_error();
      }
      labels.push_back(
          input->GetAsOrDie<ASTGraphElementLabel>()->name()->GetAsIdString());
    }
    return labels;
  }

  return make_invalid_label_sql_error();
}

// Validates whether a static property is writable. Rejects read-only
// properties, generated properties, and measure properties.
absl::Status ValidatePropertyIsWritable(const GraphPropertyDefinition* prop_def,
                                        const ASTNode* error_location) {
  GOOGLESQL_RET_CHECK(prop_def->GetDeclaration().kind() !=
            GraphPropertyDeclaration::Kind::kInvalid);
  if (prop_def->GetDeclaration().kind() ==
      GraphPropertyDeclaration::Kind::kMeasure) {
    return MakeSqlErrorAt(error_location)
           << "Cannot insert into measure property '"
           << prop_def->GetDeclaration().Name() << "'";
  }

  GOOGLESQL_ASSIGN_OR_RETURN(const ResolvedExpr* val_expr,
                   prop_def->GetValueExpression());
  GOOGLESQL_RET_CHECK_NE(val_expr, nullptr)
      << "Property definition must have a value expression for DML to "
         "validate writability";

  if (!val_expr->Is<ResolvedCatalogColumnRef>()) {
    return MakeSqlErrorAt(error_location)
           << "Cannot insert into derived property '"
           << prop_def->GetDeclaration().Name() << "'";
  }

  const auto* col_ref = val_expr->GetAs<ResolvedCatalogColumnRef>();
  if (!col_ref->column()->IsWritableColumn()) {
    return MakeSqlErrorAt(error_location) << "Cannot insert into property '"
                                          << prop_def->GetDeclaration().Name()
                                          << "' mapped from a read-only column";
  }

  return absl::OkStatus();
}

// Returns an ordered de-duplicated list of requested unique label names in the
// label filter.
// Returns an error if the label filter is not present.
absl::StatusOr<std::vector<IdString>> ValidateAndGetUniqueInsertLabelNames(
    const ASTGraphInsertElementPatternFiller* filler,
    GraphElementType::ElementKind element_kind) {
  if (filler->label_filter() == nullptr) {
    return MakeSqlErrorAt(filler)
           << "Labels are required for new "
           << (element_kind == GraphElementType::kNode ? "node" : "edge")
           << " variable in INSERT";
  }

  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<IdString> label_names,
                   ExtractInsertLabelNames(filler->label_filter()));

  // Deduplicate and preserve order
  std::vector<IdString> unique_label_names;
  IdStringHashSetCase seen_labels;
  for (const auto& name : label_names) {
    if (seen_labels.insert(name).second) {
      unique_label_names.push_back(name);
    }
  }
  return unique_label_names;
}

template <typename T>
struct TargetTableAndLabels {
  const T* table = nullptr;
  std::vector<std::unique_ptr<const ResolvedGraphLabel>> labels;
};

// Finds exactly one table matching all the requested labels and returns the
// resolved labels.
// Returns a SQL error if no table matches or if multiple tables match
// (i.e. the matches are ambiguous).
template <typename T>
absl::StatusOr<TargetTableAndLabels<T>> ResolveTargetTableAndLabels(
    const absl::flat_hash_set<const T*>& tables,
    const std::vector<IdString>& unique_label_names,
    const ASTNode* error_location, GraphElementType::ElementKind element_kind,
    bool supports_dynamic_element_type) {
  std::vector<TargetTableAndLabels<T>> matches;
  bool seen_dynamic_label_table = false;
  for (const auto* table : tables) {
    bool has_all_requested_labels = true;
    std::vector<std::unique_ptr<const ResolvedGraphLabel>> target_labels;
    target_labels.reserve(unique_label_names.size());
    for (const auto& label_name : unique_label_names) {
      const GraphElementLabel* found_label = nullptr;
      std::unique_ptr<const ResolvedLiteral> resolved_label_name;
      if (table->FindLabelByName(label_name.ToString(), found_label).ok()) {
        // Static label.
        if (supports_dynamic_element_type) {
          GOOGLESQL_ASSIGN_OR_RETURN(resolved_label_name,
                           ResolvedLiteralBuilder()
                               .set_type(types::StringType())
                               .set_value(Value::String(label_name.ToString()))
                               .set_has_explicit_type(true)
                               .Build());
        }
        target_labels.push_back(MakeResolvedGraphLabel(
            found_label, std::move(resolved_label_name)));
      } else if (table->HasDynamicLabel() && supports_dynamic_element_type) {
        GOOGLESQL_RET_CHECK(!seen_dynamic_label_table)
            << "Multiple tables with dynamic labels are not supported";
        seen_dynamic_label_table = true;
        // Dynamic label.
        GOOGLESQL_ASSIGN_OR_RETURN(resolved_label_name,
                         ResolvedLiteralBuilder()
                             .set_type(types::StringType())
                             .set_value(Value::String(label_name.ToString()))
                             .set_has_explicit_type(true)
                             .Build());
        target_labels.push_back(MakeResolvedGraphLabel(
            found_label, std::move(resolved_label_name)));
      } else {
        has_all_requested_labels = false;
        break;
      }
    }
    if (has_all_requested_labels) {
      matches.push_back(TargetTableAndLabels<T>{
          .table = table, .labels = std::move(target_labels)});
    }
  }

  const absl::string_view element_type_str =
      (element_kind == GraphElementType::kNode) ? "node" : "edge";
  if (matches.empty()) {
    return MakeSqlErrorAt(error_location)
           << "No " << element_type_str
           << " table matches all requested labels";
  }
  if (matches.size() > 1) {
    return MakeSqlErrorAt(error_location)
           << "Ambiguous label specification matches multiple "
           << element_type_str << " tables";
  }

  return std::move(matches.front());
}

// Merges the input name list and variables resolved in the INSERT statement
// into a new NameList.
absl::StatusOr<std::shared_ptr<NameList>> MergeInsertVariablesToNameList(
    const NameList* input_name_list,
    const IdStringLinkedHashMapCase<ResolvedColumn>& variables) {
  auto new_name_list = std::make_shared<NameList>();
  if (input_name_list != nullptr) {
    for (const NamedColumn& col : input_name_list->columns()) {
      GOOGLESQL_RETURN_IF_ERROR(new_name_list->AddColumn(col.name(), col.column(),
                                               col.is_explicit()));
    }
  }
  for (const auto& [var_name, col] : variables) {
    NameTarget unused_target;
    GOOGLESQL_ASSIGN_OR_RETURN(bool found,
                     new_name_list->LookupName(var_name, &unused_target));
    if (!found) {
      GOOGLESQL_RETURN_IF_ERROR(
          new_name_list->AddColumn(var_name, col, /*is_explicit=*/true));
    }
  }
  return new_name_list;
}

// Checks if the variable is already defined in the current INSERT statement or
// in the input name scope.
// If the variable exists:
//  - Returns error if the variable is re-declared with labels or properties;
//  - Returns error if the variable is an edge `element_kind`;
//  - Returns error if the variable is a node `element_kind` but found an edge;
//  - Otherwise, returns the existing ResolvedColumn and add it to the named
//    `variables` map.
// If the variable does not exist, returns std::nullopt.
absl::StatusOr<std::optional<ResolvedColumn>> ResolveExistingVariableIfExists(
    const ASTGraphInsertElementPatternFiller* filler,
    const NameScope* input_scope, GraphElementType::ElementKind element_kind,
    IdStringLinkedHashMapCase<ResolvedColumn>& variables) {
  GOOGLESQL_RET_CHECK_NE(filler, nullptr)
      << "Insert element filler must always be present";
  const ASTIdentifier* ast_variable = filler->variable_name();
  // Anonymous variable is always a new variable and does not exist before.
  if (ast_variable == nullptr) {
    return std::nullopt;
  }
  IdString var_name = ast_variable->GetAsIdString();
  bool is_existing_name = false;

  // Check if the variable is already seen in the current INSERT statement or
  // in the input name scope.
  if (variables.contains(var_name)) {
    is_existing_name = true;
  } else if (input_scope != nullptr) {
    NameTarget target;
    GOOGLESQL_ASSIGN_OR_RETURN(bool found, input_scope->LookupName(var_name, &target));
    if (found) {
      is_existing_name = true;
      variables[var_name] = target.column();
    }
  }

  // Simply return, when the variable does not exist.
  if (!is_existing_name) {
    return std::nullopt;
  }

  // The variable already exists.
  if (filler->label_filter() != nullptr) {
    return MakeSqlErrorAt(ast_variable)
           << "Label specification is not allowed for existing variable '"
           << var_name << "' in INSERT";
  }
  if (filler->property_specification() != nullptr) {
    return MakeSqlErrorAt(ast_variable)
           << "Property specification is not allowed for existing "
              "variable '"
           << var_name << "' in INSERT";
  }
  // Edge variable cannot be referenced in INSERT.
  if (element_kind == GraphElementType::kEdge) {
    return MakeSqlErrorAt(ast_variable)
           << "Referencing an existing variable '" << var_name
           << "' as an edge in INSERT is not allowed";
  }
  // This is a node variable.
  const ResolvedColumn& existing_col = variables[var_name];
  const GraphElementType* existing_element_type =
      existing_col.type()->AsGraphElement();
  GOOGLESQL_RET_CHECK_NE(existing_element_type, nullptr);
  if (existing_element_type->element_kind() != element_kind) {
    return MakeSqlErrorAt(ast_variable)
           << "Variable '" << var_name
           << "' is declared as an edge elsewhere, but used as a node here";
  }
  return existing_col;
}

// Returns the matched GraphNodeTable if the given column corresponds to a node
// being inserted in the current statement, or nullptr otherwise.
const GraphNodeTable* GetInsertedNodeTable(
    const ResolvedColumn& node_col,
    absl::Span<const std::unique_ptr<const ResolvedComputedColumnBase>>
        insert_node_list) {
  for (const auto& computed_col_base : insert_node_list) {
    if (computed_col_base->column() == node_col) {
      const auto* computed_col =
          computed_col_base->GetAs<ResolvedComputedColumn>();
      const auto* insert_element =
          computed_col->expr()->GetAs<ResolvedGraphInsertElement>();
      return insert_element->element_table()->GetAs<GraphNodeTable>();
    }
  }
  return nullptr;
}

using GraphElementTableNameErrorFunction = absl::FunctionRef<std::string()>;

// Validates that the given key column is exposed as a writable property in the
// target element table. Otherwise, returns an error, as this means the element
// table is not updatable.
// `key_column_idx` is the index of the key column in the base table of the
// `target_table`.
absl::Status ValidateKeyColumnIsWritableProperty(
    const GraphElementTable* target_table, int key_column_idx,
    const ASTNode* error_location,
    GraphElementTableNameErrorFunction table_name_error_fn) {
  const Table* base_table = target_table->GetTable();
  GOOGLESQL_RET_CHECK(base_table != nullptr)
      << "Graph element table '" << target_table->Name()
      << "' has no underlying base table";

  const Column* key_col = base_table->GetColumn(key_column_idx);
  absl::flat_hash_set<const GraphPropertyDefinition*> prop_defs;
  GOOGLESQL_RETURN_IF_ERROR(target_table->GetPropertyDefinitions(prop_defs));

  for (const auto* prop_def : prop_defs) {
    // Skip measure properties.
    if (prop_def->GetDeclaration().kind() ==
        GraphPropertyDeclaration::Kind::kMeasure) {
      continue;
    }
    GOOGLESQL_ASSIGN_OR_RETURN(const ResolvedExpr* val_expr,
                     prop_def->GetValueExpression());
    GOOGLESQL_RET_CHECK_NE(val_expr, nullptr)
        << "Property definition must have a value expression for DML to "
           "validate writability";

    // Skip properties that are not catalog column references.
    if (!val_expr->Is<ResolvedCatalogColumnRef>()) {
      continue;
    }
    const auto* col_ref = val_expr->GetAs<ResolvedCatalogColumnRef>();
    if (col_ref->column() == key_col && col_ref->column()->IsWritableColumn()) {
      return absl::OkStatus();
    }
  }
  return MakeSqlErrorAt(error_location)
         << table_name_error_fn()
         << " is not updatable because the element key column '"
         << key_col->Name() << "' is not exposed as a writable property";
}

// Validates that all element key columns of the target graph element table are
// exposed as writable properties in its property definition list.
// If `target_table` is an edge table, the referenced source/destination element
// keys must also be exposed as properties of its source/destination node
// tables.
absl::Status ValidateTargetTableIsUpdatable(
    const GraphElementTable* target_table, const ASTNode* error_location) {
  // Element key columns include:
  // - columns in the KEY clause; and
  // - SOURCE KEY and DESTINATION KEY columns for edge tables.
  absl::btree_set<int> key_column_indices(target_table->GetKeyColumns().begin(),
                                          target_table->GetKeyColumns().end());
  GOOGLESQL_RET_CHECK(!key_column_indices.empty())
      << "Graph element table '" << target_table->Name()
      << "' has no key columns";

  if (target_table->kind() == GraphElementTable::Kind::kEdge) {
    GOOGLESQL_RET_CHECK_NE(target_table->AsEdgeTable(), nullptr);
    const GraphEdgeTable* edge_table = target_table->AsEdgeTable();
    key_column_indices.insert(
        edge_table->GetSourceNodeTable()->GetEdgeTableColumns().begin(),
        edge_table->GetSourceNodeTable()->GetEdgeTableColumns().end());
    key_column_indices.insert(
        edge_table->GetDestNodeTable()->GetEdgeTableColumns().begin(),
        edge_table->GetDestNodeTable()->GetEdgeTableColumns().end());
  }

  for (int key_idx : key_column_indices) {
    // An element table key column must be exposed as a property whose value
    // expression is a direct catalog column reference (not a derived property
    // or a measure property).
    GOOGLESQL_RETURN_IF_ERROR(ValidateKeyColumnIsWritableProperty(
        target_table, key_idx, error_location, [&target_table]() {
          return absl::StrCat("Graph element table '", target_table->Name(),
                              "'");
        }));
  }

  // For edge tables, also validate that the referenced source/destination node
  // tables are updatable.
  if (target_table->kind() == GraphElementTable::Kind::kEdge) {
    const GraphEdgeTable* edge_table = target_table->AsEdgeTable();
    const GraphNodeTable* source_node_table =
        edge_table->GetSourceNodeTable()->GetReferencedNodeTable();
    absl::btree_set<int> source_node_key_col_indices(
        edge_table->GetSourceNodeTable()->GetNodeTableColumns().begin(),
        edge_table->GetSourceNodeTable()->GetNodeTableColumns().end());
    for (int key_idx : source_node_key_col_indices) {
      GOOGLESQL_RETURN_IF_ERROR(ValidateKeyColumnIsWritableProperty(
          edge_table->GetSourceNodeTable()->GetReferencedNodeTable(), key_idx,
          error_location, [&source_node_table, &edge_table]() {
            return absl::StrCat(
                "The source node table '", source_node_table->Name(),
                "' of the edge table '", edge_table->Name(), "'");
          }));
    }

    const GraphNodeTable* dest_node_table =
        edge_table->GetDestNodeTable()->GetReferencedNodeTable();
    absl::btree_set<int> dest_node_key_col_indices(
        edge_table->GetDestNodeTable()->GetNodeTableColumns().begin(),
        edge_table->GetDestNodeTable()->GetNodeTableColumns().end());
    for (int key_idx : dest_node_key_col_indices) {
      GOOGLESQL_RETURN_IF_ERROR(ValidateKeyColumnIsWritableProperty(
          edge_table->GetDestNodeTable()->GetReferencedNodeTable(), key_idx,
          error_location, [&dest_node_table, &edge_table]() {
            return absl::StrCat(
                "The destination node table '", dest_node_table->Name(),
                "' of the edge table '", edge_table->Name(), "'");
          }));
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<std::vector<std::unique_ptr<const ResolvedGraphDMLPropertyItem>>>
GraphDmlResolver::ResolveInsertProperties(
    const ASTGraphPropertySpecification* ast_prop_spec,
    const GraphElementTable* target_table, const NameScope* input_scope) {
  std::vector<std::unique_ptr<const ResolvedGraphDMLPropertyItem>>
      property_items;
  if (ast_prop_spec == nullptr) {
    return property_items;
  }
  property_items.reserve(ast_prop_spec->property_name_and_value().size());

  IdStringHashSetCase seen_props;
  for (const ASTGraphPropertyNameAndValue* prop :
       ast_prop_spec->property_name_and_value()) {
    IdString prop_id = prop->property_name()->GetAsIdString();

    if (!seen_props.insert(prop_id).second) {
      return MakeSqlErrorAt(prop)
             << "Duplicate property '" << prop_id
             << "' is not allowed in INSERT property specification";
    }

    // First, try to find the static property definition in the target element
    // table.
    const GraphPropertyDefinition* static_prop_def = nullptr;
    absl::Status find_prop_status = target_table->FindPropertyDefinitionByName(
        prop_id.ToStringView(), static_prop_def);

    if (find_prop_status.ok()) {
      GOOGLESQL_RETURN_IF_ERROR(ValidatePropertyIsWritable(static_prop_def, prop));
    } else if (absl::IsNotFound(find_prop_status)) {
      if (!target_table->HasDynamicProperties()) {
        return MakeSqlErrorAt(prop)
               << "Property '" << prop_id << "' not found in table "
               << target_table->Name();
      }

      const GraphDynamicProperties* dynamic_properties = nullptr;
      GOOGLESQL_RETURN_IF_ERROR(target_table->GetDynamicProperties(dynamic_properties));
      GOOGLESQL_RET_CHECK(dynamic_properties != nullptr);

      GOOGLESQL_ASSIGN_OR_RETURN(const ResolvedExpr* val_expr,
                       dynamic_properties->GetValueExpression());
      GOOGLESQL_RET_CHECK_NE(val_expr, nullptr);
      GOOGLESQL_RET_CHECK(val_expr->Is<ResolvedCatalogColumnRef>());
      const auto* col_ref = val_expr->GetAs<ResolvedCatalogColumnRef>();
      if (!col_ref->column()->IsWritableColumn()) {
        return MakeSqlErrorAt(prop)
               << "Cannot insert dynamic property '" << prop_id
               << "' because the dynamic properties backing column '"
               << col_ref->column()->Name() << "' is read-only";
      }
    } else {
      GOOGLESQL_RETURN_IF_ERROR(find_prop_status);
    }

    std::unique_ptr<const ResolvedExpr> resolved_value_expr;
    auto expr_info = std::make_unique<ExprResolutionInfo>(
        input_scope, "INSERT property value");
    GOOGLESQL_RETURN_IF_ERROR(resolver_->ResolveExpr(prop->value(), expr_info.get(),
                                           &resolved_value_expr));

    if (static_prop_def != nullptr) {
      // Coerce the value expression to the property's static type using
      // implicit assignment semantics.
      GOOGLESQL_RETURN_IF_ERROR(resolver_->CoerceExprToType(
          prop->value(), static_prop_def->GetDeclaration().Type(),
          TypeModifiers(), Resolver::kImplicitAssignment,
          absl::StrCat("Cannot insert value of type $1 into property '",
                       prop_id.ToStringView(), "' of type $0"),
          &resolved_value_expr));
    }

    const GraphPropertyDeclaration* static_property_decl =
        static_prop_def != nullptr ? &static_prop_def->GetDeclaration()
                                   : nullptr;
    auto prop_item = MakeResolvedGraphDMLPropertyItem(
        prop_id.ToString(), static_property_decl,
        std::move(resolved_value_expr));
    property_items.push_back(std::move(prop_item));
  }
  return property_items;
}

absl::StatusOr<const GraphElementType*> GraphDmlResolver::BuildGraphElementType(
    const GraphElementTable* target_table,
    GraphElementType::ElementKind element_kind) {
  absl::flat_hash_set<const GraphPropertyDefinition*> prop_defs;
  GOOGLESQL_RETURN_IF_ERROR(target_table->GetPropertyDefinitions(prop_defs));

  std::vector<GraphElementType::PropertyType> property_types;
  property_types.reserve(prop_defs.size());
  for (const auto* def : prop_defs) {
    property_types.push_back(
        {def->GetDeclaration().Name(), def->GetDeclaration().Type()});
  }

  const GraphElementType* elem_type = nullptr;
  if (target_table->HasDynamicProperties()) {
    GOOGLESQL_RETURN_IF_ERROR(resolver_->type_factory_->MakeDynamicGraphElementType(
        graph_->NamePath(), element_kind, property_types, &elem_type));
  } else {
    GOOGLESQL_RETURN_IF_ERROR(resolver_->type_factory_->MakeGraphElementType(
        graph_->NamePath(), element_kind, property_types, &elem_type));
  }
  return elem_type;
}

absl::StatusOr<ResolvedGraphWithNameList<const ResolvedScan>>
GraphDmlResolver::ResolveGqlInsert(
    const ASTGqlInsert& ast_insert, const NameScope* input_scope,
    ResolvedGraphWithNameList<const ResolvedScan> input) {
  auto [input_scan, input_graph_name_lists] = std::move(input);
  auto input_name_list = input_graph_name_lists.singleton_name_list;

  // Keep track of all the variables resolved in the current INSERT statement.
  IdStringLinkedHashMapCase<ResolvedColumn> variables;

  // Fetch all the node and edge tables in the graph. This includes both static
  // and dynamic element tables.
  absl::flat_hash_set<const GraphNodeTable*> node_tables;
  GOOGLESQL_RETURN_IF_ERROR(graph_->GetNodeTables(node_tables));
  absl::flat_hash_set<const GraphEdgeTable*> edge_tables;
  GOOGLESQL_RETURN_IF_ERROR(graph_->GetEdgeTables(edge_tables));

  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
      insert_node_list;
  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
      insert_edge_list;
  std::vector<ResolvedColumn> path_element_list;

  for (const ASTGraphInsertPathPattern* ast_path : ast_insert.path_patterns()) {
    GOOGLESQL_RET_CHECK(!ast_path->elements().empty())
        << "Path pattern must have at least one element";
    GOOGLESQL_RET_CHECK_EQ(ast_path->elements().size() % 2, 1)
        << "Path pattern must have an odd number of elements";
    std::vector<ResolvedColumn> element_cols(ast_path->elements().size());

    // First pass: Resolve nodes
    for (int i = 0; i < ast_path->elements().size(); ++i) {
      const ASTGraphInsertElementPattern* ast_element = ast_path->elements(i);
      if (ast_element->Is<ASTGraphInsertEdgePattern>()) {
        GOOGLESQL_RET_CHECK_EQ(i % 2, 1) << "Expected edge pattern at odd index " << i;
        continue;
      }

      GOOGLESQL_RET_CHECK(ast_element->Is<ASTGraphInsertNodePattern>());
      GOOGLESQL_RET_CHECK_EQ(i % 2, 0) << "Expected node pattern at even index " << i;
      GOOGLESQL_ASSIGN_OR_RETURN(
          element_cols[i],
          ResolveInsertNodePattern(
              ast_element->GetAsOrDie<ASTGraphInsertNodePattern>(), input_scope,
              node_tables, variables, insert_node_list));
    }

    // Second pass: Resolve edges
    for (int i = 0; i < ast_path->elements().size(); ++i) {
      const ASTGraphInsertElementPattern* ast_element = ast_path->elements(i);
      if (ast_element->Is<ASTGraphInsertNodePattern>()) {
        continue;
      }

      const auto* edge_pattern =
          ast_element->GetAsOrDie<ASTGraphInsertEdgePattern>();
      GOOGLESQL_RET_CHECK(edge_pattern->orientation() == ASTGraphEdgePattern::LEFT ||
                edge_pattern->orientation() == ASTGraphEdgePattern::RIGHT);
      bool is_left = edge_pattern->orientation() == ASTGraphEdgePattern::LEFT;
      ResolvedColumn source_col =
          is_left ? element_cols[i + 1] : element_cols[i - 1];
      ResolvedColumn dest_col =
          is_left ? element_cols[i - 1] : element_cols[i + 1];
      GOOGLESQL_ASSIGN_OR_RETURN(
          element_cols[i],
          ResolveInsertEdgePattern(edge_pattern, input_scope, source_col,
                                   dest_col, edge_tables, variables,
                                   insert_node_list, insert_edge_list));
    }

    for (int i = 0; i < ast_path->elements().size(); ++i) {
      path_element_list.push_back(element_cols[i]);
    }
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      input_graph_name_lists.singleton_name_list,
      MergeInsertVariablesToNameList(input_name_list.get(), variables));

  GOOGLESQL_ASSIGN_OR_RETURN(
      auto resolved_insert,
      ResolvedGraphInsertScanBuilder()
          .set_column_list(
              input_graph_name_lists.singleton_name_list->GetResolvedColumns())
          .set_input_scan(std::move(input_scan))
          .set_insert_node_list(std::move(insert_node_list))
          .set_insert_edge_list(std::move(insert_edge_list))
          .set_path_element_list(std::move(path_element_list))
          .Build());

  return {{.resolved_node = std::move(resolved_insert),
           .graph_name_lists = std::move(input_graph_name_lists)}};
}

absl::StatusOr<ResolvedColumn> GraphDmlResolver::ResolveInsertNodePattern(
    const ASTGraphInsertNodePattern* node_pattern, const NameScope* input_scope,
    const absl::flat_hash_set<const GraphNodeTable*>& node_tables,
    IdStringLinkedHashMapCase<ResolvedColumn>& variables,
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
        insert_node_list) {
  const ASTGraphInsertElementPatternFiller* filler = node_pattern->filler();

  // Check if the variable is already defined in the current INSERT statement
  // or in the input name scope.
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::optional<ResolvedColumn> existing_var,
      ResolveExistingVariableIfExists(filler, input_scope,
                                      GraphElementType::kNode, variables));
  if (existing_var.has_value()) {
    return existing_var.value();
  }

  // The variable is new. It's a declaration.
  // Validate that label specification is not empty and get unique label names.
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::vector<IdString> unique_label_names,
      ValidateAndGetUniqueInsertLabelNames(filler, GraphElementType::kNode));

  // Find a single matching node table that has all the requested labels.
  GOOGLESQL_ASSIGN_OR_RETURN(auto result,
                   ResolveTargetTableAndLabels(
                       node_tables, unique_label_names, filler->label_filter(),
                       GraphElementType::kNode,
                       resolver_->language().LanguageFeatureEnabled(
                           FEATURE_SQL_GRAPH_DYNAMIC_ELEMENT_TYPE)));
  const GraphNodeTable* target_table = result.table;

  GOOGLESQL_RETURN_IF_ERROR(ValidateTargetTableIsUpdatable(target_table, filler));

  // Resolve the property specification.
  GOOGLESQL_ASSIGN_OR_RETURN(auto property_items,
                   ResolveInsertProperties(filler->property_specification(),
                                           target_table, input_scope));

  // Add the newly declared variable to the variables map.
  GOOGLESQL_ASSIGN_OR_RETURN(
      const GraphElementType* element_type,
      BuildGraphElementType(target_table, GraphElementType::kNode));
  const ASTIdentifier* ast_variable = filler->variable_name();
  ResolvedColumn element_col(resolver_->AllocateColumnId(),
                             /*table_name=*/kElementTableName,
                             /*name=*/ast_variable != nullptr
                                 ? ast_variable->GetAsIdString()
                                 : kInsertNodeName,
                             element_type);
  if (ast_variable != nullptr) {
    variables[ast_variable->GetAsIdString()] = element_col;
  }

  auto computed_col = MakeResolvedComputedColumn(
      element_col,
      MakeResolvedGraphInsertElement(element_type, std::move(property_items),
                                     std::move(result.labels), target_table));
  insert_node_list.push_back(std::move(computed_col));
  return element_col;
}

absl::StatusOr<ResolvedColumn> GraphDmlResolver::ResolveInsertEdgePattern(
    const ASTGraphInsertEdgePattern* edge_pattern, const NameScope* input_scope,
    const ResolvedColumn& source_col, const ResolvedColumn& dest_col,
    const absl::flat_hash_set<const GraphEdgeTable*>& edge_tables,
    IdStringLinkedHashMapCase<ResolvedColumn>& variables,
    const std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
        insert_node_list,
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
        insert_edge_list) {
  GOOGLESQL_RET_CHECK(source_col.type()->AsGraphElement()->IsNode());
  GOOGLESQL_RET_CHECK(dest_col.type()->AsGraphElement()->IsNode());
  const ASTGraphInsertElementPatternFiller* filler = edge_pattern->filler();

  // Check if the variable is already defined in the current INSERT statement
  // or in the input name scope. If so, it's an error because edge variable
  // cannot be referenced in INSERT.
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::optional<ResolvedColumn> existing_var,
      ResolveExistingVariableIfExists(filler, input_scope,
                                      GraphElementType::kEdge, variables));
  GOOGLESQL_RET_CHECK(!existing_var.has_value());

  // The variable is new. It's a declaration.
  // Validate that label specification is not empty and get unique label names.
  GOOGLESQL_ASSIGN_OR_RETURN(
      std::vector<IdString> unique_label_names,
      ValidateAndGetUniqueInsertLabelNames(filler, GraphElementType::kEdge));

  // Find a single matching edge table that has all the requested labels.
  GOOGLESQL_ASSIGN_OR_RETURN(auto result,
                   ResolveTargetTableAndLabels(
                       edge_tables, unique_label_names, filler->label_filter(),
                       GraphElementType::kEdge,
                       resolver_->language().LanguageFeatureEnabled(
                           FEATURE_SQL_GRAPH_DYNAMIC_ELEMENT_TYPE)));

  // Validate that the edge's expected source and destination node tables match
  // the provided source and destination node tables.
  const GraphEdgeTable* target_table = result.table;
  const GraphNodeTable* expected_source_node_table =
      target_table->GetSourceNodeTable()->GetReferencedNodeTable();
  const GraphNodeTable* expected_dest_node_table =
      target_table->GetDestNodeTable()->GetReferencedNodeTable();

  // Try to find the endpoint node element tables if any of the edge's endpoints
  // is newly inserted.
  const GraphNodeTable* actual_source_node_table =
      GetInsertedNodeTable(source_col, insert_node_list);
  const GraphNodeTable* actual_dest_node_table =
      GetInsertedNodeTable(dest_col, insert_node_list);

  // Validate the edge's source node.
  GOOGLESQL_RETURN_IF_ERROR(ValidateEdgeEndpoint(*edge_pattern, *target_table,
                                       actual_source_node_table,
                                       *expected_source_node_table, source_col,
                                       /*is_source=*/true));

  // Validate the edge's destination node.
  GOOGLESQL_RETURN_IF_ERROR(ValidateEdgeEndpoint(*edge_pattern, *target_table,
                                       actual_dest_node_table,
                                       *expected_dest_node_table, dest_col,
                                       /*is_source=*/false));

  GOOGLESQL_RETURN_IF_ERROR(ValidateTargetTableIsUpdatable(target_table, filler));

  // Resolve the property specification.
  GOOGLESQL_ASSIGN_OR_RETURN(auto property_items,
                   ResolveInsertProperties(filler->property_specification(),
                                           target_table, input_scope));

  // Add the newly declared variable to the variables map.
  GOOGLESQL_ASSIGN_OR_RETURN(
      const GraphElementType* element_type,
      BuildGraphElementType(target_table, GraphElementType::kEdge));
  const ASTIdentifier* ast_variable = filler->variable_name();
  ResolvedColumn elem_col(resolver_->AllocateColumnId(),
                          /*table_name=*/kElementTableName,
                          /*name=*/ast_variable != nullptr
                              ? ast_variable->GetAsIdString()
                              : kInsertEdgeName,
                          element_type);
  if (ast_variable != nullptr) {
    variables[ast_variable->GetAsIdString()] = elem_col;
  }

  auto source_col_ref = resolver_->MakeColumnRef(source_col);
  auto dest_col_ref = resolver_->MakeColumnRef(dest_col);
  auto computed_col = MakeResolvedComputedColumn(
      elem_col,
      MakeResolvedGraphInsertElement(
          element_type, std::move(property_items), std::move(result.labels),
          std::move(source_col_ref), std::move(dest_col_ref), target_table));
  insert_edge_list.push_back(std::move(computed_col));
  return elem_col;
}

absl::Status GraphDmlResolver::ValidateEdgeEndpoint(
    const ASTGraphInsertEdgePattern& error_location,
    const GraphEdgeTable& target_table, const GraphNodeTable* actual_node_table,
    const GraphNodeTable& expected_node_table, const ResolvedColumn& node_col,
    bool is_source) {
  absl::string_view node_kind = is_source ? "source" : "destination";
  if (actual_node_table != nullptr) {
    // For newly inserted node, verify if the actual node table matches the
    // expected node table.
    if (actual_node_table != &expected_node_table) {
      return MakeSqlErrorAt(&error_location)
             << "The actual " << node_kind << " node table "
             << actual_node_table->Name() << " does not match expected "
             << node_kind << " node table " << expected_node_table.Name()
             << " for edge table " << target_table.Name();
    }
  } else {
    // For referenced node variable, verify if the passed-in node type is a
    // supertype of the required node type.
    const GraphElementType* element_type = node_col.type()->AsGraphElement();
    const GraphElementType* required_element_type = nullptr;
    GOOGLESQL_ASSIGN_OR_RETURN(
        required_element_type,
        BuildGraphElementType(&expected_node_table, GraphElementType::kNode));
    if (!required_element_type->CoercibleTo(element_type)) {
      return MakeSqlErrorAt(&error_location)
             << "The actual " << node_kind << " node type "
             << node_col.type()->TypeName(resolver_->product_mode())
             << " does not match expected " << node_kind << " node table "
             << expected_node_table.Name();
    }
  }
  return absl::OkStatus();
}

}  // namespace googlesql
