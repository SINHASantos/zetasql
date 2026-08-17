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

#ifndef GOOGLESQL_ANALYZER_GRAPH_DDL_RESOLVER_H_
#define GOOGLESQL_ANALYZER_GRAPH_DDL_RESOLVER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/analyzer/name_scope.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/property_graph.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/base/case.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/linked_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace googlesql {

class CreatePropertyGraphStmtBuilder;
class Resolver;

// This class performs resolution for DDL statements related to
// PropertyGraph GoogleSQL analysis. Not thread-safe.
class GraphDdlResolver {
 public:
  explicit GraphDdlResolver(Resolver& resolver, IdStringPool& id_string_pool)
      : resolver_(resolver) {}
  ~GraphDdlResolver() = default;
  GraphDdlResolver(const GraphDdlResolver&) = delete;
  GraphDdlResolver& operator=(const GraphDdlResolver&) = delete;

  // Resolves a CREATE PROPERTY GRAPH statement.
  absl::Status ResolveCreatePropertyGraphStmt(
      const ASTCreatePropertyGraphStatement* ast_stmt,
      std::unique_ptr<ResolvedStatement>* output) const;

  // Resolves a CREATE PROPERTY GRAPH TYPE statement, the logical-only property
  // graph type DDL. Unlike ResolveCreatePropertyGraphStmt it resolves no
  // physical table bindings: only the element types, their default labels and
  // the property declarations exposed by those labels.
  absl::Status ResolveCreatePropertyGraphTypeStmt(
      const ASTCreatePropertyGraphTypeStatement* ast_stmt,
      std::unique_ptr<ResolvedStatement>* output) const;

  // This class represents a ResolvedGraphPropertyDeclaration with is_measure
  // property. It is used to validate that when there is a
  // ResolvedGraphPropertyDeclaration that is a measure, then there should not
  // be any other ResolvedGraphPropertyDeclaration with the same name.
  class PropertyDeclarationWithIsMeasure {
   public:
    PropertyDeclarationWithIsMeasure(
        std::unique_ptr<const ResolvedGraphPropertyDeclaration>
            property_declaration,
        bool is_measure)
        : property_declaration_(std::move(property_declaration)),
          is_measure_(is_measure) {}

    bool is_measure() const { return is_measure_; }

    const ResolvedGraphPropertyDeclaration& declaration() const {
      return *property_declaration_;
    }

    std::unique_ptr<const ResolvedGraphPropertyDeclaration>
    release_declaration() {
      return std::move(property_declaration_);
    }

    const std::string& name() const { return property_declaration_->name(); }

   private:
    std::unique_ptr<const ResolvedGraphPropertyDeclaration>
        property_declaration_;
    bool is_measure_;
  };

 private:
  template <typename T>
  using StringViewHashMapCase =
      absl::flat_hash_map<absl::string_view, T,
                          googlesql_base::StringViewCaseHash,
                          googlesql_base::StringViewCaseEqual>;

  using CaseInsensitiveStringLinkedHashSet =
      absl::linked_hash_set<std::string, googlesql_base::StringViewCaseHash,
                            googlesql_base::StringViewCaseEqual>;

  template <typename T>
  using CaseInsensitiveStringHashMap =
      absl::flat_hash_map<std::string, T, googlesql_base::StringViewCaseHash,
                          googlesql_base::StringViewCaseEqual>;

  using ResolvedGraphPropertyDefinitionMap = absl::flat_hash_map<
      const ASTGraphDerivedProperty*,
      std::unique_ptr<const ResolvedGraphPropertyDefinition>>;

  // The two constants below are used to limit the recursion resolution of
  // forward measure dependencies.
  //
  // The maximum number of forward dependencies for a measure property.
  static constexpr int kMaxGraphMeasureDependencies = 20;

  // The maximum depth of a forward measure property dependency chain.
  static constexpr int kMaxGraphMeasureRecursionDepth = 10;

  // Resolves `input_table_name` into a corresponding
  // ResolvedTableScan and inserts accessible names into
  // `input_table_scan_name_list`.
  absl::StatusOr<std::unique_ptr<const ResolvedTableScan>> ResolveBaseTable(
      const ASTPathExpression* input_table_name,
      NameListPtr& input_table_scan_name_list) const;

  // Resolves an element table definition `ast_element_table`.
  // Returns the resolved element table and all the labels and properties
  // declarations associated with it.
  //
  // `node_table_map` is used to resolve the referenced node tables for edge
  // tables.
  struct ElementTableWithLabelsAndProperties {
    std::unique_ptr<const ResolvedGraphElementTable> element_table;
    std::vector<std::unique_ptr<const ResolvedGraphElementLabel>> labels;
    std::vector<std::unique_ptr<PropertyDeclarationWithIsMeasure>>
        property_decls;
    std::vector<const ASTGraphInlinedEdgeDefinition*> ast_inlined_edges;
  };
  absl::StatusOr<ElementTableWithLabelsAndProperties> ResolveGraphElementTable(
      const ASTGraphElementTable* ast_element_table,
      GraphElementTable::Kind element_kind,
      const StringViewHashMapCase<const ResolvedGraphElementTable*>&
          node_table_map) const;

  // Resolves a single node or edge type definition `ast_element_type` in a
  // CREATE PROPERTY GRAPH TYPE statement. Returns the resolved element type
  // together with its default label and the property declarations exposed by
  // that label. `element_kind` selects node-type vs edge-type handling (only
  // edge types may carry FROM/TO references).
  struct ElementTypeWithLabelAndProperties {
    std::unique_ptr<const ResolvedGraphElementType> element_type;
    std::unique_ptr<const ResolvedGraphElementLabel> label;
    std::vector<std::unique_ptr<PropertyDeclarationWithIsMeasure>>
        property_decls;
  };
  absl::StatusOr<ElementTypeWithLabelAndProperties> ResolveGraphElementType(
      const ASTGraphElementType* ast_element_type,
      PropertyGraphElementType::Kind element_kind) const;

  // Resolves an inlined edge definition (`EDGE AS ... JOIN KEY (...) REFERENCES
  // <node>`) nested inside a node table into a ResolvedGraphElementTable of
  // edge kind. The edge reuses the enclosing node table's base table and keys.
  //
  // Must be called after all node tables in the graph are resolved, so that
  // `node_table_map` is fully populated and the REFERENCES target can be
  // resolved regardless of declaration order.
  absl::StatusOr<ElementTableWithLabelsAndProperties> ResolveInlinedEdge(
      const ASTGraphInlinedEdgeDefinition* ast_inlined_edge,
      const ASTGraphElementTable* ast_enclosing_node_table,
      const ResolvedGraphElementTable* enclosing_node_table,
      const StringViewHashMapCase<const ResolvedGraphElementTable*>&
          node_table_map) const;

  // Resolves the `ast_node_table_ref` into a ResolvedGraphNodeTableReference.
  //
  // `node_table_map` is used to resolve the referenced node tables.
  absl::StatusOr<std::unique_ptr<const ResolvedGraphNodeTableReference>>
  ResolveGraphNodeTableReference(
      const ASTGraphNodeTableReference* ast_node_table_ref,
      const ResolvedTableScan& edge_table_scan,
      const StringViewHashMapCase<const ResolvedGraphElementTable*>&
          node_table_map) const;

  // Resolves a single label with its `properties` and `label_options`.
  struct LabelAndProperties {
    std::unique_ptr<const ResolvedGraphElementLabel> label;
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
        property_defs;
  };
  absl::StatusOr<LabelAndProperties> ResolveLabelAndProperties(
      const ASTNode& ast_location, absl::string_view label_name,
      const ASTGraphProperties& properties, const ASTOptionsList* label_options,
      const ResolvedTableScan& base_table_scan,
      const NameScope* input_scope) const;

  // Resolves all label and property definitions in `ast_label_properties_list`.
  // `default_label_options` is the syntactic sugar form of DEFAULT LABEL
  // OPTIONS clause. If present, the explicit DEFAULT LABEL clause cannot have
  // OPTIONS defined.
  struct LabelAndPropertiesList {
    std::vector<std::unique_ptr<const ResolvedGraphElementLabel>> labels;
    std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
        property_defs;
  };
  absl::StatusOr<LabelAndPropertiesList> ResolveLabelAndPropertiesList(
      const ASTGraphElementLabelAndPropertiesList& ast_label_properties_list,
      const ASTOptionsList* default_label_options, IdString element_table_alias,
      const ResolvedTableScan& base_table_scan,
      const NameScope* input_scope) const;

  // Resolves `ast_properties` into a list of ResolvedGraphPropertyDefinitions.
  absl::StatusOr<
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
  ResolveGraphProperties(const ASTGraphProperties* ast_properties,
                         const ResolvedTableScan& base_table_scan,
                         const NameScope* input_scope) const;

  // Resolves `properties` into a list of ResolvedGraphPropertyDefinitions.
  // Used by ResolveGraphProperties.
  absl::StatusOr<
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
  ResolveGraphPropertyList(
      const ASTNode* ast_location,
      absl::Span<const ASTGraphDerivedProperty* const> properties,
      const ResolvedTableScan& base_table_scan,
      const NameScope* input_scope) const;

  // Resolves all columns from `base_table_scan` into a list of
  // ResolvedGraphPropertyDefinitions: excluding the ones specified in
  // `all_except_column_list`.
  //
  // Used by ResolveGraphProperties to resolve the
  // PROPERTIES ALL COLUMNS [EXCEPT (...)] syntax.
  absl::StatusOr<
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>>
  ResolveGraphPropertiesAllColumns(
      const ASTNode* ast_location, const ASTColumnList* all_except_column_list,
      const ResolvedTableScan& base_table_scan) const;

  absl::StatusOr<std::unique_ptr<const ResolvedGraphPropertyDefinition>>
  ResolveGraphProperty(const ASTGraphDerivedProperty* property,
                       ExprResolutionInfo* expr_resolution_info) const;

  absl::Status ValidateGraphPropertyList(
      absl::Span<const ASTGraphDerivedProperty* const> properties) const;

  // Resolves a measure property recursively, handling forward declarations and
  // tracking visited names to detect dependency cycles.
  //
  // Inputs:
  // - `property`: The measure property to resolve.
  // - `input_scope`: The scope for resolving the expression.
  // - `ast_location`: AST location for general errors.
  // - `visited`: Set of measure names visited on the current path.
  // - `measure_properties`: All measure properties declared in the current
  //   context, including unresolved ones.
  //
  // Output:
  // - `resolved_properties`: Stores the resolved measure property definitions.
  //
  // The algorithm uses a trial-and-error approach to resolve forward
  // declarations. It tries to resolve the measure property directly. If that
  // fails, it checks if the error is due to an unrecognized name that may be a
  // forward measure property declaration. If so, it recursively tries to
  // resolve the dependency first, and if that succeeds, it registers the
  // dependency measure property and retries resolving the original measure
  // property.
  //
  // This trial-and-error approach is necessary because it is not possible to
  // determine whether a name is a forward measure property declaration or not
  // without trying to resolve it. For example, consider the following:
  //
  // ```
  // MEASURE((SELECT 1 AS a) + 1) AS b,
  // MEASURE(SUM(x)) AS a
  // ```
  //
  // The `a` in the definition expression of `b` is not a reference to the
  // measure property `a`, but we cannot know this until we actually resolve it.
  //
  // The time complexity of this function is O((V + E) * E * L), where
  // - V is the number of measure properties, and
  // - E is the number of dependencies between the measure properties, and
  // - L is the time to resolve a single measure property.
  //
  // TODO: b/508012465 - We should improve the time complexity to O((V + E) * L)
  // by avoiding the repeated resolution of the same measure property.
  absl::Status ResolveGraphMeasurePropertyRecursive(
      const ASTGraphDerivedProperty* property, const NameScope* input_scope,
      CaseInsensitiveStringLinkedHashSet& visited,
      const CaseInsensitiveStringHashMap<const ASTGraphDerivedProperty*>&
          measure_properties,
      ResolvedGraphPropertyDefinitionMap& resolved_properties) const;

  // Dedupes `property_defs` in place by property declaration name, then builds
  // a ResolvedGraphPropertyDeclaration for each remaining definition (with the
  // definition's parse location and is_measure flag).
  absl::StatusOr<std::vector<std::unique_ptr<PropertyDeclarationWithIsMeasure>>>
  BuildPropertyDeclarations(
      std::vector<std::unique_ptr<const ResolvedGraphPropertyDefinition>>&
          property_defs) const;

  Resolver& resolver_;
};

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_GRAPH_DDL_RESOLVER_H_
