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

#ifndef GOOGLESQL_ANALYZER_REWRITERS_MEASURE_DEPENDENCY_GRAPH_H_
#define GOOGLESQL_ANALYZER_REWRITERS_MEASURE_DEPENDENCY_GRAPH_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/types/measure_type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/base/case.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/linked_hash_map.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

inline constexpr int kReferencedColumnsFieldIndex = 0;
inline constexpr int kKeyColumnsFieldIndex = 1;
inline constexpr char kReferencedColumnsFieldName[] = "referenced_columns";
inline constexpr char kKeyColumnsFieldName[] = "key_columns";

using CaseInsensitiveStringSet =
    absl::flat_hash_set<std::string, googlesql_base::StringViewCaseHash,
                        googlesql_base::StringViewCaseEqual>;

template <typename ValueType>
using CaseInsensitiveMap =
    absl::flat_hash_map<std::string, ValueType,
                        googlesql_base::StringViewCaseHash,
                        googlesql_base::StringViewCaseEqual>;

// Returns the names of all columns referenced by `expr`.
absl::StatusOr<CaseInsensitiveStringSet> GetExpressionColumnNames(
    const ResolvedExpr& expr);

template <typename T>
class MeasureGraphVisitor;

// Represents a dependency graph of measure catalog `Column`s.
//
// A MeasureGraph is created for each measure source, i.e., each table/tvf scan,
// and each GetRowField that produces a measure.
//
// This is used to collect all the transitive measure dependencies and determine
// their correct evaluation layers (based on topological order) and closure
// struct types.
class MeasureGraph {
 public:
  // Represents a node in the measure dependency graph.
  //
  // It corresponds to a measure catalog column and holds information
  // needed to rewrite it.
  struct Node {
    // The MeasureType of the measure catalog column, retrieved from
    // Column::GetType().
    const MeasureType* measure_type;

    // The name of the measure catalog column, case insensitive, unique
    // identifier within the measure source table.
    std::string name;

    // The definition expression of the measure.
    const ResolvedExpr* def_expr;

    // The row identity column indices for this measure.
    std::vector<int> row_identity_column_indices;

    // Immediate dependency measure nodes that this measure references.
    CaseInsensitiveMap<const Node*> dependencies;
  };

  MeasureGraph() = default;
  MeasureGraph(const MeasureGraph&) = delete;
  MeasureGraph& operator=(const MeasureGraph&) = delete;
  MeasureGraph(MeasureGraph&&) = default;
  MeasureGraph& operator=(MeasureGraph&&) = default;

  // Adds a measure column `c` and its transitive dependency measures from
  // `table` to the graph, if not already present.
  // All dependee measures are expected to be in `table`.
  //
  // Input:
  // - `c`: The measure column to add.
  // - `table`: The table containing the measure column.
  //
  // Returns:
  // - The Node corresponding to `c` in the graph.
  // - InternalError if a dependency cycle is detected.
  // - Status representing other errors (e.g. missing dependency column).
  absl::StatusOr<const Node*> AddIfNotPresent(const Column& c,
                                              const Table& table);

  // Returns all nodes in the graph. The order is stable.
  std::vector<const Node*> nodes() const {
    std::vector<const Node*> result;
    result.reserve(nodes_.size());
    for (const auto& [name, node] : nodes_) {
      result.push_back(node.get());
    }
    return result;
  }

  // Returns the node with the given name, or nullptr if not found.
  const Node* FindNode(absl::string_view name) const {
    auto it = nodes_.find(name);
    if (it == nodes_.end()) {
      return nullptr;
    }
    return it->second.get();
  }

  // Returns nodes grouped by their topological level.
  //
  // Level 0 contains all base measures (measures that do not depend on other
  // measures). Level i contains measures that depend on measures in levels < i.
  //
  // Returns:
  // - A vector of vectors of Nodes, where the outer vector index corresponds to
  // the level.
  std::vector<std::vector<const Node*>> TopologicallySortedNodes() const;

  // Represents the result of visiting a node during topological traversal.
  template <typename T>
  struct TraversalResult {
    // The visited node.
    const Node* node;

    // The result computed by the visitor for this node.
    T result;
  };

  // Traverses the graph topologically, invoking the visitor on each node.
  //
  // The results of the visitation are stored level-by-level in the returned
  // vector of vectors of TraversalResult.
  //
  // Input:
  // - `visitor`: The visitor to invoke on each node.
  //
  // Returns:
  // - A 2D vector of TraversalResult, matching the topological levels of
  //   the graph.
  // - Status representing errors.
  template <typename T>
  absl::StatusOr<std::vector<std::vector<TraversalResult<T>>>>
  TopologicalTraversal(MeasureGraphVisitor<T>& visitor) const;

 private:
  // Transitively adds dependency measures of `c` from `table` to the graph.
  // `visited` is used to detect dependency cycles.
  absl::Status AddRecursively(const Column& c, const Table& table,
                              CaseInsensitiveStringSet& visited);

  // Use a linked hash map to have a stable traversal order. This ensures the
  // generated resolved AST is deterministic (specifically the order of fields
  // in the constructed closure structs), preventing test flakiness.
  // `Node` is wrapped in a `unique_ptr` to ensure pointer stability.
  absl::linked_hash_map<std::string, std::unique_ptr<Node>,
                        googlesql_base::StringViewCaseHash,
                        googlesql_base::StringViewCaseEqual>
      nodes_;
};

// Generic visitor interface for traversing a MeasureGraph.
//
// The template parameter `T` is the type of the result computed for each node.
template <typename T>
class MeasureGraphVisitor {
 public:
  virtual ~MeasureGraphVisitor() = default;

  // Computes the property value for a base measure node (level 0).
  //
  // Input:
  // - `base_node`: The base measure node to compute the property for.
  //
  // Returns:
  // - The computed result of type T.
  // - Status representing errors.
  virtual absl::StatusOr<T> ComputeBase(
      const MeasureGraph::Node& base_node) = 0;

  // Computes the value for a derived measure node (level > 0), given the
  // already computed results for its immediate dependencies.
  //
  // Input:
  // - `node`: The derived measure node to compute.
  // - `computed_dependencies`: Map from dependency node name to its computed
  //   result of type T.
  //
  // Returns:
  // - The computed result of type T.
  // - Status representing errors.
  virtual absl::StatusOr<T> ComputeDerived(
      const MeasureGraph::Node& node,
      const CaseInsensitiveMap<const T*>& computed_dependencies) = 0;
};

template <typename T>
absl::StatusOr<std::vector<std::vector<MeasureGraph::TraversalResult<T>>>>
MeasureGraph::TopologicalTraversal(MeasureGraphVisitor<T>& visitor) const {
  std::vector<std::vector<const Node*>> sorted_levels =
      TopologicallySortedNodes();

  // Use unique_ptr to ensure pointer stability of computed results during
  // traversal.
  CaseInsensitiveMap<std::unique_ptr<T>> cache;

  for (const auto& level_nodes : sorted_levels) {
    for (const Node* node : level_nodes) {
      GOOGLESQL_RET_CHECK(!cache.contains(node->name));
      if (node->dependencies.empty()) {
        GOOGLESQL_ASSIGN_OR_RETURN(T result, visitor.ComputeBase(*node));
        cache[node->name] = std::make_unique<T>(std::move(result));
      } else {
        CaseInsensitiveMap<const T*> computed_dependencies;
        for (const auto& [dep_name, dep_node] : node->dependencies) {
          auto it = cache.find(dep_name);
          GOOGLESQL_RET_CHECK(it != cache.end())
              << "Dependency " << dep_name << " not computed yet.";
          computed_dependencies.emplace(dep_name, it->second.get());
        }
        GOOGLESQL_ASSIGN_OR_RETURN(T result,
                         visitor.ComputeDerived(*node, computed_dependencies));
        cache[node->name] = std::make_unique<T>(std::move(result));
      }
    }
  }

  std::vector<std::vector<TraversalResult<T>>> traversal_results;
  traversal_results.reserve(sorted_levels.size());
  for (const auto& level_nodes : sorted_levels) {
    std::vector<TraversalResult<T>> level_results;
    level_results.reserve(level_nodes.size());
    for (const Node* node : level_nodes) {
      auto it = cache.find(node->name);
      GOOGLESQL_RET_CHECK(it != cache.end());
      level_results.push_back({node, std::move(*(it->second))});
    }
    traversal_results.push_back(std::move(level_results));
  }

  return traversal_results;
}

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_REWRITERS_MEASURE_DEPENDENCY_GRAPH_H_
