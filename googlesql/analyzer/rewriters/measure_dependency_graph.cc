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

#include "googlesql/analyzer/rewriters/measure_dependency_graph.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_visitor.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/linked_hash_map.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

namespace {

/* Helpers for analyzing measure expressions */

class ExpressionColumnNameCollector : public ResolvedASTVisitor {
 public:
  static absl::StatusOr<CaseInsensitiveStringSet> GetExpressionColumnNames(
      const ResolvedExpr& expr) {
    ExpressionColumnNameCollector collector;
    GOOGLESQL_RETURN_IF_ERROR(expr.Accept(&collector));
    return collector.column_names_;
  }

  absl::Status VisitResolvedExpressionColumn(
      const ResolvedExpressionColumn* node) override {
    column_names_.insert(node->name());
    return absl::OkStatus();
  }

 private:
  CaseInsensitiveStringSet column_names_;
};

// Returns the row identity column indices for `column`.
// If the column has its own row identity columns defined, returns them.
// Otherwise, returns the row identity columns of `table`.
//
// Input:
// - `column`: The measure column.
// - `table`: The table containing the column.
//
// Returns:
// - The row identity column indices.
absl::StatusOr<std::vector<int>> GetRowIdentityColumnIndices(
    const Column& column, const Table& table) {
  GOOGLESQL_RET_CHECK(column.HasMeasureExpression());
  if (std::optional<std::vector<int>> column_level_row_identity_columns =
          column.GetExpression()->RowIdentityColumns();
      column_level_row_identity_columns.has_value()) {
    GOOGLESQL_RET_CHECK(!column_level_row_identity_columns->empty())
        << "Row identity columns for measure " << column.Name()
        << " cannot be empty";
    return *column_level_row_identity_columns;
  }
  std::optional<std::vector<int>> table_level_row_identity_columns =
      table.RowIdentityColumns();
  GOOGLESQL_RET_CHECK(table_level_row_identity_columns.has_value() &&
            !table_level_row_identity_columns->empty())
      << "No row identity columns found for measure " << column.Name()
      << " in table " << table.Name();
  return *table_level_row_identity_columns;
}

// Returns the measure columns that `measure_expr` directly depends on.
// The returned columns are sorted by name case-insensitively.
//
// Input:
// - `measure_expr`: The measure definition expression.
// - `table`: The table containing the measures.
//
// Returns:
// - A list of dependency measure columns.
absl::StatusOr<std::vector<const Column*>> GetDependencyMeasures(
    const ResolvedExpr& measure_expr, const Table& table) {
  GOOGLESQL_ASSIGN_OR_RETURN(CaseInsensitiveStringSet referenced_column_names,
                   GetExpressionColumnNames(measure_expr));

  std::vector<std::string> sorted_names(referenced_column_names.begin(),
                                        referenced_column_names.end());
  std::sort(sorted_names.begin(), sorted_names.end(),
            googlesql_base::CaseLess());

  std::vector<const Column*> dependencies;
  for (const std::string& name : sorted_names) {
    const Column* dep_col = table.FindColumnByName(name);
    GOOGLESQL_RET_CHECK(dep_col != nullptr);
    if (dep_col->GetType()->IsMeasureType()) {
      dependencies.push_back(dep_col);
    }
  }
  return dependencies;
}

/* Helpers for topological sorting */

// Recursively computes the topological level of `node`.
// Level is stored in `levels` map to avoid recomputation.
//
// Input:
// - `node`: The node to compute level for. Must not be null.
// - `levels`: Map storing computed levels.
//
// Returns:
// - The level of the node.
int GetOrComputeLevel(
    const MeasureGraph::Node* node,
    absl::flat_hash_map<const MeasureGraph::Node*, int>& levels) {
  auto it = levels.find(node);
  if (it != levels.end()) return it->second;
  int max_dep_level = -1;
  for (const auto& [name, dep] : node->dependencies) {
    max_dep_level = std::max(max_dep_level, GetOrComputeLevel(dep, levels));
  }
  int level = max_dep_level + 1;
  levels[node] = level;
  return level;
}

}  // namespace

absl::StatusOr<CaseInsensitiveStringSet> GetExpressionColumnNames(
    const ResolvedExpr& expr) {
  return ExpressionColumnNameCollector::GetExpressionColumnNames(expr);
}

/* MeasureGraph Implementation */

absl::StatusOr<const MeasureGraph::Node*> MeasureGraph::AddIfNotPresent(
    const Column& c, const Table& table) {
  GOOGLESQL_RET_CHECK(c.GetType()->IsMeasureType());
  CaseInsensitiveStringSet visited;
  GOOGLESQL_RETURN_IF_ERROR(AddRecursively(c, table, visited));
  auto it = nodes_.find(c.Name());
  GOOGLESQL_RET_CHECK(it != nodes_.end());
  return it->second.get();
}

std::vector<std::vector<const MeasureGraph::Node*>>
MeasureGraph::TopologicallySortedNodes() const {
  absl::flat_hash_map<const Node*, int> levels;
  int max_level = -1;
  for (const auto& [name, node] : nodes_) {
    max_level = std::max(max_level, GetOrComputeLevel(node.get(), levels));
  }
  std::vector<std::vector<const Node*>> levels_nodes(max_level + 1);
  for (const auto& [name, node] : nodes_) {
    levels_nodes[levels.at(node.get())].push_back(node.get());
  }
  return levels_nodes;
}

absl::Status MeasureGraph::AddRecursively(const Column& c, const Table& table,
                                          CaseInsensitiveStringSet& visited) {
  if (nodes_.contains(c.Name())) {
    return absl::OkStatus();
  }

  if (visited.contains(c.Name())) {
    return absl::InternalError("Cycle detected in measure definitions");
  }

  visited.insert(c.Name());
  auto cleanup_visited =
      absl::MakeCleanup([&visited, &c] { visited.erase(c.Name()); });

  GOOGLESQL_RET_CHECK(c.HasMeasureExpression() &&
            c.GetExpression()->HasResolvedExpression());
  const ResolvedExpr* measure_expr = c.GetExpression()->GetResolvedExpression();
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<const Column*> dependencies,
                   GetDependencyMeasures(*measure_expr, table));

  for (const Column* dep : dependencies) {
    GOOGLESQL_RETURN_IF_ERROR(AddRecursively(*dep, table, visited));
  }

  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<int> row_identity_column_indices,
                   GetRowIdentityColumnIndices(c, table));

  auto node = std::make_unique<Node>(Node{
      .measure_type = c.GetType()->AsMeasure(),
      .name = c.Name(),
      .def_expr = measure_expr,
      .row_identity_column_indices = std::move(row_identity_column_indices),
  });

  for (const Column* dep : dependencies) {
    auto it = nodes_.find(dep->Name());
    GOOGLESQL_RET_CHECK(it != nodes_.end());
    node->dependencies[dep->Name()] = it->second.get();
  }

  nodes_[c.Name()] = std::move(node);

  return absl::OkStatus();
}

}  // namespace googlesql
