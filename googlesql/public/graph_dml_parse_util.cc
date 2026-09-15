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

#include "googlesql/public/graph_dml_parse_util.h"

#include <any>

#include "googlesql/parser/parse_tree.h"
#include "googlesql/parser/parse_tree_visitor.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"

namespace googlesql {
namespace {

class GraphDmlVisitor : public DefaultParseTreeStatusVisitor {
 public:
  bool has_graph_dml() const { return has_graph_dml_; }

  absl::Status VisitASTGqlInsert(const ASTGqlInsert* node,
                                 std::any& output) override {
    has_graph_dml_ = true;
    // Stop visiting children once the first DML node is found.
    return absl::OkStatus();
  }

  absl::Status VisitASTGqlSet(const ASTGqlSet* node,
                              std::any& output) override {
    has_graph_dml_ = true;
    // Stop visiting children once the first DML node is found.
    return absl::OkStatus();
  }

  absl::Status VisitASTGqlRemove(const ASTGqlRemove* node,
                                 std::any& output) override {
    has_graph_dml_ = true;
    // Stop visiting children once the first DML node is found.
    return absl::OkStatus();
  }

  absl::Status VisitASTGqlDelete(const ASTGqlDelete* node,
                                 std::any& output) override {
    has_graph_dml_ = true;
    // Stop visiting children once the first DML node is found.
    return absl::OkStatus();
  }

  absl::Status Visit(const ASTNode* node, std::any& output) override {
    if (has_graph_dml_) {
      // Short-circuit traversal if a DML node has already been encountered
      return absl::OkStatus();
    }
    return DefaultVisit(node, output);
  }

 private:
  bool has_graph_dml_ = false;
};

}  // namespace

absl::StatusOr<bool> HasGraphDml(const ASTNode* root) {
  if (root == nullptr) {
    return false;
  }
  GraphDmlVisitor visitor;
  std::any unused_output;
  GOOGLESQL_RETURN_IF_ERROR(visitor.Visit(root, unused_output));
  return visitor.has_graph_dml();
}

}  // namespace googlesql
