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

#ifndef GOOGLESQL_PUBLIC_GRAPH_DML_PARSE_UTIL_H_
#define GOOGLESQL_PUBLIC_GRAPH_DML_PARSE_UTIL_H_

#include "googlesql/parser/parse_tree.h"
#include "absl/status/statusor.h"

namespace googlesql {

// Returns true if the given parsed AST contains any Graph DML operator
// (`ASTGqlInsert`, `ASTGqlSet`, `ASTGqlRemove`, or `ASTGqlDelete`).
absl::StatusOr<bool> HasGraphDml(const ASTNode* root);

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_GRAPH_DML_PARSE_UTIL_H_
