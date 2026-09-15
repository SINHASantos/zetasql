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

#ifndef GOOGLESQL_PUBLIC_TIME_SERIES_TVF_UTIL_H_
#define GOOGLESQL_PUBLIC_TIME_SERIES_TVF_UTIL_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"

namespace googlesql {

class Type;
class TVFRelation;

// Represents a single step (either index-based for STRUCT or descriptor-based
// for PROTO) required to traverse into a nested struct or proto field.
struct TypeFieldPathStep {
  enum Kind { STRUCT_FIELD, PROTO_FIELD };
  Kind kind;
  // Index of the field if it is a STRUCT_FIELD.
  int struct_field_index = -1;
  // Descriptor of the field if it is a PROTO_FIELD.
  const google::protobuf::FieldDescriptor* proto_field_descriptor = nullptr;
  // The type of the field if kind == STRUCT_FIELD, or nullptr for PROTO_FIELD.
  // Callers requiring a Type* for proto fields should resolve
  // proto_field_descriptor using their own TypeFactory.
  const Type* type = nullptr;
};

struct ResolvedTimestampColumnPath {
  // The index of the resolved column in the TVFRelation. For SQL tables, this
  // is the index of the column in the input table. For value tables, this will
  // be set to 0.
  int column_index = -1;
  // Steps to traverse from the top-level column to the leaf timestamp field.
  std::vector<TypeFieldPathStep> steps;
};

// Convert a dotted identifier path string to a vector of path components,
// unquoting identifiers if necessary.
//
// Reserved keywords (such as SELECT, FROM, or INTERVAL) are not treated as
// reserved and are parsed as an identifier, even at the start of the path.
//
// Unquoted slashes ('/') and backslashes ('\') are not
// supported. Quoted components may contain these characters, but backslashes
// are treated as literals and not processed as escape sequences. (e.g.
// "`a\\b`.c" => {"a\\b", "c"})
//
//  Examples:
//   "abc" => {"abc"}
//   "abc.def" => {"abc", "def"}
//   "select.from.interval" => {"select", "from", "interval"}
//   "abc.`def`" => {"abc", "def"}
//   "`abc.def`.ghi" => {"abc.def", "ghi"}
//   "abc/def" => error
//   "abc..def" => error
//
// Returned errors are of status InvalidArgument, appropriate for returning to
// the user at analysis-time.
// - Path structure errors:
//   - if the input string is empty.
//   - if the input begins or ends with '.'.
//   - if the path contains empty components (e.g. "a..b", "abc.``").
//   - if the path contains unclosed backticks (e.g. "a.`b").
// - Quoted component errors:
//   - if any backslash-enclosed component is not immediately preceded by the
//     start of the string or a dot ('.') (e.g. "a`b`").
//   - if any backslash-enclosed component is not immediately followed by a dot
//     ('.') or the end of the string (e.g. "`a`bc").
// - Unquoted component errors:
//   - if the first component starts with a digit (e.g. "123.abc").
//   - if it contains a slash ('/') or backslash ('\') character.
//   - if it contains any character that is not ASCII alphanumeric or an
//     underscore (e.g. "a*b", "%b", "a#", "你好").
absl::StatusOr<std::vector<std::string>> ParseSimpleIdentifierPath(
    absl::string_view path);

// Resolves and validates a timestamp field path (e.g. "column.field") against
// an input TVFRelation.
// - SQL Table:
//   The first component of the path is resolved case-insensitively against the
//   top-level columns of the relation. Subsequent path components are resolved
//   as fields within that column (e.g. "column.subfield"). If the path has
//   only one component, it is matched directly with a top-level column.
// - Value Table:
//   The caller should ensure that the value table type is a STRUCT or PROTO.
//   The first path component (and subsequent ones) are resolved as fields
//   within that value table.
//
// This function serves two main purposes:
// 1. It verifies that the path is structurally valid following the rules of
//    ParseSimpleIdentifierPath(). Validates that there are no repeated fields
//    or map fields in the path, and that the leaf field type or column type
//    is a TIMESTAMP or google.protobuf.Timestamp (or a field annotated with
//    GoogleSQL TIMESTAMP).
// 2. It returns the column index and the sequential steps (struct field indices
//    or protobuf field descriptors) required by query engines and evaluators to
//    extract the leaf value from the column or field of the input relation.
//
// Returned errors are InvalidArgument Status. The caller should associate the
// error with the timestamp column AST location.
absl::StatusOr<ResolvedTimestampColumnPath> ResolveTimestampColumnPath(
    const TVFRelation& input_relation, absl::string_view path_string);

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_TIME_SERIES_TVF_UTIL_H_
