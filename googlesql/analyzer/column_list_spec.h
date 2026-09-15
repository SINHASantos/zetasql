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

#ifndef GOOGLESQL_ANALYZER_COLUMN_LIST_SPEC_H_
#define GOOGLESQL_ANALYZER_COLUMN_LIST_SPEC_H_

#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/id_string.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"

namespace googlesql {

// Represents a column_list_spec object.
// A column_list_spec is a list of unresolved column names, allowed as an
// argument in TVFs and UNPACK expressions. The enclosed expression must
// resolve to an array of non-empty non-null strings.
class ColumnListSpec {
 public:
  ColumnListSpec() = default;
  explicit ColumnListSpec(std::vector<IdString> column_names)
      : column_names_(std::move(column_names)) {}

  ColumnListSpec(ColumnListSpec&&) = default;
  ColumnListSpec& operator=(ColumnListSpec&&) = default;

  ColumnListSpec(const ColumnListSpec&) = delete;
  ColumnListSpec& operator=(const ColumnListSpec&) = delete;

  ~ColumnListSpec() = default;

  const Type* type() const { return types::ColumnListSpecType(); }

  const std::vector<IdString>& column_names() const { return column_names_; }

  std::vector<IdString> release_column_names() {
    return std::move(column_names_);
  }

  std::string DebugString() const {
    return absl::StrCat("ColumnListSpec(column_names=[",
                        absl::StrJoin(column_names_, ", ", IdStringFormatter),
                        "])");
  }

 private:
  std::vector<IdString> column_names_;
};

}  // namespace googlesql

#endif  // GOOGLESQL_ANALYZER_COLUMN_LIST_SPEC_H_
