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

#ifndef GOOGLESQL_PUBLIC_TYPES_VECTOR_TYPE_UTIL_H_
#define GOOGLESQL_PUBLIC_TYPES_VECTOR_TYPE_UTIL_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace googlesql {

class Type;
class TypeFactory;

// String constant for VECTOR type name.
inline constexpr absl::string_view kVectorTypeName = "VECTOR";

// Returns true if `type` is the GoogleSQL built-in VECTOR type.
bool IsVectorType(const Type* type);

// Make a GoogleSQL built-in VECTOR type.
absl::StatusOr<const Type*> MakeVectorType(TypeFactory* type_factory);

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_TYPES_VECTOR_TYPE_UTIL_H_
