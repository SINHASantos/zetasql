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

#ifndef GOOGLESQL_PUBLIC_TYPES_BUILTIN_DECLARATIVE_TYPES_H_
#define GOOGLESQL_PUBLIC_TYPES_BUILTIN_DECLARATIVE_TYPES_H_

#include <optional>

#include "googlesql/public/types/declarative_type.h"
#include "absl/strings/string_view.h"

namespace googlesql {

// This file is a registry for GoogleSQL built-in DeclarativeTypes. It is the
// source of truth for the specifications of those types.

// Retrieves the handlers for the declarative built-in type with the given
// `type_id`. Returns nullptr if no such handlers exist (therefore the type does
// not support type parameters).
//
// `type_id` is the "local_id" part of the built-in type's DeclarativeTypeId.
// The namespace is assumed to be "GoogleSQL".
std::optional<TypeParameterHandlers> GetBuiltinTypeParameterHandlers(
    absl::string_view type_id);

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_TYPES_BUILTIN_DECLARATIVE_TYPES_H_
