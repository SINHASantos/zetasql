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

#include "googlesql/public/types/type_modifiers.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "googlesql/public/type_modifiers.pb.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/collation.h"
#include "googlesql/public/types/type_parameters.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

// static
TypeModifiers TypeModifiers::MakeTypeModifiers(TypeParameters type_parameters,
                                               Collation collation) {
  return TypeModifiers(std::move(type_parameters), std::move(collation));
}

TypeModifiers::TypeModifiers(const TypeModifiers& that)
    : type_parameters_(that.type_parameters_
                           ? new TypeParameters(*that.type_parameters_)
                           : nullptr),
      collation_(that.collation_ ? new Collation(*that.collation_) : nullptr) {}

TypeModifiers& TypeModifiers::operator=(const TypeModifiers& that) {
  if (this == &that) {
    return *this;
  }

  type_parameters_.reset(that.type_parameters_
                             ? new TypeParameters(*that.type_parameters_)
                             : nullptr);
  collation_.reset(that.collation_ ? new Collation(*that.collation_) : nullptr);
  return *this;
}

TypeModifiers::TypeModifiers(TypeParameters type_parameters,
                             Collation collation)
    : type_parameters_(
          std::make_unique<TypeParameters>(std::move(type_parameters))),
      collation_(std::make_unique<Collation>(std::move(collation))) {}

const TypeParameters& TypeModifiers::type_parameters() const {
  return type_parameters_ == nullptr ? TypeParameters::EmptyTypeParameters()
                                     : *type_parameters_;
}

const Collation& TypeModifiers::collation() const {
  return collation_ == nullptr ? Collation::EmptyCollation() : *collation_;
}

TypeParameters TypeModifiers::release_type_parameters() {
  if (type_parameters_ == nullptr) {
    return TypeParameters::EmptyTypeParameters();
  }
  TypeParameters result = std::move(*type_parameters_);
  type_parameters_.reset();
  return result;
}

Collation TypeModifiers::release_collation() {
  if (collation_ == nullptr) {
    return Collation::EmptyCollation();
  }
  Collation result = std::move(*collation_);
  collation_.reset();
  return result;
}

bool TypeModifiers::Equals(const TypeModifiers& that) const {
  return type_parameters().Equals(
             that.type_parameters(),
             /*default_timestamp_precision=*/std::nullopt) &&
         collation().Equals(that.collation());
}

bool TypeModifiers::EqualsWithDefaultTimestampPrecision(
    const TypeModifiers& that, int64_t default_timestamp_precision) const {
  return type_parameters().Equals(that.type_parameters(),
                                  default_timestamp_precision) &&
         collation().Equals(that.collation());
}

absl::StatusOr<bool> TypeModifiers::EqualsAnnotations(
    const AnnotationMap* annotation_map,
    int64_t default_timestamp_precision) const {
  GOOGLESQL_ASSIGN_OR_RETURN(bool equals_collation,
                   collation().EqualsCollationAnnotation(annotation_map));
  if (!equals_collation) {
    return false;
  }

  return type_parameters().EqualsAnnotations(annotation_map,
                                             default_timestamp_precision);
}

// static
absl::StatusOr<TypeModifiers> TypeModifiers::MakeTypeModifiers(
    const AnnotationMap* annotation_map) {
  GOOGLESQL_ASSIGN_OR_RETURN(TypeParameters type_parameters,
                   TypeParameters::MakeTypeParameters(annotation_map));
  Collation collation;
  if (annotation_map != nullptr) {
    GOOGLESQL_ASSIGN_OR_RETURN(collation, Collation::MakeCollation(*annotation_map));
  }
  return TypeModifiers(std::move(type_parameters), std::move(collation));
}

absl::Status TypeModifiers::Serialize(TypeModifiersProto* proto) const {
  GOOGLESQL_RETURN_IF_ERROR(
      type_parameters().Serialize(proto->mutable_type_parameters()));
  GOOGLESQL_RETURN_IF_ERROR(collation().Serialize(proto->mutable_collation()));
  return absl::OkStatus();
}

// static
absl::StatusOr<TypeModifiers> TypeModifiers::Deserialize(
    const TypeModifiersProto& proto) {
  TypeParameters type_params;
  if (proto.has_type_parameters()) {
    GOOGLESQL_ASSIGN_OR_RETURN(type_params,
                     TypeParameters::Deserialize(proto.type_parameters()));
  }
  Collation collation;
  if (proto.has_collation()) {
    GOOGLESQL_ASSIGN_OR_RETURN(collation, Collation::Deserialize(proto.collation()));
  }
  return TypeModifiers(std::move(type_params), std::move(collation));
}

std::string TypeModifiers::DebugString() const {
  if (IsEmpty()) {
    return absl::StrCat("null");
  }
  std::string debug_string;

  if (!type_parameters().IsEmpty()) {
    absl::StrAppend(&debug_string,
                    "type_parameters:", type_parameters().DebugString());
  }
  if (!collation().Empty()) {
    absl::StrAppend(&debug_string, debug_string.empty() ? "" : ", ",
                    "collation:", collation().DebugString());
  }
  return debug_string;
}

absl::StatusOr<TypeModifiers> TypeModifiers::GetChild(int i) const {
  TypeParameters child_type_parameters;
  if (type_parameters().num_children() > 0) {
    GOOGLESQL_RET_CHECK_LT(i, type_parameters().num_children());
    child_type_parameters = type_parameters().child(i);
  }

  Collation child_collation;
  if (collation().num_children() > 0) {
    GOOGLESQL_RET_CHECK_LT(i, collation().num_children());
    child_collation = collation().child(i);
  }

  return TypeModifiers(std::move(child_type_parameters),
                       std::move(child_collation));
}

}  // namespace googlesql
