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

#include "googlesql/public/types/builtin_declarative_types.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/common/errors.h"
#include "googlesql/public/proto/vector_encoding_id.pb.h"
#include "googlesql/public/types/declarative_type.h"
#include "googlesql/public/types/type_parameters.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

static absl::StatusOr<TypeParameters> ResolveVectorTypeParameters(
    const std::vector<TypeParameterValue>& resolved_type_parameter_list,
    ProductMode mode) {
  GOOGLESQL_RET_CHECK(!resolved_type_parameter_list.empty());
  if (resolved_type_parameter_list.size() > 2) {
    return MakeSqlError() << "VECTOR type has too many type parameters. Found "
                          << resolved_type_parameter_list.size()
                          << " parameters";
  }
  const TypeParameterValue& param = resolved_type_parameter_list[0];
  if (param.IsSpecialLiteral() || !param.GetValue().has_int64_value()) {
    return MakeSqlError()
           << "VECTOR length parameter must be an integer literal";
  }
  int64_t length = param.GetValue().int64_value();
  if (length <= 0) {
    return MakeSqlError() << "VECTOR length must be greater than 0";
  }
  VectorTypeParametersProto proto;
  proto.set_length(length);

  // Try parsing the encoding parameter.
  if (resolved_type_parameter_list.size() > 1) {
    const TypeParameterValue& encoding_param = resolved_type_parameter_list[1];
    if (encoding_param.IsSpecialLiteral() ||
        !encoding_param.GetValue().has_string_value()) {
      return MakeSqlError()
             << "VECTOR encoding parameter must be a string literal";
    }
    std::string encoding_str = encoding_param.GetValue().string_value();
    absl::AsciiStrToUpper(&encoding_str);
    googlesql::VectorEncodingId::Id encoding_enum;
    if (!googlesql::VectorEncodingId_Id_Parse(encoding_str, &encoding_enum) ||
        encoding_enum == googlesql::VectorEncodingId::UNKNOWN_VECTOR_ENCODING) {
      return MakeSqlError()
             << R"(Unrecognized VECTOR encoding: ")"
             << encoding_param.GetValue().string_value() << R"(")";
    }
    proto.set_encoding(encoding_enum);
  }

  return TypeParameters::MakeVectorTypeParameters(proto);
}

static absl::Status ValidateVectorTypeParameters(
    const TypeParameters& type_parameters, ProductMode mode) {
  if (type_parameters.IsEmpty()) {
    return absl::OkStatus();
  }
  GOOGLESQL_RET_CHECK(type_parameters.IsVectorTypeParameters());
  return TypeParameters::ValidateVectorTypeParameters(
      *type_parameters.vector_type_parameters());
}

static absl::flat_hash_map<absl::string_view, TypeParameterHandlers>
InitBuiltinTypeParameterHandlers() {
  absl::flat_hash_map<absl::string_view, TypeParameterHandlers> handlers_map;
  absl::StatusOr<TypeParameterHandlers> vector_handlers =
      TypeParameterHandlers::Create(&ResolveVectorTypeParameters,
                                    &ValidateVectorTypeParameters);
  if (vector_handlers.ok()) {
    handlers_map.emplace("VECTOR", *std::move(vector_handlers));
  }
  return handlers_map;
}

static const absl::flat_hash_map<absl::string_view, TypeParameterHandlers>&
GetBuiltinTypeParameterHandlersMap() {
  static const absl::NoDestructor<
      absl::flat_hash_map<absl::string_view, TypeParameterHandlers>>
      kBuiltinTypeParameterHandlers(InitBuiltinTypeParameterHandlers());
  return *kBuiltinTypeParameterHandlers;
}

std::optional<TypeParameterHandlers> GetBuiltinTypeParameterHandlers(
    absl::string_view type_id) {
  const auto& handlers_map = GetBuiltinTypeParameterHandlersMap();
  auto it = handlers_map.find(type_id);
  if (it == handlers_map.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace googlesql
