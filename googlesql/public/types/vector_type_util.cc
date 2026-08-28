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

#include "googlesql/public/types/vector_type_util.h"

#include <optional>
#include <string>
#include <utility>

#include "googlesql/public/options.pb.h"
#include "googlesql/public/types/builtin_declarative_types.h"
#include "googlesql/public/types/declarative_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/status/statusor.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

bool IsVectorType(const Type* type) {
  return type != nullptr && type->IsDeclarativeType() &&
         type->AsDeclarativeType()->IsGoogleSQLBuiltin(kVectorTypeName);
}

absl::StatusOr<const Type*> MakeVectorType(TypeFactory* type_factory) {
  std::optional<TypeParameterHandlers> type_parameter_handlers =
      GetBuiltinTypeParameterHandlers(kVectorTypeName);
  GOOGLESQL_RET_CHECK(type_parameter_handlers.has_value());
  return type_factory->MakeDeclarativeType(
      DeclarativeTypeDescriptor()
          .set_type_id({std::string(TypeId::kGoogleSqlNamespace),
                        std::string(kVectorTypeName)})
          .set_display_name(kVectorTypeName)
          .set_backing_type(type_factory->get_bytes())
          .set_returning_strategy(
              DeclarativeTypeDescriptor::ReturningDelegated{})
          .set_type_parameter_handlers(std::move(type_parameter_handlers))
          .set_additional_required_language_features({FEATURE_VECTOR_TYPE}));
}

}  // namespace googlesql
