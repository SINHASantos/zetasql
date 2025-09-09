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

#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status GetStandaloneBuiltinEnumTypes(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToTypeMap* types) {
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_MULTIWAY_UNNEST)) {
    const Type* array_zip_mode_type = types::ArrayZipModeEnumType();
    ZETASQL_RETURN_IF_ERROR(InsertType(types, options, array_zip_mode_type));
  }
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_BITWISE_AGGREGATE_BYTES_SIGNATURES)) {
    const Type* bitwise_agg_mode_type = types::BitwiseAggModeEnumType();
    ZETASQL_RETURN_IF_ERROR(InsertType(types, options, bitwise_agg_mode_type));
  }
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_KLL_QUANTILES_EXTRACT_RELATIVE_RANK)) {
    ZETASQL_ASSIGN_OR_RETURN(const Type* rank_type_type, types::RankTypeEnumType());
    ZETASQL_RETURN_IF_ERROR(InsertType(types, options, rank_type_type));
  }
  return absl::OkStatus();
}

}  // namespace zetasql
