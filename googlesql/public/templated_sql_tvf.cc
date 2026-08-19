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

#include "googlesql/public/templated_sql_tvf.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/analyzer/name_scope.h"
#include "googlesql/analyzer/resolver.h"
#include "googlesql/common/errors.h"
#include "googlesql/common/status_payload_utils.h"
#include "googlesql/parser/ast_node_kind.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/parser/parser.h"
#include "googlesql/proto/function.pb.h"
#include "googlesql/proto/internal_error_location.pb.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/cycle_detector.h"
#include "googlesql/public/deprecation_warning.pb.h"
#include "googlesql/public/error_helpers.h"
#include "googlesql/public/error_location.pb.h"
#include "googlesql/public/function.pb.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/input_argument_type.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/simple_table.pb.h"
#include "googlesql/public/strings.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_deserializer.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_ast_enums.pb.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/map_util.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

absl::Status TemplatedSQLTVF::Serialize(
    FileDescriptorSetMap* file_descriptor_set_map,
    TableValuedFunctionProto* proto) const {
  GOOGLESQL_RETURN_IF_ERROR(
      TableValuedFunction::Serialize(file_descriptor_set_map, proto));
  proto->set_type(FunctionEnums::TEMPLATED_SQL_TVF);
  for (const std::string& arg_name : GetArgumentNames()) {
    proto->add_argument_name(arg_name);
  }
  parse_resume_location_.Serialize(proto->mutable_parse_resume_location());
  return absl::OkStatus();
}

// static
absl::Status TemplatedSQLTVF::Deserialize(
    const TableValuedFunctionProto& proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result) {
  std::vector<std::string> path;
  for (const std::string& name : proto.name_path()) {
    path.push_back(name);
  }
  std::unique_ptr<FunctionSignature> signature;
  GOOGLESQL_ASSIGN_OR_RETURN(signature,
                   FunctionSignature::Deserialize(
                       proto.signature(), TypeDeserializer(factory, pools)));

  std::vector<std::string> arg_name_list;
  arg_name_list.reserve(proto.argument_name_size());
  for (const std::string& arg_name : proto.argument_name()) {
    arg_name_list.push_back(arg_name);
  }
  GOOGLESQL_RET_CHECK(proto.has_parse_resume_location()) << proto.DebugString();
  const ParseResumeLocation parse_resume_location =
      ParseResumeLocation::FromProto(proto.parse_resume_location());

  std::unique_ptr<TableValuedFunctionOptions> options;
  GOOGLESQL_RETURN_IF_ERROR(
      TableValuedFunctionOptions::Deserialize(proto.options(), &options));

  *result = std::make_unique<TemplatedSQLTVF>(path, *signature, arg_name_list,
                                              parse_resume_location, *options);

  if (proto.has_anonymization_info()) {
    GOOGLESQL_RET_CHECK(!proto.anonymization_info().userid_column_name().empty());
    const std::vector<std::string> userid_column_name_path = {
        proto.anonymization_info().userid_column_name().begin(),
        proto.anonymization_info().userid_column_name().end()};
    GOOGLESQL_RETURN_IF_ERROR(
        (*result)->GetAs<TemplatedSQLTVF>()->SetUserIdColumnNamePath(
            userid_column_name_path));
  }
  return absl::OkStatus();
}

std::shared_ptr<TemplatedSQLTVFSignature>
TemplatedSQLTVFSignature::CopyWithoutResolvedTemplatedQuery() const {
  std::shared_ptr<TemplatedSQLTVFSignature> copy =
      std::make_shared<TemplatedSQLTVFSignature>(
          input_arguments(), result_schema(), options(),
          /*resolved_templated_query=*/nullptr, arg_name_list_);
  std::optional<const AnonymizationInfo> anonymization_info =
      GetAnonymizationInfo();
  if (anonymization_info.has_value()) {
    copy->SetAnonymizationInfo(
        std::make_unique<AnonymizationInfo>(*anonymization_info));
  }
  return copy;
}

namespace {
static bool module_initialization_complete = []() {
  TableValuedFunction::RegisterDeserializer(
      FunctionEnums::TEMPLATED_SQL_TVF,
      TemplatedSQLTVF::Deserialize);
  return true;
} ();
}  // namespace

}  // namespace googlesql
