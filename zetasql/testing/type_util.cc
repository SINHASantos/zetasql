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

#include "zetasql/testing/type_util.h"

#include <string>
#include <vector>

#include "google/protobuf/duration.pb.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/latlng.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/descriptor.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/testdata/test_proto3.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/base/check.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/compiler/importer.h"
#include "google/protobuf/descriptor.h"

namespace zetasql {
namespace testing {

bool HasFloatingPointNumber(const zetasql::Type* type) {
  if (type->IsArray()) {
    return HasFloatingPointNumber(type->AsArray()->element_type());
  } else if (type->IsStruct()) {
    for (int i = 0; i < type->AsStruct()->num_fields(); i++) {
      if (HasFloatingPointNumber(type->AsStruct()->field(i).type)) {
        return true;
      }
    }
    return false;
  } else if (type->kind() == zetasql::TYPE_DOUBLE ||
             type->kind() == zetasql::TYPE_FLOAT) {
    return true;
  } else {
    return false;
  }
}

std::vector<const Type*> ZetaSqlComplexTestTypes(
    zetasql::TypeFactory* type_factory,
    google::protobuf::compiler::Importer* importer) {
  for (const std::string& filepath : ZetaSqlTestProtoFilepaths()) {
    importer->Import(filepath);
  }

  std::vector<const Type*> output;

  for (const std::string& proto_name : ZetaSqlRandomTestProtoNames()) {
    const google::protobuf::Descriptor* descriptor =
        importer->pool()->FindMessageTypeByName(proto_name);
    ABSL_CHECK(descriptor != nullptr)
        << "Cannot fine Proto Message Type: " << proto_name
        << ", available files: "
        << absl::StrJoin(ZetaSqlTestProtoFilepaths(), ",");

    const Type* proto_type;
    ZETASQL_CHECK_OK(type_factory->MakeProtoType(descriptor, &proto_type));
    output.push_back(proto_type);
  }

  for (const std::string& enum_name : ZetaSqlTestEnumNames()) {
    const google::protobuf::EnumDescriptor* descriptor =
        importer->pool()->FindEnumTypeByName(enum_name);
    ABSL_CHECK(descriptor != nullptr)
        << "Cannot fine Enum Type: " << enum_name << ", available files: "
        << absl::StrJoin(ZetaSqlTestProtoFilepaths(), ",");
    const Type* enum_type;
    ZETASQL_CHECK_OK(type_factory->MakeEnumType(descriptor, &enum_type));
    output.push_back(enum_type);
  }

  const Type* struct_int64_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      {{"int64_val", zetasql::types::Int64Type()}}, &struct_int64_type));
  output.push_back(struct_int64_type);

  return output;
}

std::vector<const Type*> ZetaSqlComplexTestTypes(
    zetasql::TypeFactory* type_factory) {
  std::vector<const Type*> output;
  std::vector<const google::protobuf::Descriptor*> proto_descriptors = {
      zetasql_test__::KitchenSinkPB::descriptor(),
      google::protobuf::Duration::descriptor(),
      google::protobuf::Timestamp::descriptor(),
      google::type::Date::descriptor(),
      google::type::TimeOfDay::descriptor(),
      google::type::LatLng::descriptor(),
      google::protobuf::DoubleValue::descriptor(),
      google::protobuf::FloatValue::descriptor(),
      google::protobuf::Int64Value::descriptor(),
      google::protobuf::UInt64Value::descriptor(),
      google::protobuf::Int32Value::descriptor(),
      google::protobuf::UInt32Value::descriptor(),
      google::protobuf::BoolValue::descriptor(),
      google::protobuf::StringValue::descriptor(),
      google::protobuf::BytesValue::descriptor()};
  for (const auto& descriptor : proto_descriptors) {
    const Type* proto_type;
    ZETASQL_CHECK_OK(type_factory->MakeProtoType(descriptor, &proto_type));
    output.push_back(proto_type);
  }

  std::vector<const google::protobuf::EnumDescriptor*> enum_descriptors = {
      zetasql_test__::TestEnum_descriptor()};
  for (const auto& descriptor : enum_descriptors) {
    const Type* enum_type;
    ZETASQL_CHECK_OK(type_factory->MakeEnumType(descriptor, &enum_type));
    output.push_back(enum_type);
  }

  const Type* struct_int64_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      {{"int64_val", zetasql::types::Int64Type()}}, &struct_int64_type));
  output.push_back(struct_int64_type);

  return output;
}

std::vector<std::string> ZetaSqlTestProtoFilepaths() {
  // `rounding_mode`, `array_find_mode`, `array_zip_mode`, `bitwise_agg_mode`
  // fixes `Enum not found` error in RQG / RSG: b/293474126.
  return {"zetasql/public/functions/rounding_mode.proto",
          "zetasql/public/functions/array_find_mode.proto",
          "zetasql/public/functions/array_zip_mode.proto",
          "zetasql/public/functions/bitwise_agg_mode.proto",
          "zetasql/testdata/test_schema.proto",
          "zetasql/testdata/test_proto3.proto",
          "google/protobuf/duration.proto",
          "google/protobuf/timestamp.proto",
          "google/protobuf/wrappers.proto",
          "google/type/latlng.proto",
          "google/type/timeofday.proto",
          "google/type/date.proto"};
}

std::vector<std::string> ZetaSqlTestProtoNames() {
  return {"zetasql_test__.KitchenSinkPB",
          "zetasql_test__.MessageWithMapField",
          "zetasql_test__.MessageWithMapField.StringInt32MapEntry",
          "zetasql_test__.CivilTimeTypesSinkPB",
          "zetasql_test__.RecursiveMessage",
          "zetasql_test__.RecursivePB",
          "zetasql_test__.Proto3KitchenSink",
          "zetasql_test__.Proto3KitchenSink.Nested",
          "zetasql_test__.Proto3MessageWithNulls",
          "zetasql_test__.EmptyMessage",
          "zetasql_test__.Proto3TestExtraPB",
          "google.protobuf.Timestamp",
          "google.protobuf.Duration",
          "google.type.Date",
          "google.type.TimeOfDay",
          "google.type.LatLng",
          "google.protobuf.DoubleValue",
          "google.protobuf.FloatValue",
          "google.protobuf.Int64Value",
          "google.protobuf.UInt64Value",
          "google.protobuf.Int32Value",
          "google.protobuf.UInt32Value",
          "google.protobuf.BoolValue",
          "google.protobuf.StringValue",
          "google.protobuf.BytesValue"};
}

std::vector<std::string> ZetaSqlRandomTestProtoNames() {
  return {
      "zetasql_test__.KitchenSinkPB",  // Formatted one per line.
      // TODO: b/281436434 - Add OneofProto back once the bug is fixed.
      // "zetasql_test__.OneofProto",
      "google.protobuf.Timestamp",
      "google.protobuf.Duration",
      "google.type.Date",
      "google.type.TimeOfDay",
      "google.type.LatLng",
      "google.protobuf.DoubleValue",
      "google.protobuf.FloatValue",
      "google.protobuf.Int64Value",
      "google.protobuf.UInt64Value",
      "google.protobuf.Int32Value",
      "google.protobuf.UInt32Value",
      "google.protobuf.BoolValue",
      "google.protobuf.StringValue",
      "google.protobuf.BytesValue",
  };
}

std::vector<std::string> ZetaSqlTestEnumNames() {
  // `RoundingMode`, `ArrayFindMode`, `ArrayZipMode` fixes `Enum not found`
  // error in RQG / RSG: b/293474126.
  return {"zetasql_test__.TestEnum", "zetasql_test__.AnotherTestEnum",
          "zetasql.functions.RoundingMode",
          "zetasql.functions.ArrayFindEnums.ArrayFindMode",
          "zetasql.functions.ArrayZipEnums.ArrayZipMode"};
}

}  // namespace testing
}  // namespace zetasql
