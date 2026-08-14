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
#include <utility>

#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/collation.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/types/type_parameters.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"

using ::absl_testing::IsOkAndHolds;

namespace googlesql {

TEST(TypeModifiersTest, Creation) {
  {
    // Test creating empty TypeModifiers.
    TypeModifiers type_modifiers;
    EXPECT_TRUE(type_modifiers.type_parameters().IsTopLevelEmpty());
    EXPECT_TRUE(type_modifiers.type_parameters().IsEmpty());
    EXPECT_TRUE(type_modifiers.collation().Empty());

    // Test serialization / deserialization.
    TypeModifiersProto proto;
    GOOGLESQL_ASSERT_OK(type_modifiers.Serialize(&proto));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers deserialized_type_modifiers,
                         TypeModifiers::Deserialize(proto));
    ASSERT_TRUE(type_modifiers.Equals(deserialized_type_modifiers));

    EXPECT_EQ(type_modifiers.DebugString(), "null");
  }
  // TypeModifiers with empty <type_parameters>.
  {
    Collation collation = Collation::MakeScalar("und:ci");

    TypeModifiers type_modifiers =
        TypeModifiers::MakeTypeModifiers(TypeParameters(), collation);
    EXPECT_TRUE(type_modifiers.type_parameters().Equals(TypeParameters()));
    EXPECT_TRUE(type_modifiers.collation().Equals(collation));

    // Test serialization / deserialization.
    TypeModifiersProto proto;
    GOOGLESQL_ASSERT_OK(type_modifiers.Serialize(&proto));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers deserialized_type_modifiers,
                         TypeModifiers::Deserialize(proto));
    ASSERT_TRUE(type_modifiers.Equals(deserialized_type_modifiers));

    EXPECT_EQ(type_modifiers.DebugString(), "collation:und:ci");
  }
  {
    StringTypeParametersProto string_type_param_proto;
    string_type_param_proto.set_max_length(1000);
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        TypeParameters type_param,
        TypeParameters::MakeStringTypeParameters(string_type_param_proto));
    Collation collation = Collation::MakeScalar("und:ci");

    TypeModifiers type_modifiers =
        TypeModifiers::MakeTypeModifiers(type_param, collation);
    EXPECT_TRUE(type_modifiers.type_parameters().Equals(type_param));
    EXPECT_TRUE(type_modifiers.collation().Equals(collation));

    // Test serialization / deserialization.
    TypeModifiersProto proto;
    GOOGLESQL_ASSERT_OK(type_modifiers.Serialize(&proto));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers deserialized_type_modifiers,
                         TypeModifiers::Deserialize(proto));
    ASSERT_TRUE(type_modifiers.Equals(deserialized_type_modifiers));

    EXPECT_EQ(type_modifiers.DebugString(),
              "type_parameters:(max_length=1000), collation:und:ci");
  }
  {
    // TypeModifiers does not require that underlying modifiers must have the
    // same nested structure.
    StringTypeParametersProto string_type_param_proto;
    string_type_param_proto.set_max_length(1000);
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        TypeParameters type_param,
        TypeParameters::MakeStringTypeParameters(string_type_param_proto));

    TypeFactory type_factory;
    const ArrayType* array_of_string_type;
    GOOGLESQL_QCHECK_OK(
        type_factory.MakeArrayType(types::StringType(), &array_of_string_type));
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(array_of_string_type);
    annotation_map->AsStructMap()->mutable_field(0)->SetAnnotation(
        static_cast<int>(AnnotationKind::kCollation),
        SimpleValue::String("und:ci"));
    annotation_map->Normalize();

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                         Collation::MakeCollation(*annotation_map));

    TypeModifiers type_modifiers =
        TypeModifiers::MakeTypeModifiers(type_param, collation);
    EXPECT_TRUE(type_modifiers.type_parameters().Equals(type_param));
    EXPECT_TRUE(type_modifiers.collation().Equals(collation));

    // Test serialization / deserialization.
    TypeModifiersProto proto;
    GOOGLESQL_ASSERT_OK(type_modifiers.Serialize(&proto));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers deserialized_type_modifiers,
                         TypeModifiers::Deserialize(proto));
    ASSERT_TRUE(type_modifiers.Equals(deserialized_type_modifiers));

    EXPECT_EQ(type_modifiers.DebugString(),
              "type_parameters:(max_length=1000), collation:[und:ci]");
  }
}

TEST(TypeModifiersTest, AssignmentAfterMove) {
  {
    // Test that TypeModifiers can be reassigned after being moved.
    TypeModifiers type_modifiers;
    TypeModifiers type_modifiers2 = std::move(type_modifiers);
    type_modifiers = type_modifiers2;
    EXPECT_EQ(type_modifiers, type_modifiers2);
  }
}

static absl::StatusOr<TypeModifiers>
CreateTypeModifiersForStructWithTimestampPrecision(int64_t precision) {
  TimestampTypeParametersProto timestamp_type_parameters_proto;
  timestamp_type_parameters_proto.set_precision(precision);
  GOOGLESQL_ASSIGN_OR_RETURN(TypeParameters ts_type_params_precision,
                   TypeParameters::MakeTimestampTypeParameters(
                       timestamp_type_parameters_proto));
  return TypeModifiers::MakeTypeModifiers(
      TypeParameters::MakeTypeParametersWithChildList(
          {TypeParameters(), std::move(ts_type_params_precision)}),
      Collation());
}

TEST(TypeModifiersTest, EqualsWithDefaultTimestampPrecision) {
  TypeModifiers type_modifiers1;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers type_modifiers2,
                       CreateTypeModifiersForStructWithTimestampPrecision(6));

  EXPECT_FALSE(type_modifiers1.Equals(type_modifiers2));

  EXPECT_TRUE(type_modifiers1.EqualsWithDefaultTimestampPrecision(
      type_modifiers2, /*default_timestamp_precision=*/6));
  EXPECT_FALSE(type_modifiers1.EqualsWithDefaultTimestampPrecision(
      type_modifiers2, /*default_timestamp_precision=*/3));
}

TEST(TypeModifiersTest, GetChild) {
  // Test empty TypeModifiers.
  {
    TypeModifiers empty_type_modifiers;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers child, empty_type_modifiers.GetChild(0));
    EXPECT_TRUE(child.IsEmpty());

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(child, empty_type_modifiers.GetChild(100));
    EXPECT_TRUE(child.IsEmpty());
  }

  // Test non-empty TypeModifiers with children.
  {
    StringTypeParametersProto string_type_param_proto;
    string_type_param_proto.set_max_length(100);
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        TypeParameters string_type_param,
        TypeParameters::MakeStringTypeParameters(string_type_param_proto));

    TypeParameters struct_type_params =
        TypeParameters::MakeTypeParametersWithChildList(
            {string_type_param, TypeParameters()});

    TypeModifiers type_modifiers =
        TypeModifiers::MakeTypeModifiers(struct_type_params, Collation());

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers child0, type_modifiers.GetChild(0));
    EXPECT_FALSE(child0.IsEmpty());
    EXPECT_TRUE(child0.type_parameters().Equals(string_type_param));

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeModifiers child1, type_modifiers.GetChild(1));
    EXPECT_TRUE(child1.IsEmpty());

    // Test out of bounds.
    EXPECT_FALSE(type_modifiers.GetChild(2).ok());
  }
}

TEST(TypeModifiersTest, ReleaseTypeParametersAndCollation) {
  StringTypeParametersProto string_proto;
  string_proto.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_params,
                       TypeParameters::MakeStringTypeParameters(string_proto));
  Collation collation = Collation::MakeScalar("und:ci");

  TypeModifiers modifiers =
      TypeModifiers::MakeTypeModifiers(type_params, collation);

  EXPECT_FALSE(modifiers.type_parameters().IsEmpty());
  EXPECT_FALSE(modifiers.collation().Empty());

  // Release type parameters.
  TypeParameters released_params = modifiers.release_type_parameters();
  EXPECT_TRUE(released_params.Equals(type_params));
  // The internal pointer should be reset to nullptr, returning
  // EmptyTypeParameters.
  EXPECT_EQ(&modifiers.type_parameters(),
            &TypeParameters::EmptyTypeParameters());

  // Release collation.
  Collation released_collation = modifiers.release_collation();
  EXPECT_TRUE(released_collation.Equals(collation));
  // The internal pointer should be reset to nullptr, returning EmptyCollation.
  EXPECT_EQ(&modifiers.collation(), &Collation::EmptyCollation());
}

}  // namespace googlesql
