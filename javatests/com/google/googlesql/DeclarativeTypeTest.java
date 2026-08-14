/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.googlesql;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.googlesql.GoogleSQLOptions.ProductMode;
import com.google.googlesql.GoogleSQLType.DeclarativeTypeProto;
import com.google.googlesql.GoogleSQLType.TypeProto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeclarativeTypeTest {

  @Test
  public void testDeclarativeTypeIdentity() {
    TypeFactory factory = TypeFactory.uniqueNames();
    DeclarativeType type1 =
        factory.createDeclarativeType(
            DeclarativeTypeDescriptor.builder()
                .setTypeId(new DeclarativeTypeDescriptor.TypeId("NS", "T1", /* versionId= */ "v1"))
                .setDisplayName("display_t1")
                .setBackingType(TypeFactory.createSimpleType(GoogleSQLType.TypeKind.TYPE_INT64))
                .build());

    DeclarativeType type1Same =
        factory.createDeclarativeType(
            DeclarativeTypeDescriptor.builder()
                .setTypeId(new DeclarativeTypeDescriptor.TypeId("NS", "T1", /* versionId= */ "v1"))
                .setDisplayName("display_t1")
                .setBackingType(TypeFactory.createSimpleType(GoogleSQLType.TypeKind.TYPE_INT64))
                .build());

    DeclarativeType typeDifferentVersionId =
        factory.createDeclarativeType(
            DeclarativeTypeDescriptor.builder()
                .setTypeId(new DeclarativeTypeDescriptor.TypeId("NS", "T1", /* versionId= */ "v2"))
                .setDisplayName("display_t1")
                .setBackingType(TypeFactory.createSimpleType(GoogleSQLType.TypeKind.TYPE_INT64))
                .build());

    assertThat(type1.equals(type1Same)).isTrue();
    assertThat(type1.hashCode()).isEqualTo(type1Same.hashCode());

    assertThat(type1.equals(typeDifferentVersionId)).isFalse();
  }

  @Test
  public void testDeclarativeTypeAccessorsAndCoercion() {
    TypeFactory factory = TypeFactory.uniqueNames();
    DeclarativeType type1 =
        factory.createDeclarativeType(
            DeclarativeTypeDescriptor.builder()
                .setTypeId(new DeclarativeTypeDescriptor.TypeId("NS", "T1", /* versionId= */ "v1"))
                .setDisplayName("display_t1")
                .setBackingType(TypeFactory.createSimpleType(GoogleSQLType.TypeKind.TYPE_INT64))
                .build());

    assertThat(type1.isDeclarativeType()).isTrue();
    assertThat(type1.asDeclarativeType()).isSameInstanceAs(type1);
    assertThat(type1.isGoogleSqlBuiltin()).isFalse();
    assertThat(type1.getId())
        .isEqualTo(new DeclarativeTypeDescriptor.TypeId("NS", "T1", /* versionId= */ "v1"));
    assertThat(type1.typeName(ProductMode.PRODUCT_INTERNAL)).isEqualTo("display_t1");
    assertThat(type1.debugString(false)).isEqualTo("display_t1");

    Type intType = TypeFactory.createSimpleType(GoogleSQLType.TypeKind.TYPE_INT64);
    assertThat(intType.isDeclarativeType()).isFalse();
    assertThat(intType.asDeclarativeType()).isNull();
    assertThat(type1.equals(intType)).isFalse();
    assertThat(type1.equivalent(type1)).isTrue();
    assertThat(type1.equivalent(intType)).isFalse();
  }

  @Test
  public void testDeclarativeTypeReturningDelegated() {
    TypeFactory factory = TypeFactory.uniqueNames();

    DeclarativeTypeDescriptor descriptor =
        DeclarativeTypeDescriptor.builder()
            .setTypeId(new DeclarativeTypeDescriptor.TypeId("NS", "T1", /* versionId= */ ""))
            .setDisplayName("t1")
            .setBackingType(TypeFactory.createSimpleType(GoogleSQLType.TypeKind.TYPE_INT64))
            .setReturningStrategy(DeclarativeTypeProto.ReturningStrategy.RETURNING_DELEGATED)
            .build();
    DeclarativeType declarativeType = factory.createDeclarativeType(descriptor);
    TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
    declarativeType.serialize(typeProtoBuilder, new FileDescriptorSetsBuilder());
    assertThat(typeProtoBuilder.getDeclarativeType().getReturningStrategy())
        .isEqualTo(DeclarativeTypeProto.ReturningStrategy.RETURNING_DELEGATED);
  }

  @Test
  public void testClassAndProtoSize() {
    assertWithMessage(
            "The number of fields of DeclarativeTypeProto has changed, please also update the "
                + "serialization code accordingly.")
        .that(DeclarativeTypeProto.getDescriptor().getFields())
        .hasSize(8);
    assertWithMessage(
            "The number of fields of TypeIdProto has changed, please also update the "
                + "serialization code accordingly.")
        .that(DeclarativeTypeProto.TypeIdProto.getDescriptor().getFields())
        .hasSize(3);
    assertWithMessage(
            "The number of fields in DeclarativeTypeDescriptor class has changed, "
                + "please also update the proto and serialization code accordingly.")
        .that(TestUtil.getNonStaticFieldCount(DeclarativeTypeDescriptor.class))
        .isEqualTo(8);
    assertWithMessage(
            "The number of fields in DeclarativeTypeDescriptor.TypeId class has changed, "
                + "please also update the proto and serialization code accordingly.")
        .that(TestUtil.getNonStaticFieldCount(DeclarativeTypeDescriptor.TypeId.class))
        .isEqualTo(3);
  }
}
