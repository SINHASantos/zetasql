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

package com.google.zetasql;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.FunctionProtos.TableValuedFunctionOptionsProto;
import com.google.zetasql.FunctionProtos.TableValuedFunctionProto;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.ArgumentCardinality;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.TableValuedFunction.FixedOutputSchemaTVF;
import com.google.zetasql.TableValuedFunction.ForwardInputSchemaToOutputSchemaTVF;
import com.google.zetasql.TableValuedFunction.ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class TableValuedFunctionTest {

  @Test
  public void testTvfDebugStringAndToString() {
    ImmutableList<String> functionNamePath = ImmutableList.of("foo", "bar", "tvf_name");
    TVFRelation tvfSchema =
        TVFRelation.createColumnBased(
            ImmutableList.of(
                TVFRelation.Column.create(
                    "column_name", TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));
    TableValuedFunctionOptionsProto tvfOptions =
        TableValuedFunctionOptionsProto.newBuilder().setUsesUpperCaseSqlName(false).build();
    FunctionArgumentType relationWithSchemaArg =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_TYPE_RELATION,
            FunctionArgumentType.FunctionArgumentTypeOptions.builder()
                .setCardinality(ArgumentCardinality.REQUIRED)
                .setRelationInputSchema(tvfSchema)
                .build(),
            1);
    FunctionSignature signatureRelationArg =
        new FunctionSignature(
            relationWithSchemaArg, ImmutableList.of(relationWithSchemaArg), /* contextId= */ 1);
    FunctionSignature signatureRepeatedStringAndOptionalInt64Arg =
        new FunctionSignature(
            relationWithSchemaArg,
            ImmutableList.of(
                new FunctionArgumentType(
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING),
                    ArgumentCardinality.REPEATED),
                new FunctionArgumentType(
                    TypeFactory.createSimpleType(TypeKind.TYPE_INT64),
                    ArgumentCardinality.OPTIONAL)),
            /* contextId= */ 2);
    TableValuedFunction tvf =
        new FixedOutputSchemaTVF(
            functionNamePath,
            ImmutableList.of(signatureRelationArg, signatureRepeatedStringAndOptionalInt64Arg),
            tvfSchema,
            tvfOptions);
    assertThat(tvf.toString())
        .isEqualTo(
            "foo.bar.tvf_name((TABLE<column_name STRING>) -> TABLE<column_name STRING>; (repeated"
                + " STRING, optional INT64) -> TABLE<column_name STRING>)");
    assertThat(tvf.toDebugString(/* verbose= */ true))
        .isEqualTo(
            "foo.bar.tvf_name\n"
                + "  (TABLE<column_name STRING>) -> TABLE<column_name STRING>\n"
                + "  (repeated STRING, optional INT64) -> TABLE<column_name STRING>");
  }

  @Test
  public void testTvfSerializeAndDeserializeWithMultipleSignatures() {
    TypeFactory typeFactory = TypeFactory.nonUniqueNames();
    FileDescriptorSetsBuilder fileDescriptorSetsBuilder = new FileDescriptorSetsBuilder();
    ImmutableList<String> functionPath = ImmutableList.of("test_tvf_name");
    TVFRelation tvfSchema =
        TVFRelation.createColumnBased(
            ImmutableList.of(
                TVFRelation.Column.create(
                    "column1", TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));
    TableValuedFunctionOptionsProto tvfOptions =
        TableValuedFunctionOptionsProto.newBuilder().setUsesUpperCaseSqlName(false).build();

    FunctionArgumentType relationWithSchemaArg =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_TYPE_RELATION,
            FunctionArgumentType.FunctionArgumentTypeOptions.builder()
                .setRelationInputSchema(tvfSchema)
                .build(),
            1);
    FunctionArgumentType anyRelationArg =
        new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_RELATION);
    FunctionArgumentType anyTypeArg =
        new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_ARBITRARY);

    long contextId = 0;
    FunctionSignature signatureRelationArg =
        new FunctionSignature(
            relationWithSchemaArg, ImmutableList.of(relationWithSchemaArg), /* contextId= */ 1);
    FunctionSignature signatureRelationAndAnyArg =
        new FunctionSignature(
            relationWithSchemaArg,
            ImmutableList.of(relationWithSchemaArg, anyTypeArg),
            ++contextId);
    FunctionSignature signatureAnyRelationArg =
        new FunctionSignature(anyRelationArg, ImmutableList.of(anyRelationArg), ++contextId);
    FunctionSignature signatureAnyRelationAndAnyArg =
        new FunctionSignature(
            anyRelationArg, ImmutableList.of(anyRelationArg, anyTypeArg), ++contextId);

    // Test FixedOutputSchemaTVF
    {
      ImmutableList<FunctionSignature> signatures =
          ImmutableList.of(signatureRelationArg, signatureRelationAndAnyArg);
      FixedOutputSchemaTVF tvf =
          new FixedOutputSchemaTVF(functionPath, signatures, tvfSchema, tvfOptions);

      TableValuedFunctionProto tvfProto = tvf.serialize(fileDescriptorSetsBuilder);
      TableValuedFunction deserializedTvf =
          TableValuedFunction.deserialize(
              tvfProto, fileDescriptorSetsBuilder.getDescriptorPools(), typeFactory);

      assertThat(deserializedTvf).isInstanceOf(FixedOutputSchemaTVF.class);
      assertThat(((FixedOutputSchemaTVF) deserializedTvf).getOutputSchema()).isEqualTo(tvfSchema);
      assertThat(deserializedTvf.getFunctionSignatures()).hasSize(2);
      assertThat(tvf.toString()).isEqualTo(deserializedTvf.toString());
    }

    // Test ForwardInputSchemaToOutputSchemaTVF
    {
      ImmutableList<FunctionSignature> signatures =
          ImmutableList.of(
              signatureRelationArg, signatureRelationAndAnyArg, signatureAnyRelationArg);
      ForwardInputSchemaToOutputSchemaTVF tvf =
          new ForwardInputSchemaToOutputSchemaTVF(
              functionPath,
              signatures,
              /* customContext= */ null,
              /* volatility= */ null,
              tvfOptions);

      TableValuedFunctionProto tvfProto = tvf.serialize(fileDescriptorSetsBuilder);
      TableValuedFunction deserializedTvf =
          TableValuedFunction.deserialize(
              tvfProto, fileDescriptorSetsBuilder.getDescriptorPools(), typeFactory);

      assertThat(deserializedTvf).isInstanceOf(ForwardInputSchemaToOutputSchemaTVF.class);
      assertThat(deserializedTvf.getFunctionSignatures()).hasSize(3);
      assertThat(tvf.toString()).isEqualTo(deserializedTvf.toString());
    }

    // Test ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF
    {
      ImmutableList<FunctionSignature> signatures =
          ImmutableList.of(signatureAnyRelationArg, signatureAnyRelationAndAnyArg);
      TVFRelation.Column extraColumn =
          TVFRelation.Column.create(
              "extra_col", TypeFactory.createSimpleType(TypeKind.TYPE_STRING));
      ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF tvf =
          new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
              functionPath,
              signatures,
              ImmutableList.of(extraColumn),
              /* customContext= */ null,
              /* volatility= */ null,
              tvfOptions);

      TableValuedFunctionProto tvfProto = tvf.serialize(fileDescriptorSetsBuilder);
      TableValuedFunction deserializedTvf =
          TableValuedFunction.deserialize(
              tvfProto, fileDescriptorSetsBuilder.getDescriptorPools(), typeFactory);

      assertThat(deserializedTvf)
          .isInstanceOf(ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF.class);
      assertThat(deserializedTvf.getFunctionSignatures()).hasSize(2);
      assertThat(tvf.toString()).isEqualTo(deserializedTvf.toString());
    }
  }
}
