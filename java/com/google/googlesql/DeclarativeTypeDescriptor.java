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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.googlesql.GoogleSQLType.DeclarativeTypeProto;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/** Descriptor for configuring a DeclarativeType. */
@Immutable
public final class DeclarativeTypeDescriptor implements Serializable {

  /** The identifier of a declarative type. */
  @Immutable
  public static final class TypeId implements Serializable {
    public static final String GOOGLE_SQL_NAMESPACE = "GoogleSQL";

    private final String nameSpace;
    private final String localId;
    private final String versionId;

    public TypeId(String nameSpace, String localId, String versionId) {
      Preconditions.checkNotNull(nameSpace);
      Preconditions.checkArgument(!nameSpace.isEmpty(), "nameSpace must not be empty");
      Preconditions.checkNotNull(localId);
      Preconditions.checkArgument(!localId.isEmpty(), "localId must not be empty");
      Preconditions.checkNotNull(versionId);
      if (nameSpace.equals(GOOGLE_SQL_NAMESPACE)) {
        Preconditions.checkArgument(versionId.isEmpty(), "versionId must be empty for built-ins");
      }
      this.nameSpace = nameSpace;
      this.localId = localId;
      this.versionId = versionId;
    }

    public String getNameSpace() {
      return nameSpace;
    }

    public String getLocalId() {
      return localId;
    }

    public String getVersionId() {
      return versionId;
    }

    public boolean isGoogleSqlBuiltin() {
      return Objects.equals(nameSpace, GOOGLE_SQL_NAMESPACE);
    }

    @Override
    @SuppressWarnings("PatternMatchingInstanceof")
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TypeId)) {
        return false;
      }
      TypeId that = (TypeId) o;
      return Objects.equals(versionId, that.versionId)
          && Objects.equals(nameSpace, that.nameSpace)
          && Objects.equals(localId, that.localId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nameSpace, localId, versionId);
    }
  }

  private final TypeId typeId;
  private final String displayName;
  private final Type backingType;
  private final DeclarativeTypeProto.AllowCoercionMode coercionFromBackingType;
  private final DeclarativeTypeProto.AllowCoercionMode coercionToBackingType;
  private final DeclarativeTypeProto.ReturningStrategy returningStrategy;
  private final DeclarativeTypeProto.EqualityStrategy equalityStrategy;
  private final DeclarativeTypeProto.TypeParamsStrategy typeParamsStrategy;
  private final ImmutableSet<Integer> additionalRequiredLanguageFeatures;

  private DeclarativeTypeDescriptor(Builder builder) {
    this.typeId = builder.typeId;
    this.displayName = builder.displayName;
    this.backingType = builder.backingType;
    this.coercionFromBackingType = builder.coercionFromBackingType;
    this.coercionToBackingType = builder.coercionToBackingType;
    this.returningStrategy = builder.returningStrategy;
    this.equalityStrategy = builder.equalityStrategy;
    this.typeParamsStrategy = builder.typeParamsStrategy;
    this.additionalRequiredLanguageFeatures = builder.additionalRequiredLanguageFeatures;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public TypeId getTypeId() {
    return typeId;
  }

  public String getDisplayName() {
    return displayName;
  }

  public Type getBackingType() {
    return backingType;
  }

  public DeclarativeTypeProto.AllowCoercionMode getCoercionFromBackingType() {
    return coercionFromBackingType;
  }

  public DeclarativeTypeProto.AllowCoercionMode getCoercionToBackingType() {
    return coercionToBackingType;
  }

  public DeclarativeTypeProto.ReturningStrategy getReturningStrategy() {
    return returningStrategy;
  }

  public DeclarativeTypeProto.EqualityStrategy getEqualityStrategy() {
    return equalityStrategy;
  }

  public DeclarativeTypeProto.TypeParamsStrategy getTypeParamsStrategy() {
    return typeParamsStrategy;
  }

  public ImmutableSet<Integer> getAdditionalRequiredLanguageFeatures() {
    return additionalRequiredLanguageFeatures;
  }

  public boolean isIdenticalTo(DeclarativeTypeDescriptor other) {
    if (other == null) {
      return false;
    }
    return Objects.equals(this.typeId, other.typeId)
        && Objects.equals(this.displayName, other.displayName)
        && this.backingType.equalsInternal(other.backingType, /* equivalent= */ false)
        && this.coercionFromBackingType == other.coercionFromBackingType
        && this.coercionToBackingType == other.coercionToBackingType
        && this.returningStrategy == other.returningStrategy
        && this.equalityStrategy == other.equalityStrategy
        && this.typeParamsStrategy == other.typeParamsStrategy
        && Objects.equals(
            this.additionalRequiredLanguageFeatures, other.additionalRequiredLanguageFeatures);
  }

  /** Builder for {@link DeclarativeTypeDescriptor}. */
  public static final class Builder {
    private TypeId typeId;
    private String displayName;
    private Type backingType;
    private DeclarativeTypeProto.AllowCoercionMode coercionFromBackingType =
        DeclarativeTypeProto.AllowCoercionMode.ALLOW_COERCION_MODE_NO_COERCION;
    private DeclarativeTypeProto.AllowCoercionMode coercionToBackingType =
        DeclarativeTypeProto.AllowCoercionMode.ALLOW_COERCION_MODE_NO_COERCION;
    private DeclarativeTypeProto.ReturningStrategy returningStrategy =
        DeclarativeTypeProto.ReturningStrategy.RETURNING_DISALLOWED;
    private DeclarativeTypeProto.EqualityStrategy equalityStrategy =
        DeclarativeTypeProto.EqualityStrategy.EQUALITY_DISALLOWED;
    private DeclarativeTypeProto.TypeParamsStrategy typeParamsStrategy =
        DeclarativeTypeProto.TypeParamsStrategy.TYPE_PARAMS_DISALLOWED;
    private ImmutableSet<Integer> additionalRequiredLanguageFeatures = ImmutableSet.of();

    private Builder() {}

    private Builder(DeclarativeTypeDescriptor descriptor) {
      this.typeId = descriptor.typeId;
      this.displayName = descriptor.displayName;
      this.backingType = descriptor.backingType;
      this.coercionFromBackingType = descriptor.coercionFromBackingType;
      this.coercionToBackingType = descriptor.coercionToBackingType;
      this.returningStrategy = descriptor.returningStrategy;
      this.equalityStrategy = descriptor.equalityStrategy;
      this.typeParamsStrategy = descriptor.typeParamsStrategy;
      this.additionalRequiredLanguageFeatures = descriptor.additionalRequiredLanguageFeatures;
    }

    @CanIgnoreReturnValue
    public Builder setTypeId(TypeId typeId) {
      this.typeId = typeId;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setDisplayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setBackingType(Type backingType) {
      this.backingType = backingType;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setCoercionFromBackingType(
        DeclarativeTypeProto.AllowCoercionMode coercionFromBackingType) {
      this.coercionFromBackingType = coercionFromBackingType;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setCoercionToBackingType(
        DeclarativeTypeProto.AllowCoercionMode coercionToBackingType) {
      this.coercionToBackingType = coercionToBackingType;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setReturningStrategy(DeclarativeTypeProto.ReturningStrategy returningStrategy) {
      this.returningStrategy = returningStrategy;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setEqualityStrategy(DeclarativeTypeProto.EqualityStrategy equalityStrategy) {
      this.equalityStrategy = equalityStrategy;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setTypeParamsStrategy(
        DeclarativeTypeProto.TypeParamsStrategy typeParamsStrategy) {
      this.typeParamsStrategy = typeParamsStrategy;
      return this;
    }

    @CanIgnoreReturnValue
    public Builder setAdditionalRequiredLanguageFeatures(
        Set<Integer> additionalRequiredLanguageFeatures) {
      this.additionalRequiredLanguageFeatures =
          ImmutableSet.copyOf(additionalRequiredLanguageFeatures);
      return this;
    }

    public DeclarativeTypeDescriptor build() {
      return new DeclarativeTypeDescriptor(this);
    }
  }

  public void serialize(
      DeclarativeTypeProto.Builder builder, FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    builder
        .getTypeIdBuilder()
        .setNameSpace(typeId.getNameSpace())
        .setLocalId(typeId.getLocalId())
        .setVersionId(typeId.getVersionId());
    builder
        .setDisplayName(displayName)
        .setCoercionFromBackingType(coercionFromBackingType)
        .setCoercionToBackingType(coercionToBackingType)
        .setReturningStrategy(returningStrategy)
        .setEqualityStrategy(equalityStrategy)
        .setTypeParamsStrategy(typeParamsStrategy)
        .addAllAdditionalRequiredLanguageFeatures(additionalRequiredLanguageFeatures);

    backingType.serialize(builder.getBackingTypeBuilder(), fileDescriptorSetsBuilder);
  }
}
