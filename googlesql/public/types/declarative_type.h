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

#ifndef GOOGLESQL_PUBLIC_TYPES_DECLARATIVE_TYPE_H_
#define GOOGLESQL_PUBLIC_TYPES_DECLARATIVE_TYPE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "googlesql/public/language_options.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/value_equality_check_options.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace googlesql {

// Uniquely identifies a declaratively-defined type.
// Any two instances with the same TypeId are identical and Type::Equals() must
// return true, and their behaviors must be identical.
//
// TypeFactory currently caches DeclarativeTypes based on TypeId, and any
// repeated calls to MakeDeclarativeType() with the same TypeId return the same
// Type* and GOOGLESQL_RET_CHECK that the descriptors are identical.
struct TypeId {
  static constexpr absl::string_view kGoogleSqlNamespace = "GoogleSQL";

  // For GoogleSQL built-in types, the namespace is kGoogleSqlNamespace.
  std::string name_space;
  // An ID that uniquely identifies the type within the namespace.
  // *DO NOT* use this alone for identity checks. The whole TypeId needs to be
  // considered.
  std::string local_id;
  // An optional version ID.
  // This can be used by systems managing user-defined types (UDTs) to
  // distinguish different iterations of a type (e.g., if a type is dropped
  // and re-created under the same name/handle).
  // For GoogleSQL built-in types, this should always be empty.
  std::string version_id;

  bool operator==(const TypeId& other) const {
    return name_space == other.name_space && local_id == other.local_id &&
           version_id == other.version_id;
  }

  bool IsGoogleSQLBuiltin() const { return name_space == kGoogleSqlNamespace; }

  template <typename H>
  friend H AbslHashValue(H h, const TypeId& type_id) {
    return H::combine(std::move(h), type_id.name_space, type_id.local_id,
                      type_id.version_id);
  }
};

// This contains all the information to fully specify a DeclarativeType. It
// describes the type's properties, traits, and full behavior. It also
// specifies its identity through the TypeId, which uniquely identifies the
// type.
//
// Unlike primitive types where the analyzer code sometimes hard-codes some
// behaviors, everything about a DeclarativeType's behavior is localized to its
// `DeclarativeTypeDescriptor`'s setup.
//
// This is quite similar to how ProtoType and EnumType are created generically,
// relying on the descriptor. However, descriptors for declaratively-defined
// types ("declarative types" for short) define the type's identity as well, not
// just its behavior.
//
// Notes:
// * `TypeId` is always required. It uniquely identifies the type.
//   If multiple instances are created with the same TypeId, they must be
//   identical: they are the same type, and Type::Equals() returns true.
//
// * `backing_type` is always required. It is used primarily for value
//    representation. For example:
//   - An engine implementing a type ComplexNumber could, under the hood,
//     represent values as a struct of two doubles.
//   - User-created types: a `CREATE TYPE` statement normally would specify a
//     backing type for the engine to know how to represent values.
//     The user-visible "base type" naturally lends itself to be the
//     `backing_type`.
//
//   Serialization and deserialization are delegated to the backing type. The
//   framework also supports delegating some traits (such as equality) to the
//   backing type.
//
// See (broken link) for more details.
class DeclarativeTypeDescriptor final {
 public:
  DeclarativeTypeDescriptor() = default;
  DeclarativeTypeDescriptor(const DeclarativeTypeDescriptor& other) {
    data_ = std::make_unique<Data>(*other.data_);
  }
  DeclarativeTypeDescriptor(DeclarativeTypeDescriptor&&) = default;

  enum class AllowCoercionMode {
    kNoCoercion = 0,    // No coercion is allowed.
    kExplicitOnly,      // Only explicit coercion is allowed.
    kAllowAllCoercion,  // Implicit, assignment and explicit are all allowed.
  };

  struct ReturningDisallowed {};
  struct ReturningDelegated {};
  using ReturningStrategy =
      std::variant<ReturningDisallowed, ReturningDelegated>;

  struct EqualityDisallowed {};
  struct EqualityDelegated {};
  using EqualityStrategy = std::variant<EqualityDelegated, EqualityDisallowed>;

  const TypeId& type_id() const { return data_->type_id; }
  DeclarativeTypeDescriptor& set_type_id(const TypeId& type_id) {
    data_->type_id = type_id;
    return *this;
  }

  const std::string& display_name() const { return data_->display_name; }
  DeclarativeTypeDescriptor& set_display_name(absl::string_view display_name) {
    data_->display_name = display_name;
    return *this;
  }

  const Type* backing_type() const { return data_->backing_type; }
  DeclarativeTypeDescriptor& set_backing_type(const Type* backing_type) {
    data_->backing_type = backing_type;
    return *this;
  }

  AllowCoercionMode coercion_from_backing_type() const {
    return data_->coercion_from_backing_type;
  }
  DeclarativeTypeDescriptor& set_coercion_from_backing_type(
      AllowCoercionMode coercion_from_backing_type) {
    data_->coercion_from_backing_type = coercion_from_backing_type;
    return *this;
  }

  AllowCoercionMode coercion_to_backing_type() const {
    return data_->coercion_to_backing_type;
  }
  DeclarativeTypeDescriptor& set_coercion_to_backing_type(
      AllowCoercionMode coercion_to_backing_type) {
    data_->coercion_to_backing_type = coercion_to_backing_type;
    return *this;
  }

  ReturningStrategy returning_strategy() const {
    return data_->returning_strategy;
  }
  DeclarativeTypeDescriptor& set_returning_strategy(
      const ReturningStrategy& returning_strategy) {
    data_->returning_strategy = returning_strategy;
    return *this;
  }

  const EqualityStrategy& equality_strategy() const {
    return data_->equality_strategy;
  }
  DeclarativeTypeDescriptor& set_equality_strategy(
      const EqualityStrategy& equality_strategy) {
    data_->equality_strategy = equality_strategy;
    return *this;
  }

  const LanguageOptions::LanguageFeatureSet&
  additional_required_language_features() const {
    return data_->additional_required_language_features;
  }
  DeclarativeTypeDescriptor& set_additional_required_language_features(
      const LanguageOptions::LanguageFeatureSet&
          additional_required_language_features) {
    data_->additional_required_language_features =
        additional_required_language_features;
    return *this;
  }

  size_t GetEstimatedOwnedMemoryBytesSize() const;

  // A descriptor is identical to other if all fields are identical.
  bool IsIdenticalTo(const DeclarativeTypeDescriptor& other) const;

 private:
  struct Data {
    // Internal ID which uniquely identifies this type.
    // Type identity (Equals()) is determined through this ID.
    TypeId type_id;

    // Not used for type identity, but is still user-visible, e.g. in the result
    // of TYPEOF(), displaying function signatures, or in error messages.
    std::string display_name;

    // The backing type for this declarative type.
    const Type* backing_type = nullptr;

    // Allowed coercion modes to and from the `backing_type`.
    AllowCoercionMode coercion_from_backing_type =
        AllowCoercionMode::kNoCoercion;

    // Allowed coercion modes to the `backing_type`.
    AllowCoercionMode coercion_to_backing_type = AllowCoercionMode::kNoCoercion;

    // The returning strategy for this declarative type.
    ReturningStrategy returning_strategy = ReturningDisallowed{};

    // The equality strategy for this declarative type.
    EqualityStrategy equality_strategy = EqualityDisallowed{};

    // *Additional* required features for this type. Does not include other
    // features which are required for the backing type.
    // IsSupportedType() checks both, plus FEATURE_DECLARATIVE_TYPE_FRAMEWORK.
    LanguageOptions::LanguageFeatureSet additional_required_language_features;
  };
  // Allocate on the heap.
  std::unique_ptr<Data> data_ = std::make_unique<Data>();
};

// A declaratively-specified type ("declarative type", for short).
// Such types are created from a `DeclarativeTypeDescriptor`, which fully
// specifies the type's properties, traits, behavior and even identity.
//
// Note that DeclarativeTypes are first-class citizens in the Type system.
// The fact that they are implemented through the declarative type framework is
// an implementation detail which should stay hidden from the various
// components.
//
// This class encapsulates the `DeclarativeTypeDescriptor` and presents the type
// to the GoogleSQL analyzer (Resolver, Coercer, etc.) as an opaque type.
// There is no implied semantic relationship or coercibility (in SQL) to/from
// the backing type.
// Value representation is delegated to the backing type.
// Various other traits are defined in terms of the backing type, e.g. through
// disabling or delegating.
//
// See (broken link) for more on declarative types.
class DeclarativeType final : public Type {
 public:
#ifndef SWIG
  DeclarativeType(const DeclarativeType&) = delete;
  DeclarativeType& operator=(const DeclarativeType&) = delete;
#endif  // SWIG

  std::string ShortTypeName(ProductMode mode) const override;
  std::string TypeName(ProductMode mode) const override;

  const TypeId& id() const { return data_.type_id(); }

  // Returns true if this is a GoogleSQL built-in type.
  bool IsGoogleSQLBuiltin() const { return id().IsGoogleSQLBuiltin(); }

  // Returns true if this is a GoogleSQL built-in type with the given local ID.
  bool IsGoogleSQLBuiltin(absl::string_view local_id) const {
    return IsGoogleSQLBuiltin() && id().local_id == local_id;
  }

  absl::StatusOr<std::string> TypeNameWithModifiers(
      const TypeModifiers& type_modifiers, ProductMode mode) const override;

  std::vector<const Type*> ComponentTypes() const final;

  const DeclarativeType* AsDeclarativeType() const override { return this; }

  // Indicates whether this type can be coerced to the given `to_type`.
  // `is_explicit` indicates whether this is for an explicit or implicit
  // coercion.
  bool CanCoerceTo(const Type* to_type, bool is_explicit) const;

  // Indicates whether `from_type` can coerce to this type.
  // `is_explicit` indicates whether this is for an explicit or implicit
  // coercion.
  bool CanCoerceFrom(const Type* from_type, bool is_explicit) const;

  std::string CapitalizedName() const final;

  bool IsSupportedType(const LanguageOptions& language_options) const final;

  int64_t GetEstimatedOwnedMemoryBytesSize() const final;

  uint64_t GetValueContentExternallyAllocatedByteSize(
      const ValueContent& value) const final;

  bool SupportsEquality() const final;

  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const final;

  void ClearValueContent(const ValueContent& value) const final;

  void CopyValueContent(const ValueContent& from, ValueContent* to) const final;

  // Returns the candidate supertypes for this declarative type.
  TypeListView GetCandidateSuperTypes() const { return candidate_super_types_; }

 protected:
  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const final;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const final;

  bool SupportsPartitioningImpl(const LanguageOptions& language_options,
                                const Type** no_partitioning_type) const final;

  bool SupportsReturningImpl(const LanguageOptions& language_options,
                             const Type** no_returning_type) const final;

 private:
  DeclarativeType(const TypeFactoryBase& factory,
                  DeclarativeTypeDescriptor data);

  const Type* backing_type() const { return data_.backing_type(); }

  bool EqualsForSameKind(const Type* that, bool equivalent) const final;

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override;

  absl::HashState HashTypeParameter(absl::HashState state) const final;

  bool ValueContentEquals(const ValueContent& x, const ValueContent& y,
                          const ValueEqualityCheckOptions& options) const final;

  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const final;

  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const final;
  absl::HashState HashValueContentIgnoringFloat(
      const ValueContent& value, absl::HashState state) const final;

  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const final;

  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const final;

  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const final;

  bool IsIdenticalTo(const DeclarativeType* other) const;

  // Retrieves the ValueContent corresponding to the backing type.
  // Values are 16 bytes, split as 8 bytes of content, plus 8 bytes for
  // metadata, which could be a Type* or splits as a 4-byte TypeKind enum, with
  // the 4 remaining bytes used for additional content for some kinds.
  // So there are 4 cases:
  // a) 8-byte content, plus a TypeKind
  // b) 8-byte content, plus a Type*
  // c) 8-byte pointer to variable width content, plus enum or pointer
  // d) 12-byte content, plus a TypeKind (for DATETIME and TIME)
  //
  // For declarative types, we always need a Type* pointer, which points to the
  // DeclarativeType, and whose descriptor fully describes the type, including
  // what its backing type is.
  //
  // For a backing type stored as (a), (b), or (c), the value content can be
  // stored inline the same way, with a Type* pointer pointing to the
  // DeclarativeType.
  //
  // For a backing type stored as (d), this doesn't fit, so the
  // backing type content is converted to be stored as a pointer to an
  // out-of-line 12-byte value.
  static const ValueContent& GetBackingContent(
      const ValueContent& value_content, const DeclarativeType* decl_type);

  friend class TypeFactory;
  friend class Value;

  DeclarativeTypeDescriptor data_;

  std::vector<const Type*> candidate_super_types_;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_TYPES_DECLARATIVE_TYPE_H_
