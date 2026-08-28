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

#include "googlesql/public/annotation/vector_length.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "googlesql/common/errors.h"
#include "googlesql/public/annotation/default_annotation_spec.h"
#include "googlesql/public/builtin_function.pb.h"
#include "googlesql/public/constant.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_parameters.h"
#include "googlesql/public/types/vector_type_util.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace googlesql {

namespace {

// The vector length annotation can be deduced from a constant literal or a
// constant. Constant expressions like `10+5` are not supported.
absl::StatusOr<std::optional<int64_t>> GetConstantOrLiteralInt64(
    const ResolvedExpr* expr) {
  if (expr->Is<ResolvedLiteral>()) {
    const Value& val = expr->GetAs<ResolvedLiteral>()->value();
    GOOGLESQL_RET_CHECK(val.type()->IsInt64());
    if (!val.is_null()) {
      return val.int64_value();
    }
  } else if (expr->Is<ResolvedConstant>()) {
    const Constant* constant = expr->GetAs<ResolvedConstant>()->constant();
    // If the constant does not have a value at analysis time, we cannot deduce
    // the vector length and return std::nullopt to signal to the analyzer.
    if (constant->HasValue()) {
      GOOGLESQL_ASSIGN_OR_RETURN(Value val, constant->GetValue());
      GOOGLESQL_RET_CHECK(val.type()->IsInt64());
      if (!val.is_null()) {
        return val.int64_value();
      }
    }
  }
  return std::nullopt;
}

// The vector length annotation can be deduced from an array constant literal or
// a constant. Constant expressions like `ARRAY_CONCAT([1, 2, 3], [4, 5, 6])`
// are not supported.
absl::StatusOr<std::optional<int64_t>> GetArrayLengthFromConstantOrLiteral(
    const ResolvedExpr* expr) {
  if (expr->Is<ResolvedLiteral>()) {
    const Value& val = expr->GetAs<ResolvedLiteral>()->value();
    GOOGLESQL_RET_CHECK(val.type()->IsArray());
    if (!val.is_null()) {
      GOOGLESQL_ASSIGN_OR_RETURN(Value::ListView elements, val.elements_view());
      return elements.size();
    }
  } else if (expr->Is<ResolvedConstant>()) {
    const Constant* constant = expr->GetAs<ResolvedConstant>()->constant();
    // If the constant does not have a value at analysis time, we cannot deduce
    // the vector length and return std::nullopt to signal to the analyzer.
    if (constant->HasValue()) {
      GOOGLESQL_ASSIGN_OR_RETURN(Value val, constant->GetValue());
      GOOGLESQL_RET_CHECK(val.type()->IsArray());
      if (!val.is_null()) {
        GOOGLESQL_ASSIGN_OR_RETURN(Value::ListView elements, val.elements_view());
        return elements.size();
      }
    }
  }
  return std::nullopt;
}

absl::StatusOr<std::optional<int64_t>> GetVectorLengthFromAnnotationMap(
    const AnnotationMap* annotation_map) {
  if (annotation_map == nullptr) {
    return std::nullopt;
  }
  const SimpleValue* in_length =
      annotation_map->GetAnnotation(VectorLengthAnnotation::GetId());
  if (in_length == nullptr) {
    return std::nullopt;
  }
  GOOGLESQL_RET_CHECK(in_length->IsValid());
  GOOGLESQL_RET_CHECK(in_length->has_int64_value());
  return in_length->int64_value();
}

}  // namespace

absl::Status VectorLengthAnnotation::PropagateFromTypeParameters(
    const Type* target_type, const TypeParameters& target_type_params,
    const AnnotationMap* input_map, AnnotationMap& result_annotation_map,
    bool return_null_on_error, const ParseLocationRange* error_location) {
  std::vector<const Type*> component_types = target_type->ComponentTypes();
  // If the target type is not a composite type, then we are propagating a base
  // type.
  if (component_types.empty()) {
    if (!IsVectorType(target_type)) {
      return absl::OkStatus();
    }
    GOOGLESQL_RET_CHECK_EQ(target_type_params.num_children(), 0);
    GOOGLESQL_RET_CHECK(target_type_params.IsEmpty() ||
              target_type_params.IsVectorTypeParameters());

    std::optional<int64_t> target_length;
    if (target_type_params.IsVectorTypeParameters()) {
      const VectorTypeParametersProto* vector_params =
          target_type_params.vector_type_parameters();
      if (vector_params != nullptr && vector_params->has_length()) {
        target_length = vector_params->length();
      }
    }

    GOOGLESQL_ASSIGN_OR_RETURN(std::optional<int64_t> input_length,
                     GetVectorLengthFromAnnotationMap(input_map));

    // If the target type length already has a value, we use it since it takes
    // precedence. Otherwise, we simply pass on the input length.
    if (target_length.has_value()) {
      result_annotation_map.SetAnnotation<VectorLengthAnnotation>(
          SimpleValue::Int64(*target_length));
      return absl::OkStatus();
    }
    if (input_length.has_value()) {
      result_annotation_map.SetAnnotation<VectorLengthAnnotation>(
          SimpleValue::Int64(*input_length));
    }
    return absl::OkStatus();
  }

  // If the target type is a composite type, then we need to propagate the
  // annotations to all the children.
  GOOGLESQL_RET_CHECK(result_annotation_map.IsStructMap());
  GOOGLESQL_RET_CHECK_EQ(result_annotation_map.AsStructMap()->num_fields(),
               component_types.size());

  if (target_type_params.num_children() > 0) {
    GOOGLESQL_RET_CHECK_EQ(target_type_params.num_children(), component_types.size());
  }

  for (int i = 0; i < component_types.size(); ++i) {
    const AnnotationMap* input_child = nullptr;
    if (input_map != nullptr && input_map->IsStructMap() &&
        i < input_map->AsStructMap()->num_fields()) {
      input_child = input_map->AsStructMap()->field(i);
    }

    GOOGLESQL_RETURN_IF_ERROR(PropagateFromTypeParameters(
        component_types[i],
        target_type_params.num_children() > 0 ? target_type_params.child(i)
                                              : TypeParameters(),
        input_child, *result_annotation_map.AsStructMap()->mutable_field(i),
        return_null_on_error, error_location));
  }
  return absl::OkStatus();
}

absl::Status VectorLengthAnnotation::CheckAndPropagateForCast(
    const ResolvedCast& cast, AnnotationMap* result_annotation_map) {
  GOOGLESQL_RET_CHECK(result_annotation_map != nullptr);

  return PropagateFromTypeParameters(
      cast.type(), cast.type_modifiers().type_parameters(),
      cast.expr()->type_annotation_map(), *result_annotation_map,
      cast.return_null_on_error(), cast.GetParseLocationRangeOrNULL());
}

absl::Status VectorLengthAnnotation::CheckAndPropagateForFunctionCallBase(
    const ResolvedFunctionCallBase& function_call,
    AnnotationMap* result_annotation_map) {
  GOOGLESQL_RETURN_IF_ERROR(DefaultAnnotationSpec::CheckAndPropagateForFunctionCallBase(
      function_call, result_annotation_map));

  if (function_call.function()->IsGoogleSQLBuiltin(FN_ENCODE_VECTOR)) {
    std::optional<int64_t> target_length;
    std::optional<int64_t> array_length;

    // In GoogleSQL ResolvedFunctionCall, arguments are positionally guaranteed
    // to match the function signature, regardless of whether the user used
    // named arguments in the query. Index 0 is always the input array. Index 1
    // (if present) is the target length.
    GOOGLESQL_RET_CHECK_GT(function_call.argument_list_size(), 0);
    GOOGLESQL_ASSIGN_OR_RETURN(array_length, GetArrayLengthFromConstantOrLiteral(
                                       function_call.argument_list(0)));

    if (function_call.argument_list_size() > 1) {
      GOOGLESQL_ASSIGN_OR_RETURN(target_length, GetConstantOrLiteralInt64(
                                          function_call.argument_list(1)));
    }

    // The given length parameter takes precedence over the array length.
    if (target_length.has_value()) {
      result_annotation_map->SetAnnotation<VectorLengthAnnotation>(
          SimpleValue::Int64(*target_length));
      return absl::OkStatus();
    }
    if (array_length.has_value()) {
      result_annotation_map->SetAnnotation<VectorLengthAnnotation>(
          SimpleValue::Int64(*array_length));
      return absl::OkStatus();
    }
  }
  return absl::OkStatus();
}

absl::Status VectorLengthAnnotation::ScalarMergeIfCompatible(
    const AnnotationMap* in, AnnotationMap& out) const {
  // Get the input and output vector length if they exist.
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<int64_t> in_length,
                   GetVectorLengthFromAnnotationMap(in));
  GOOGLESQL_ASSIGN_OR_RETURN(std::optional<int64_t> out_length,
                   GetVectorLengthFromAnnotationMap(&out));
  if (!in_length.has_value() || !out_length.has_value()) {
    out.UnsetAnnotation(VectorLengthAnnotation::GetId());
    return absl::OkStatus();
  }
  // Now if the annotation does not match, raise an error.
  if (in_length.value() != out_length.value()) {
    return MakeSqlError() << VectorLengthAnnotation::Name()
                          << " conflict: " << in_length.value() << " vs. "
                          << out_length.value();
  }
  return absl::OkStatus();
}

}  // namespace googlesql
