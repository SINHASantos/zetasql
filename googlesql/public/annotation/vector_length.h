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

#ifndef GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_LENGTH_H_
#define GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_LENGTH_H_

#include "googlesql/public/annotation/default_annotation_spec.h"
#include "googlesql/public/parse_location.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/type.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"

namespace googlesql {

// VectorLength propagates the vector length parameter across operations
// and casts. The VECTOR type requires a strictly positive integer length.
// This class defines the propagation behavior of this length annotation for
// ResolvedAst nodes.
class VectorLengthAnnotation : public DefaultAnnotationSpec {
 public:
  VectorLengthAnnotation() = default;
  ~VectorLengthAnnotation() override = default;

  static int GetId() { return static_cast<int>(AnnotationKind::kVectorLength); }

  int Id() const override { return GetId(); }

  // Determines whether the vector length should be propagated to the function's
  // result. Currently handles specific vector functions such as ENCODE_VECTOR
  // where the output vector length is derived from the function arguments.
  absl::Status CheckAndPropagateForFunctionCallBase(
      const ResolvedFunctionCallBase& function_call,
      AnnotationMap* result_annotation_map) override;

  // Assigns an annotation to the output of a cast if the target type is a
  // VECTOR and specifies a length parameter, e.g. CAST(.. AS VECTOR(3)).
  absl::Status CheckAndPropagateForCast(
      const ResolvedCast& cast, AnnotationMap* result_annotation_map) override;

  // This is called to assign the annotation vector length of the output type
  // from TypeParameters. This function operates recursively within composite
  // types.
  static absl::Status PropagateFromTypeParameters(
      const Type* target_type, const TypeParameters& target_type_params,
      const AnnotationMap* input_map, AnnotationMap& result_annotation_map,
      bool return_null_on_error, const ParseLocationRange* error_location);

  // If one of the annotation is null, we simply drop the annotation. If both
  // annotations are non-null, then we verify the annotations match, otherwise
  // we report an error.
  absl::Status ScalarMergeIfCompatible(const AnnotationMap* in,
                                       AnnotationMap& out) const override;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_ANNOTATION_VECTOR_LENGTH_H_
