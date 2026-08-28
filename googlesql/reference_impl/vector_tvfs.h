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

#ifndef GOOGLESQL_REFERENCE_IMPL_VECTOR_TVFS_H_
#define GOOGLESQL_REFERENCE_IMPL_VECTOR_TVFS_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/reference_impl/evaluation.h"
#include "googlesql/reference_impl/function.h"
#include "absl/status/statusor.h"

namespace googlesql {

// Table-valued function (TVF) evaluator for `VECTOR_SEARCH`. Supports both
// the batch vector search variant (finding nearest neighbors for a table of
// query vectors) and the single vector search variant (finding nearest
// neighbors for a single query vector).
class VectorSearchTVF : public BuiltinTableValuedFunction {
 public:
  explicit VectorSearchTVF(FunctionKind kind)
      : BuiltinTableValuedFunction(kind) {}

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>> CreateEvaluator(
      std::vector<TableValuedFunction::TvfEvaluatorArg> args,
      std::shared_ptr<FunctionSignature> function_call_signature,
      std::shared_ptr<const TVFSignature> tvf_signature,
      EvaluationContext* context) override;

  std::string debug_name() const override { return "vector_search"; }
};

// Table-valued function (TVF) evaluator for `KMEANS`. Performs k-means
// clustering on the vector column of an input table and returns the resulting
// cluster centroids (with `cluster_id` and `cluster_vector`).
class KMeansTVF : public BuiltinTableValuedFunction {
 public:
  explicit KMeansTVF(FunctionKind kind) : BuiltinTableValuedFunction(kind) {}

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>> CreateEvaluator(
      std::vector<TableValuedFunction::TvfEvaluatorArg> args,
      std::shared_ptr<FunctionSignature> function_call_signature,
      std::shared_ptr<const TVFSignature> tvf_signature,
      EvaluationContext* context) override;

  std::string debug_name() const override { return "kmeans"; }
};

}  // namespace googlesql

#endif  // GOOGLESQL_REFERENCE_IMPL_VECTOR_TVFS_H_
