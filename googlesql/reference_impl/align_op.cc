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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/reference_impl/evaluation.h"
#include "googlesql/reference_impl/operator.h"
#include "googlesql/reference_impl/tuple.h"
#include "googlesql/reference_impl/tuple_comparator.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace googlesql {

absl::StatusOr<std::unique_ptr<WithinBoundExprArg>> WithinBoundExprArg::Create(
    ResolvedWithinBoundExpr::BoundKind bound_kind,
    std::unique_ptr<ValueExpr> expr) {
  return absl::WrapUnique(new WithinBoundExprArg(bound_kind, std::move(expr)));
}

WithinBoundExprArg::WithinBoundExprArg(
    ResolvedWithinBoundExpr::BoundKind bound_kind,
    std::unique_ptr<ValueExpr> expr)
    : AlgebraArg(VariableId(), std::move(expr)), bound_kind_(bound_kind) {}

std::string WithinBoundExprArg::DebugInternal(const std::string& indent,
                                              bool verbose) const {
  std::string str = ResolvedWithinBoundExpr::BoundKindToString(bound_kind_);
  if (has_value_expr()) {
    return absl::StrCat(str, "(", value_expr()->DebugString(verbose), ")");
  }
  return str;
}

absl::Status WithinBoundExprArg::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  if (has_value_expr()) {
    return mutable_value_expr()->SetSchemasForEvaluation(params_schemas);
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<WithinBoundsArg>> WithinBoundsArg::Create(
    std::unique_ptr<WithinBoundExprArg> lower_bound,
    std::unique_ptr<WithinBoundExprArg> upper_bound) {
  return absl::WrapUnique(
      new WithinBoundsArg(std::move(lower_bound), std::move(upper_bound)));
}

WithinBoundsArg::WithinBoundsArg(
    std::unique_ptr<WithinBoundExprArg> lower_bound,
    std::unique_ptr<WithinBoundExprArg> upper_bound)
    : AlgebraArg(VariableId(), nullptr),
      lower_bound_(std::move(lower_bound)),
      upper_bound_(std::move(upper_bound)) {}

absl::Status WithinBoundsArg::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  GOOGLESQL_RETURN_IF_ERROR(lower_bound_->SetSchemasForEvaluation(params_schemas));
  GOOGLESQL_RETURN_IF_ERROR(upper_bound_->SetSchemasForEvaluation(params_schemas));
  return absl::OkStatus();
}

std::string WithinBoundsArg::DebugInternal(const std::string& indent,
                                           bool verbose) const {
  return absl::StrCat(
      "WITHIN ", "(lower_bound=", lower_bound_->DebugString(verbose),
      ", upper_bound=", upper_bound_->DebugString(verbose), ")");
}

EstimatorArg::EstimatorArg(std::unique_ptr<AggregateArg> aggregate_arg,
                           std::unique_ptr<WithinBoundsArg> within_bounds)
    : ExprArg(aggregate_arg->variable(), aggregate_arg->type()),
      aggregate_arg_(std::move(aggregate_arg)),
      within_bounds_(std::move(within_bounds)) {}

absl::Status EstimatorArg::SetSchemasForEvaluation(
    const TupleSchema& input_schema,
    absl::Span<const TupleSchema* const> params_schemas) {
  GOOGLESQL_RETURN_IF_ERROR(aggregate_arg_->SetSchemasForEvaluation(
      input_schema, params_schemas, /*grouping_keys_schema=*/nullptr));
  GOOGLESQL_RETURN_IF_ERROR(within_bounds_->SetSchemasForEvaluation(params_schemas));
  return absl::OkStatus();
}

std::string EstimatorArg::DebugInternal(const std::string& indent,
                                        bool verbose) const {
  return absl::StrCat(aggregate_arg_->DebugString(verbose), " ",
                      within_bounds_->DebugString(verbose));
}

absl::StatusOr<std::unique_ptr<AlignOp>> AlignOp::Create(
    std::unique_ptr<RelationalOp> input, const VariableId& timestamp_var,
    std::unique_ptr<ValueExpr> period, std::unique_ptr<ValueExpr> origin,
    std::unique_ptr<WithinBoundsArg> output_within,
    std::vector<std::unique_ptr<KeyArg>> partition_keys,
    const VariableId& aligned_timestamp_var,
    std::vector<std::unique_ptr<EstimatorArg>> estimators) {
  return absl::WrapUnique(new AlignOp(
      std::move(input), timestamp_var, std::move(period), std::move(origin),
      std::move(output_within), std::move(partition_keys),
      aligned_timestamp_var, std::move(estimators)));
}

AlignOp::AlignOp(std::unique_ptr<RelationalOp> input,
                 const VariableId& timestamp_var,
                 std::unique_ptr<ValueExpr> period,
                 std::unique_ptr<ValueExpr> origin,
                 std::unique_ptr<WithinBoundsArg> output_within,
                 std::vector<std::unique_ptr<KeyArg>> partition_keys,
                 const VariableId& aligned_timestamp_var,
                 std::vector<std::unique_ptr<EstimatorArg>> estimators)
    : timestamp_var_(timestamp_var),
      aligned_timestamp_var_(aligned_timestamp_var) {
  SetArg(kInput, std::make_unique<RelationalArg>(std::move(input)));
  SetArg(kPeriod, std::make_unique<ExprArg>(std::move(period)));
  if (origin != nullptr) {
    SetArg(kOrigin, std::make_unique<ExprArg>(std::move(origin)));
  }
  SetArg(kOutputWithin, std::move(output_within));
  SetArgs<KeyArg>(kPartitionKey, std::move(partition_keys));
  SetArgs<EstimatorArg>(kEstimator, std::move(estimators));
}

const RelationalOp* AlignOp::input() const {
  return GetArg(kInput)->relational_op();
}
RelationalOp* AlignOp::mutable_input() {
  return GetMutableArg(kInput)->mutable_relational_op();
}

const ValueExpr* AlignOp::period() const {
  return GetArg(kPeriod)->value_expr();
}
ValueExpr* AlignOp::mutable_period() {
  return GetMutableArg(kPeriod)->mutable_value_expr();
}

const ValueExpr* AlignOp::origin() const {
  const AlgebraArg* arg = GetArg(kOrigin);
  return arg ? arg->value_expr() : nullptr;
}
ValueExpr* AlignOp::mutable_origin() {
  AlgebraArg* arg = GetMutableArg(kOrigin);
  return arg ? arg->mutable_value_expr() : nullptr;
}

const WithinBoundsArg* AlignOp::output_within() const {
  return static_cast<const WithinBoundsArg*>(GetArg(kOutputWithin));
}
WithinBoundsArg* AlignOp::mutable_output_within() {
  return static_cast<WithinBoundsArg*>(GetMutableArg(kOutputWithin));
}

absl::Span<const KeyArg* const> AlignOp::partition_keys() const {
  return GetArgs<KeyArg>(kPartitionKey);
}
absl::Span<KeyArg* const> AlignOp::mutable_partition_keys() {
  return GetMutableArgs<KeyArg>(kPartitionKey);
}

absl::Span<const EstimatorArg* const> AlignOp::estimators() const {
  return GetArgs<EstimatorArg>(kEstimator);
}
absl::Span<EstimatorArg* const> AlignOp::mutable_estimators() {
  return GetMutableArgs<EstimatorArg>(kEstimator);
}

absl::Status AlignOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  GOOGLESQL_RETURN_IF_ERROR(mutable_input()->SetSchemasForEvaluation(params_schemas));
  GOOGLESQL_RETURN_IF_ERROR(mutable_period()->SetSchemasForEvaluation(params_schemas));
  if (mutable_origin() != nullptr) {
    GOOGLESQL_RETURN_IF_ERROR(mutable_origin()->SetSchemasForEvaluation(params_schemas));
  }
  GOOGLESQL_RETURN_IF_ERROR(
      mutable_output_within()->SetSchemasForEvaluation(params_schemas));

  const std::unique_ptr<const TupleSchema> input_schema =
      input()->CreateOutputSchema();
  for (KeyArg* key : mutable_partition_keys()) {
    // Partition key expressions can reference variables from the outer query
    // parameters (params_schemas) and the input scan (input_schema).
    GOOGLESQL_RETURN_IF_ERROR(key->mutable_value_expr()->SetSchemasForEvaluation(
        ConcatSpans(params_schemas, {input_schema.get()})));
  }

  for (EstimatorArg* estimator : mutable_estimators()) {
    GOOGLESQL_RETURN_IF_ERROR(
        estimator->SetSchemasForEvaluation(*input_schema, params_schemas));
  }

  return absl::OkStatus();
}

std::unique_ptr<TupleSchema> AlignOp::CreateOutputSchema() const {
  std::vector<VariableId> variables;
  variables.reserve(partition_keys().size() + estimators().size() + 1);
  for (const KeyArg* key : partition_keys()) {
    variables.push_back(key->variable());
  }
  for (const auto* estimator : estimators()) {
    variables.push_back(estimator->aggregate_arg()->variable());
  }
  variables.push_back(aligned_timestamp_var_);
  return std::make_unique<TupleSchema>(variables);
}

std::string AlignOp::IteratorDebugString() const {
  return absl::StrCat("AlignIterator(", input()->IteratorDebugString(), ")");
}

std::string AlignOp::DebugInternal(const std::string& indent,
                                   bool verbose) const {
  return absl::StrCat(
      "AlignOp(",
      ArgDebugString({"input", "period", "origin", "output_within",
                      "partition_keys", "estimators"},
                     {k1, k1, kOpt, kOpt, kN, kN}, indent, verbose),
      ")");
}

absl::StatusOr<std::unique_ptr<TupleIterator>> AlignOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  return absl::UnimplementedError("ALIGN evaluation is not implemented");
}

}  // namespace googlesql
