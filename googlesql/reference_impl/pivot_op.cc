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

// This file contains the code for evaluating PIVOT operators.

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/type.h"
#include "googlesql/public/value.h"
#include "googlesql/reference_impl/evaluation.h"
#include "googlesql/reference_impl/function.h"
#include "googlesql/reference_impl/operator.h"
#include "googlesql/reference_impl/tuple.h"
#include "googlesql/reference_impl/tuple_comparator.h"
#include "googlesql/reference_impl/variable_id.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/map_util.h"

namespace googlesql {

namespace {

struct GroupValue {
  std::unique_ptr<TupleData> key;
  std::vector<std::unique_ptr<AggregateArgAccumulator>> aggregators;
};

class PivotTupleIterator : public TupleIterator {
 public:
  PivotTupleIterator(absl::Span<const KeyArg* const> keys,
                     const ValueExpr* for_expr,
                     absl::Span<const ExprArg* const> pivot_values_exprs,
                     absl::Span<const AggregateArg* const> aggregators,
                     const RelationalOp* input,
                     absl::Span<const TupleData* const> params,
                     int num_extra_slots, EvaluationContext* context)
      : keys_(keys),
        for_expr_(for_expr),
        pivot_values_exprs_(pivot_values_exprs),
        aggregators_(aggregators),
        input_(input),
        params_(DeepCopyTupleDatas(params)),
        num_extra_slots_(num_extra_slots),
        context_(context) {}

  TupleData* Next() override {
    if (!initialized_) {
      status_ = Initialize();
      if (!status_.ok()) {
        return nullptr;
      }
      initialized_ = true;
    }

    if (current_index_ < results_.size()) {
      return results_[current_index_++].get();
    }
    return nullptr;
  }

  absl::Status Status() const override { return status_; }

  bool PreservesOrder() const override { return false; }

  const TupleSchema& Schema() const override {
    if (schema_ == nullptr) {
      std::vector<VariableId> variables;
      for (const KeyArg* key : keys_) {
        variables.push_back(key->variable());
      }
      for (const auto* aggregator : aggregators_) {
        variables.push_back(aggregator->variable());
      }
      schema_ = std::make_unique<TupleSchema>(variables);
    }
    return *schema_;
  }

  std::string DebugString() const override { return "PivotTupleIterator"; }

 private:
  absl::Status Initialize() {
    std::vector<const TupleData*> params_ptrs = StripSharedPtrs(params_);
    GOOGLESQL_ASSIGN_OR_RETURN(std::unique_ptr<TupleIterator> input_iter,
                     input_->Eval(params_ptrs, 0, context_));

    absl::flat_hash_map<TupleDataPtr, std::unique_ptr<GroupValue>> group_map;
    std::vector<std::unique_ptr<TupleData>> group_map_keys_memory;

    // Evaluate pivot values once.
    std::vector<Value> pivot_values;
    pivot_values.reserve(pivot_values_exprs_.size());
    for (const ExprArg* pv_expr : pivot_values_exprs_) {
      TupleSlot slot;
      absl::Status status;
      if (!pv_expr->value_expr()->EvalSimple(params_ptrs, context_, &slot,
                                             &status)) {
        return status;
      }
      pivot_values.push_back(slot.value());
    }

    std::vector<const TupleData*> extended_params_vec = params_ptrs;
    extended_params_vec.push_back(nullptr);
    absl::Span<const TupleData* const> extended_params(extended_params_vec);
    UnorderedArrayCollisionTracker unordered_array_collision_tracker;

    while (true) {
      const TupleData* input_row = input_iter->Next();
      if (input_row == nullptr) {
        break;
      }
      extended_params_vec.back() = input_row;

      // Evaluate group key.
      auto key_tuple = std::make_unique<TupleData>(keys_.size());
      auto collated_key_tuple = std::make_unique<TupleData>(keys_.size());
      for (int i = 0; i < keys_.size(); ++i) {
        TupleSlot slot;
        absl::Status status;
        if (!keys_[i]->value_expr()->EvalSimple(extended_params, context_,
                                                &slot, &status)) {
          return status;
        }

        if (  // Once we know the query is known to be non-deterministic, we
              // short-circuit to avoid any overhead from non-determinism
              // detection.
            context_->IsDeterministicOutput()) {
          GOOGLESQL_ASSIGN_OR_RETURN(
              bool could_indicate_nondeterministic_grouping,
              unordered_array_collision_tracker
                  .CouldIndicateNondetermisticGrouping(i, slot.value()));
          // On the first row we know it is not real non-determinism yet.
          if (could_indicate_nondeterministic_grouping && !group_map.empty()) {
            context_->SetNonDeterministicOutput();
          }
        }

        key_tuple->mutable_slot(i)->SetValue(slot.value());
        if (keys_[i]->collation().collator == nullptr) {
          collated_key_tuple->mutable_slot(i)->SetValue(slot.value());
        } else {
          GOOGLESQL_ASSIGN_OR_RETURN(
              Value collated_value,
              ReplaceStringsWithCollationKeys(
                  slot.value(), keys_[i]->collation().collation_key_type,
                  keys_[i]->collation().collator.get()));
          collated_key_tuple->mutable_slot(i)->SetValue(
              std::move(collated_value));
        }
      }

      GroupValue* matched_group = nullptr;
      std::unique_ptr<GroupValue>* matched_group_value =
          googlesql_base::FindOrNull(group_map, TupleDataPtr(collated_key_tuple.get()));
      if (matched_group_value == nullptr) {
        auto gv = std::make_unique<GroupValue>();
        gv->key = std::move(key_tuple);
        gv->aggregators.reserve(aggregators_.size());
        for (const AggregateArg* agg : aggregators_) {
          GOOGLESQL_ASSIGN_OR_RETURN(auto accumulator,
                           agg->CreateAccumulator(params_ptrs, context_));
          gv->aggregators.push_back(std::move(accumulator));
        }
        matched_group = gv.get();
        GOOGLESQL_RET_CHECK(
            group_map
                .insert({TupleDataPtr(collated_key_tuple.get()), std::move(gv)})
                .second);
        group_map_keys_memory.push_back(std::move(collated_key_tuple));
      } else {
        matched_group = matched_group_value->get();
      }

      // Evaluate FOR expression.
      TupleSlot for_expr_slot;
      absl::Status status;
      if (!for_expr_->EvalSimple(extended_params, context_, &for_expr_slot,
                                 &status)) {
        return status;
      }

      // Find matching pivot value and dispatch to aggregators.
      for (int pv_idx = 0; pv_idx < pivot_values.size(); ++pv_idx) {
        if (!IsDistinctFrom(for_expr_slot.value(), pivot_values[pv_idx])) {
          bool stop_bit = false;
          absl::Status agg_status;
          if (!matched_group->aggregators[pv_idx]->Accumulate(
                  *input_row, &stop_bit, &agg_status)) {
            return agg_status;
          }
        }
      }
    }
    GOOGLESQL_RETURN_IF_ERROR(input_iter->Status());

    if (group_map.empty() && keys_.empty()) {
      // We are doing full aggregation over empty input, so we must compute
      // trivial values for the aggregators.
      auto gv = std::make_unique<GroupValue>();
      gv->key = std::make_unique<TupleData>(0);
      gv->aggregators.reserve(aggregators_.size());
      for (const AggregateArg* agg : aggregators_) {
        GOOGLESQL_ASSIGN_OR_RETURN(auto accumulator,
                         agg->CreateAccumulator(params_ptrs, context_));
        gv->aggregators.push_back(std::move(accumulator));
      }
      auto group_key_ptr = TupleDataPtr(gv->key.get());
      GOOGLESQL_RET_CHECK(
          group_map.insert({std::move(group_key_ptr), std::move(gv)}).second);
      // The key, which is the collated_key_tuple, is simply an empty tuple.
      group_map_keys_memory.push_back(std::make_unique<TupleData>());
    }

    // Generate output results.
    results_.reserve(group_map.size());
    for (auto& [key, gv] : group_map) {
      auto output_row = std::make_unique<TupleData>(
          Schema().variables().size() + num_extra_slots_);
      for (int i = 0; i < keys_.size(); ++i) {
        output_row->mutable_slot(i)->SetValue(gv->key->slot(i).value());
      }
      for (size_t agg_idx = 0; agg_idx < aggregators_.size(); ++agg_idx) {
        GOOGLESQL_ASSIGN_OR_RETURN(Value val, gv->aggregators[agg_idx]->GetFinalResult(
                                        /*inputs_in_defined_order=*/false));
        output_row->mutable_slot(static_cast<int>(keys_.size() + agg_idx))
            ->SetValue(val);
      }
      results_.push_back(std::move(output_row));
    }
    return absl::OkStatus();
  }

  const absl::Span<const KeyArg* const> keys_;
  const ValueExpr* for_expr_;
  const absl::Span<const ExprArg* const> pivot_values_exprs_;
  const absl::Span<const AggregateArg* const> aggregators_;
  const RelationalOp* input_;
  const std::vector<std::shared_ptr<const TupleData>> params_;
  const int num_extra_slots_;
  EvaluationContext* context_;

  bool initialized_ = false;
  absl::Status status_;
  std::vector<std::unique_ptr<TupleData>> results_;
  int current_index_ = 0;
  mutable std::unique_ptr<TupleSchema> schema_;
};

}  // namespace

PivotOp::PivotOp(std::vector<std::unique_ptr<KeyArg>> keys,
                 std::unique_ptr<ValueExpr> for_expr,
                 std::vector<std::unique_ptr<ExprArg>> pivot_values,
                 std::vector<std::unique_ptr<AggregateArg>> aggregators,
                 std::unique_ptr<RelationalOp> input) {
  SetArgs(kKey, std::move(keys));
  SetArg(kForExpr, std::make_unique<ExprArg>(std::move(for_expr)));
  SetArgs(kPivotValue, std::move(pivot_values));
  SetArgs(kPivotAggregator, std::move(aggregators));
  SetArg(kInput, std::make_unique<RelationalArg>(std::move(input)));
  (void)set_is_order_preserving(false);
}

absl::StatusOr<std::unique_ptr<PivotOp>> PivotOp::Create(
    std::vector<std::unique_ptr<KeyArg>> keys,
    std::unique_ptr<ValueExpr> for_expr,
    std::vector<std::unique_ptr<ExprArg>> pivot_values,
    std::vector<std::unique_ptr<AggregateArg>> aggregators,
    std::unique_ptr<RelationalOp> input) {
  return absl::WrapUnique(
      new PivotOp(std::move(keys), std::move(for_expr), std::move(pivot_values),
                  std::move(aggregators), std::move(input)));
}

absl::Status PivotOp::SetSchemasForEvaluation(
    absl::Span<const TupleSchema* const> params_schemas) {
  GOOGLESQL_RETURN_IF_ERROR(
      GetMutableArg(kInput)->mutable_relational_op()->SetSchemasForEvaluation(
          params_schemas));

  std::unique_ptr<TupleSchema> input_schema =
      GetArg(kInput)->relational_op()->CreateOutputSchema();
  std::vector<const TupleSchema*> extended_params =
      ConcatSpans(params_schemas, {input_schema.get()});

  for (auto* key : GetMutableArgs<KeyArg>(kKey)) {
    GOOGLESQL_RETURN_IF_ERROR(
        key->mutable_value_expr()->SetSchemasForEvaluation(extended_params));
  }
  GOOGLESQL_RETURN_IF_ERROR(
      GetMutableArg(kForExpr)->mutable_value_expr()->SetSchemasForEvaluation(
          extended_params));
  for (auto* pivot_value : GetMutableArgs<ExprArg>(kPivotValue)) {
    GOOGLESQL_RETURN_IF_ERROR(pivot_value->mutable_value_expr()->SetSchemasForEvaluation(
        params_schemas));
  }
  for (auto* aggregator : GetMutableArgs<AggregateArg>(kPivotAggregator)) {
    GOOGLESQL_RETURN_IF_ERROR(
        aggregator->SetSchemasForEvaluation(*input_schema, params_schemas));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<TupleIterator>> PivotOp::CreateIterator(
    absl::Span<const TupleData* const> params, int num_extra_slots,
    EvaluationContext* context) const {
  auto iter = std::make_unique<PivotTupleIterator>(
      GetArgs<KeyArg>(kKey), GetArg(kForExpr)->value_expr(),
      GetArgs<ExprArg>(kPivotValue), GetArgs<AggregateArg>(kPivotAggregator),
      GetArg(kInput)->relational_op(), params, num_extra_slots, context);
  return MaybeReorder(std::move(iter), context);
}

std::unique_ptr<TupleSchema> PivotOp::CreateOutputSchema() const {
  std::vector<VariableId> variables;
  for (const KeyArg* key : GetArgs<KeyArg>(kKey)) {
    variables.push_back(key->variable());
  }
  for (const auto* aggregator : GetArgs<AggregateArg>(kPivotAggregator)) {
    variables.push_back(aggregator->variable());
  }
  return std::make_unique<TupleSchema>(variables);
}

std::string PivotOp::IteratorDebugString() const {
  return absl::StrCat(
      "PivotOp(", GetArg(kInput)->relational_op()->IteratorDebugString(), ")");
}

std::string PivotOp::DebugInternal(const std::string& indent,
                                   bool verbose) const {
  return absl::StrCat(
      "PivotOp(",
      ArgDebugString({"keys", "for", "pivot_values", "aggregators", "input"},
                     {kN, k1, kN, kN, k1}, indent, verbose),
      ")");
}

}  // namespace googlesql
