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

#ifndef GOOGLESQL_REFERENCE_IMPL_MEMORY_ACCOUNTANT_H_
#define GOOGLESQL_REFERENCE_IMPL_MEMORY_ACCOUNTANT_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <queue>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "googlesql/base/check.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"

namespace googlesql {

// Tracks the amount of memory used for tuples in places that accumulate a bunch
// of them.
class MemoryAccountant {
 public:
  // Constructs a MemoryAccountant that can allocate at most 'total_num_bytes'
  // at once.
  explicit MemoryAccountant(int64_t total_num_bytes,
                            absl::string_view name = "")
      : total_num_bytes_(total_num_bytes),
        remaining_bytes_(total_num_bytes),
        name_(name) {}

  MemoryAccountant(const MemoryAccountant&) = delete;
  MemoryAccountant& operator=(const MemoryAccountant&) = delete;
  ~MemoryAccountant() { ABSL_DCHECK_EQ(remaining_bytes_, total_num_bytes_); }

  // If there are 'num_bytes' available, updates the number of remaining bytes
  // accordingly and returns true. Else returns false and populates
  // 'status'. Does not return absl::Status for performance reasons.
  bool RequestBytes(int64_t num_bytes, absl::Status* status) {
    ABSL_DCHECK_GE(num_bytes, 0);
    if (num_bytes > remaining_bytes_) {
      *status = absl::ResourceExhaustedError(absl::Substitute(
          "Out of memory for MemoryAccountant($0): requested $1 bytes but only "
          "$2 are available out of a total of $3.",
          name_, num_bytes, remaining_bytes_, total_num_bytes_));

      return false;
    }
    remaining_bytes_ -= num_bytes;
    return true;
  }

  // Casts `num_bytes` to an int64_t and calls RequestBytes. Returns false and
  // populates `status` if the cast fails (ie: if `num_bytes` is too large) or
  // if RequestBytes returns false.
  bool RequestUInt64Bytes(uint64_t num_bytes, absl::Status* status);

  // Returns 'num_bytes' so they are available to future calls to
  // RequestBytes().
  void ReturnBytes(int64_t num_bytes) {
    remaining_bytes_ += num_bytes;
    ABSL_DCHECK_LE(remaining_bytes_, total_num_bytes_);
  }

  int64_t remaining_bytes() const { return remaining_bytes_; }

 private:
  const int64_t total_num_bytes_;
  int64_t remaining_bytes_;
  std::string name_;
};

// Represents a memory reservation on an accountant bytes already allocated by
// the caller.
// Frees the bytes in the destructor.
class MemoryReservation {
 public:
  // Constructs an empty MemoryReservation
  explicit MemoryReservation(MemoryAccountant* accountant)
      : accountant_(accountant), num_bytes_(0) {}

  // A memory reservation is moveable, but not copyable. Moving it transfers
  // ownership; copying id disallowed altogether to avoid double-free.
  MemoryReservation(const MemoryReservation&) = delete;
  MemoryReservation operator=(const MemoryReservation&) = delete;
  MemoryReservation(MemoryReservation&& reservation)
      : accountant_(reservation.accountant_),
        num_bytes_(reservation.num_bytes_) {
    // Prevent double free when original memory reservation is destroyed.
    // Also, avoid potential crash if the accountant is destroyed before the
    // original reservation.
    reservation.num_bytes_ = 0;
    reservation.accountant_ = nullptr;
  }

  // The destructor frees allocated bytes back to the memory accountant.
  ~MemoryReservation() {
    if (accountant_ != nullptr) {
      accountant_->ReturnBytes(num_bytes_);
    }
  }

  // Allocates <num_bytes> and updates the reservation accordingly.
  ABSL_MUST_USE_RESULT bool Increase(int64_t num_bytes, absl::Status* status) {
    bool success = accountant_->RequestBytes(num_bytes, status);
    if (success) {
      num_bytes_ += num_bytes;
    }
    return success;
  }

  // Allocates <num_bytes> and updates the reservation accordingly.
  ABSL_MUST_USE_RESULT bool IncreaseUInt64(uint64_t num_bytes,
                                           absl::Status* status) {
    bool success = accountant_->RequestUInt64Bytes(num_bytes, status);
    if (success) {
      // SAFETY: RequestUInt64Bytes returns false when `num_bytes` is too large
      // to be represented as an int64_t and when `num_bytes` is greater than
      // a certain int64_t threshold that decreases with each bytes requests.
      // Hence, this addition is safe and will not overflow.
      num_bytes_ += num_bytes;
    }
    return success;
  }

 private:
  MemoryAccountant* accountant_;
  int64_t num_bytes_;
};

// Interface to track memory usage of relational operators.
class RelationalOpMemoryTracker {
 public:
  virtual ~RelationalOpMemoryTracker() = default;

  // Requests `bytes` of memory. Returns an error status if allocation fails.
  virtual absl::Status RequestBytes(int64_t bytes) = 0;

  // Returns `bytes` of memory.
  virtual void ReturnBytes(int64_t bytes) = 0;
};

// RAII helper implementation of RelationalOpMemoryTracker that tracks memory
// using MemoryAccountant and automatically returns any requested bytes upon
// destruction.
class RelationalOpAccountantMemoryTracker : public RelationalOpMemoryTracker {
 public:
  explicit RelationalOpAccountantMemoryTracker(MemoryAccountant* accountant);

  RelationalOpAccountantMemoryTracker(
      const RelationalOpAccountantMemoryTracker&) = delete;
  RelationalOpAccountantMemoryTracker& operator=(
      const RelationalOpAccountantMemoryTracker&) = delete;

  ~RelationalOpAccountantMemoryTracker() override;

  absl::Status RequestBytes(int64_t bytes) override;
  void ReturnBytes(int64_t bytes) override;

  int64_t requested_bytes() const { return requested_bytes_; }

 private:
  MemoryAccountant* const accountant_;
  int64_t requested_bytes_ = 0;
};

// Adapter around queue or priority_queue that automatically tracks memory usage
// using RelationalOpMemoryTracker when elements are pushed or popped.
template <typename Container,
          typename SizeFn = int64_t (*)(const typename Container::value_type&)>
class MemoryTrackedQueue {
 public:
  using ValueType = typename Container::value_type;

  MemoryTrackedQueue(RelationalOpMemoryTracker* memory_tracker, SizeFn size_fn,
                     Container container = Container())
      : memory_tracker_(memory_tracker),
        size_fn_(std::move(size_fn)),
        container_(std::move(container)) {}

  bool empty() const { return container_.empty(); }
  size_t size() const { return container_.size(); }

  const ValueType& top() const {
    ABSL_DCHECK(!container_.empty());
    if constexpr (requires { container_.front(); }) {
      return container_.front();
    } else {
      return container_.top();
    }
  }

  absl::Status Push(ValueType item) {
    if (memory_tracker_ != nullptr) {
      GOOGLESQL_RETURN_IF_ERROR(memory_tracker_->RequestBytes(size_fn_(item)));
    }
    container_.push(std::move(item));
    return absl::OkStatus();
  }

  ValueType Pop() {
    ABSL_DCHECK(!container_.empty());
    ValueType item = GetNextItem();
    container_.pop();
    if (memory_tracker_ != nullptr) {
      memory_tracker_->ReturnBytes(size_fn_(item));
    }
    return item;
  }

 private:
  ValueType GetNextItem() {
    if constexpr (requires { container_.front(); }) {
      return std::move(container_.front());
    } else {
      return std::move(const_cast<ValueType&>(container_.top()));
    }
  }

  RelationalOpMemoryTracker* const memory_tracker_ = nullptr;
  SizeFn size_fn_;
  Container container_;
};

}  // namespace googlesql

#endif  // GOOGLESQL_REFERENCE_IMPL_MEMORY_ACCOUNTANT_H_
