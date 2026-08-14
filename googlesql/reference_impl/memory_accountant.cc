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

#include "googlesql/reference_impl/memory_accountant.h"

#include <cstdint>
#include <limits>

#include "googlesql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"

namespace googlesql {

namespace {

bool SafeUInt64ToInt64NumBytes(uint64_t num_bytes, int64_t* res,
                               absl::Status* status) {
  if (num_bytes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    *status = absl::ResourceExhaustedError(absl::Substitute(
        "Out of memory: impossible to request $0 bytes.", num_bytes));
    return false;
  }
  *res = static_cast<int64_t>(num_bytes);
  return true;
}

}  // namespace

bool MemoryAccountant::RequestUInt64Bytes(uint64_t num_bytes,
                                          absl::Status* status) {
  int64_t num_bytes_int64;
  if (!SafeUInt64ToInt64NumBytes(num_bytes, &num_bytes_int64, status)) {
    return false;
  }
  return RequestBytes(num_bytes_int64, status);
}

RelationalOpAccountantMemoryTracker::RelationalOpAccountantMemoryTracker(
    MemoryAccountant* accountant)
    : accountant_(accountant) {}

RelationalOpAccountantMemoryTracker::~RelationalOpAccountantMemoryTracker() {
  if (requested_bytes_ > 0 && accountant_ != nullptr) {
    accountant_->ReturnBytes(requested_bytes_);
  }
}

absl::Status RelationalOpAccountantMemoryTracker::RequestBytes(int64_t bytes) {
  if (accountant_ == nullptr) return absl::OkStatus();
  absl::Status status;
  if (!accountant_->RequestBytes(bytes, &status)) {
    return status;
  }
  requested_bytes_ += bytes;
  return absl::OkStatus();
}

void RelationalOpAccountantMemoryTracker::ReturnBytes(int64_t bytes) {
  if (accountant_ == nullptr) return;
  accountant_->ReturnBytes(bytes);
  requested_bytes_ -= bytes;
  ABSL_DCHECK_GE(requested_bytes_, 0);
}

}  // namespace googlesql
