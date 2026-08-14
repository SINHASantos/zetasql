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

#include <algorithm>
#include <cstdint>
#include <queue>

#include "googlesql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/check.h"
#include "absl/status/status.h"

namespace googlesql {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

// Implementation of RelationalOpMemoryTracker that counts memory usage
// without enforcing memory limits, for use in tests.
class CountingRelationalOpMemoryTracker : public RelationalOpMemoryTracker {
 public:
  CountingRelationalOpMemoryTracker() = default;

  CountingRelationalOpMemoryTracker(const CountingRelationalOpMemoryTracker&) =
      delete;
  CountingRelationalOpMemoryTracker& operator=(
      const CountingRelationalOpMemoryTracker&) = delete;

  ~CountingRelationalOpMemoryTracker() override = default;

  absl::Status RequestBytes(int64_t bytes) override {
    current_bytes_ += bytes;
    peak_bytes_ = std::max(peak_bytes_, current_bytes_);
    total_requested_bytes_ += bytes;
    return absl::OkStatus();
  }

  void ReturnBytes(int64_t bytes) override {
    current_bytes_ -= bytes;
    total_returned_bytes_ += bytes;
    ABSL_DCHECK_GE(current_bytes_, 0);
  }

  int64_t current_bytes() const { return current_bytes_; }
  int64_t peak_bytes() const { return peak_bytes_; }
  int64_t total_requested_bytes() const { return total_requested_bytes_; }
  int64_t total_returned_bytes() const { return total_returned_bytes_; }

 private:
  int64_t current_bytes_ = 0;
  int64_t peak_bytes_ = 0;
  int64_t total_requested_bytes_ = 0;
  int64_t total_returned_bytes_ = 0;
};

TEST(RelationalOpAccountantMemoryTrackerTest, NullAccountantIsNoOp) {
  RelationalOpAccountantMemoryTracker tracker(/*accountant=*/nullptr);
  EXPECT_THAT(tracker.RequestBytes(100), IsOk());
  tracker.ReturnBytes(100);
  EXPECT_EQ(tracker.requested_bytes(), 0);
}

TEST(RelationalOpAccountantMemoryTrackerTest, RequestAndReturnBytes) {
  MemoryAccountant accountant(/*total_num_bytes=*/500, "test");
  {
    RelationalOpAccountantMemoryTracker tracker(&accountant);
    EXPECT_THAT(tracker.RequestBytes(200), IsOk());
    EXPECT_EQ(tracker.requested_bytes(), 200);
    EXPECT_EQ(accountant.remaining_bytes(), 300);

    tracker.ReturnBytes(100);
    EXPECT_EQ(tracker.requested_bytes(), 100);
    EXPECT_EQ(accountant.remaining_bytes(), 400);

    // Destructor will return remaining 100 bytes automatically.
  }
  EXPECT_EQ(accountant.remaining_bytes(), 500);
}

TEST(RelationalOpAccountantMemoryTrackerTest, ExceedsMemoryLimit) {
  MemoryAccountant accountant(/*total_num_bytes=*/100, "test");
  RelationalOpAccountantMemoryTracker tracker(&accountant);
  EXPECT_THAT(tracker.RequestBytes(150),
              StatusIs(absl::StatusCode::kResourceExhausted));
  EXPECT_EQ(tracker.requested_bytes(), 0);
  EXPECT_EQ(accountant.remaining_bytes(), 100);
}

TEST(CountingRelationalOpMemoryTrackerTest, CountsAllocationsAndDeallocations) {
  CountingRelationalOpMemoryTracker tracker;
  EXPECT_EQ(tracker.current_bytes(), 0);
  EXPECT_EQ(tracker.peak_bytes(), 0);

  EXPECT_THAT(tracker.RequestBytes(100), IsOk());
  EXPECT_EQ(tracker.current_bytes(), 100);
  EXPECT_EQ(tracker.peak_bytes(), 100);
  EXPECT_EQ(tracker.total_requested_bytes(), 100);

  EXPECT_THAT(tracker.RequestBytes(200), IsOk());
  EXPECT_EQ(tracker.current_bytes(), 300);
  EXPECT_EQ(tracker.peak_bytes(), 300);
  EXPECT_EQ(tracker.total_requested_bytes(), 300);

  tracker.ReturnBytes(100);
  EXPECT_EQ(tracker.current_bytes(), 200);
  EXPECT_EQ(tracker.peak_bytes(), 300);
  EXPECT_EQ(tracker.total_returned_bytes(), 100);

  tracker.ReturnBytes(200);
  EXPECT_EQ(tracker.current_bytes(), 0);
  EXPECT_EQ(tracker.peak_bytes(), 300);
  EXPECT_EQ(tracker.total_returned_bytes(), 300);
}

TEST(MemoryTrackedQueueTest, TracksQueuePushAndPop) {
  CountingRelationalOpMemoryTracker tracker;
  MemoryTrackedQueue<std::queue<int>> queue(
      &tracker, [](const int& item) { return static_cast<int64_t>(item); });

  EXPECT_TRUE(queue.empty());
  EXPECT_THAT(queue.Push(100), IsOk());
  EXPECT_THAT(queue.Push(200), IsOk());
  EXPECT_EQ(queue.size(), 2);
  EXPECT_EQ(tracker.current_bytes(), 300);
  EXPECT_EQ(tracker.peak_bytes(), 300);

  EXPECT_EQ(queue.Pop(), 100);
  EXPECT_EQ(tracker.current_bytes(), 200);

  EXPECT_EQ(queue.Pop(), 200);
  EXPECT_EQ(tracker.current_bytes(), 0);
  EXPECT_TRUE(queue.empty());
}

}  // namespace
}  // namespace googlesql
