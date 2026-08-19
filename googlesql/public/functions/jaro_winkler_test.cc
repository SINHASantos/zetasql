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

#include "googlesql/public/functions/jaro_winkler.h"

#include <limits>
#include <optional>
#include <string>

#include "googlesql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace functions {
namespace {

using ::absl_testing::StatusIs;

struct JaroWinklerSimilarityParam {
  std::string text1;
  std::string text2;
  double expected_output = 0.0;
  double expected_bytes_output = 0.0;

  JaroWinklerSimilarityParam(absl::string_view text1, absl::string_view text2,
                             double expected_output)
      : text1(text1),
        text2(text2),
        expected_output(expected_output),
        expected_bytes_output(expected_output) {}
  JaroWinklerSimilarityParam(absl::string_view text1, absl::string_view text2,
                             double expected_output,
                             double expected_bytes_output)
      : text1(text1),
        text2(text2),
        expected_output(expected_output),
        expected_bytes_output(expected_bytes_output) {}
};

class JaroWinklerSimilarityParamTest
    : public ::testing::TestWithParam<JaroWinklerSimilarityParam> {};

TEST_P(JaroWinklerSimilarityParamTest, JaroWinklerSimilarity) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      double result, JaroWinklerSimilarity(GetParam().text1, GetParam().text2));
  EXPECT_NEAR(result, GetParam().expected_output, 1e-6)
      << "For JaroWinklerSimilarity(" << GetParam().text1 << ", "
      << GetParam().text2 << ")";

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      double result_bytes,
      JaroWinklerSimilarityBytes(GetParam().text1, GetParam().text2));
  EXPECT_NEAR(result_bytes, GetParam().expected_bytes_output, 1e-6)
      << "For JaroWinklerSimilarityBytes(" << GetParam().text1 << ", "
      << GetParam().text2 << ")";
}

INSTANTIATE_TEST_SUITE_P(
    JaroWinklerSimilarity, JaroWinklerSimilarityParamTest,
    testing::Values(JaroWinklerSimilarityParam("MARHTA", "MARTHA", 0.961111),
                    JaroWinklerSimilarityParam("DWAYNE", "DUANE", 0.84),
                    JaroWinklerSimilarityParam("DIXON", "DICKSONX", 0.813333),
                    JaroWinklerSimilarityParam("HELLO", "HELLO", 1.0),
                    JaroWinklerSimilarityParam("A", "A", 1.0),
                    JaroWinklerSimilarityParam("", "", 1.0),
                    JaroWinklerSimilarityParam("", "A", 0.0),
                    JaroWinklerSimilarityParam("ABC", "XYZ", 0.0),
                    JaroWinklerSimilarityParam("A😀B", "A😁B", 0.8, 0.933333),
                    JaroWinklerSimilarityParam("你好", "再见", 0.0, 0.0),
                    JaroWinklerSimilarityParam("中国", "中文", 0.666667,
                                               0.666667),
                    JaroWinklerSimilarityParam("A中B", "A国B", 0.8, 0.6)));

TEST(JaroWinklerSimilarity, CustomParameters) {
  // Jaro similarity of "MARHTA" and "MARTHA" is 0.944444.
  // Common prefix is "MAR" (length 3).

  // Test custom prefix_scaling_factor = 0.2
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result,
        JaroWinklerSimilarity("MARHTA", "MARTHA",
                              /*prefix_scaling_factor=*/0.2,
                              /*prefix_boost_threshold=*/std::nullopt));
    EXPECT_NEAR(result, 0.977778, 1e-5);

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result_bytes,
        JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                   /*prefix_scaling_factor=*/0.2,
                                   /*prefix_boost_threshold=*/std::nullopt));
    EXPECT_NEAR(result_bytes, 0.977778, 1e-5);
  }

  // Test custom prefix_boost_threshold = 0.95 (no boost awarded because
  // 0.944444 < 0.95)
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result,
        JaroWinklerSimilarity("MARHTA", "MARTHA",
                              /*prefix_scaling_factor=*/std::nullopt,
                              /*prefix_boost_threshold=*/0.95));
    EXPECT_NEAR(result, 0.944444, 1e-5);

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result_bytes,
        JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                   /*prefix_scaling_factor=*/std::nullopt,
                                   /*prefix_boost_threshold=*/0.95));
    EXPECT_NEAR(result_bytes, 0.944444, 1e-5);
  }

  // Test custom prefix_boost_threshold = 0.90 (boost awarded because
  // 0.944444 >= 0.90)
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result,
        JaroWinklerSimilarity("MARHTA", "MARTHA",
                              /*prefix_scaling_factor=*/std::nullopt,
                              /*prefix_boost_threshold=*/0.90));
    EXPECT_NEAR(result, 0.961111, 1e-5);

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result_bytes,
        JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                   /*prefix_scaling_factor=*/std::nullopt,
                                   /*prefix_boost_threshold=*/0.90));
    EXPECT_NEAR(result_bytes, 0.961111, 1e-5);
  }

  // Test custom scaling factor 0.2 and boost threshold 0.90 together
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result, JaroWinklerSimilarity("MARHTA", "MARTHA",
                                             /*prefix_scaling_factor=*/0.2,
                                             /*prefix_boost_threshold=*/0.90));
    EXPECT_NEAR(result, 0.977778, 1e-5);

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        double result_bytes,
        JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                   /*prefix_scaling_factor=*/0.2,
                                   /*prefix_boost_threshold=*/0.90));
    EXPECT_NEAR(result_bytes, 0.977778, 1e-5);
  }
}

TEST(JaroWinklerSimilarity, InvalidParameters) {
  // prefix_scaling_factor range is [0, 0.25]
  EXPECT_THAT(JaroWinklerSimilarity("MARHTA", "MARTHA",
                                    /*prefix_scaling_factor=*/-0.01,
                                    /*prefix_boost_threshold=*/std::nullopt),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JaroWinklerSimilarity("MARHTA", "MARTHA",
                                    /*prefix_scaling_factor=*/0.26,
                                    /*prefix_boost_threshold=*/std::nullopt),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      JaroWinklerSimilarity(
          "MARHTA", "MARTHA",
          /*prefix_scaling_factor=*/std::numeric_limits<double>::quiet_NaN(),
          /*prefix_boost_threshold=*/std::nullopt),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                 /*prefix_scaling_factor=*/-0.01,
                                 /*prefix_boost_threshold=*/std::nullopt),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                 /*prefix_scaling_factor=*/0.26,
                                 /*prefix_boost_threshold=*/std::nullopt),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      JaroWinklerSimilarityBytes(
          "MARHTA", "MARTHA",
          /*prefix_scaling_factor=*/std::numeric_limits<double>::quiet_NaN(),
          /*prefix_boost_threshold=*/std::nullopt),
      StatusIs(absl::StatusCode::kOutOfRange));

  // prefix_boost_threshold range is [0, 1]
  EXPECT_THAT(JaroWinklerSimilarity("MARHTA", "MARTHA",
                                    /*prefix_scaling_factor=*/std::nullopt,
                                    /*prefix_boost_threshold=*/-0.01),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JaroWinklerSimilarity("MARHTA", "MARTHA",
                                    /*prefix_scaling_factor=*/std::nullopt,
                                    /*prefix_boost_threshold=*/1.01),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      JaroWinklerSimilarity(
          "MARHTA", "MARTHA",
          /*prefix_scaling_factor=*/std::nullopt,
          /*prefix_boost_threshold=*/std::numeric_limits<double>::quiet_NaN()),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                         /*prefix_scaling_factor=*/std::nullopt,
                                         /*prefix_boost_threshold=*/-0.01),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JaroWinklerSimilarityBytes("MARHTA", "MARTHA",
                                         /*prefix_scaling_factor=*/std::nullopt,
                                         /*prefix_boost_threshold=*/1.01),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      JaroWinklerSimilarityBytes(
          "MARHTA", "MARTHA",
          /*prefix_scaling_factor=*/std::nullopt,
          /*prefix_boost_threshold=*/std::numeric_limits<double>::quiet_NaN()),
      StatusIs(absl::StatusCode::kOutOfRange));
}

}  // namespace
}  // namespace functions
}  // namespace googlesql
