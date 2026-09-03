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

#include "googlesql/common/round_decimal.h"

#include <cmath>
#include <cstdint>
#include <ios>
#include <iostream>
#include <limits>

#include "gtest/gtest.h"
#include "googlesql/base/source_location.h"

namespace googlesql {
namespace util_math {

namespace {

TEST(TruncAndRoundTest, RoundDecimalBasic) {
  EXPECT_EQ(RoundDecimal(123.456, -3), 0.0);
  EXPECT_EQ(RoundDecimal(123.456, -2), 100.0);
  EXPECT_EQ(RoundDecimal(123.456, -1), 120.0);
  EXPECT_EQ(RoundDecimal(123.456, 0), 123.0);
  EXPECT_EQ(RoundDecimal(123.456, 1), 123.5);
  EXPECT_EQ(RoundDecimal(123.456, 2), 123.46);
  EXPECT_EQ(RoundDecimal(123.456, 3), 123.456);
  EXPECT_EQ(RoundDecimal(123.456, 4), 123.456);
  EXPECT_EQ(RoundDecimal(123.456, std::numeric_limits<int64_t>::max()),
            123.456);
  EXPECT_EQ(RoundDecimal(1.5e308, -308),
            std::numeric_limits<double>::infinity());
  EXPECT_EQ(RoundDecimal(1.4999e308, -308), 1.0e308);
  EXPECT_EQ(RoundDecimal(1.7e308, -309), 0.0);
  EXPECT_EQ(RoundDecimal(1.7e308, std::numeric_limits<int64_t>::min()), 0.0);
  // 1.5e39 in double precision is actually slightly less than 1.5 * 10^39.
  EXPECT_EQ(RoundDecimal(1.5e39, -39), 1.0e39);
  EXPECT_EQ(RoundDecimal(-std::numeric_limits<double>::infinity(), 5),
            -std::numeric_limits<double>::infinity());
  EXPECT_EQ(RoundDecimal(-std::numeric_limits<double>::infinity(), -1000),
            -std::numeric_limits<double>::infinity());
  EXPECT_TRUE(
      std::isnan(RoundDecimal(std::numeric_limits<double>::quiet_NaN(), 5)));
}

TEST(TruncAndRoundTest, TruncDecimalBasic) {
  EXPECT_EQ(TruncDecimal(123.456, -3), 0.0);
  EXPECT_EQ(TruncDecimal(123.456, -2), 100.0);
  EXPECT_EQ(TruncDecimal(123.456, -1), 120.0);
  EXPECT_EQ(TruncDecimal(123.456, 0), 123.0);
  EXPECT_EQ(TruncDecimal(123.456, 1), 123.4);
  EXPECT_EQ(TruncDecimal(123.456, 2), 123.45);
  EXPECT_EQ(TruncDecimal(123.456, 3), 123.456);
  EXPECT_EQ(TruncDecimal(123.456, 4), 123.456);
  EXPECT_EQ(TruncDecimal(123.456, std::numeric_limits<int64_t>::max()),
            123.456);
  EXPECT_EQ(TruncDecimal(1.5e308, -308), 1.0e308);
  EXPECT_EQ(TruncDecimal(1.4999e308, -308), 1.0e308);
  EXPECT_EQ(TruncDecimal(1.7e308, -309), 0.0);
  EXPECT_EQ(TruncDecimal(1.7e308, std::numeric_limits<int64_t>::min()), 0.0);
  EXPECT_EQ(TruncDecimal(-std::numeric_limits<double>::infinity(), 5),
            -std::numeric_limits<double>::infinity());
  EXPECT_EQ(TruncDecimal(-std::numeric_limits<double>::infinity(), -1000),
            -std::numeric_limits<double>::infinity());
  EXPECT_TRUE(
      std::isnan(TruncDecimal(-std::numeric_limits<double>::quiet_NaN(), 5)));
  EXPECT_EQ(TruncDecimal(-0x1.9e8a461c56cb4p+11, 12), -0x1.9e8a461c56cb2p11);
  EXPECT_EQ(TruncDecimal(-0x1.96e0e5dcp+5, 14), -0x1.96e0e5dbfffffp5);
  EXPECT_EQ(TruncDecimal(0x1.75f17b5d5ba7ap+20, 9), 0x1.75f17b5d5ba76p20);
}

TEST(TruncAndRoundTest, ExactCases) {
  // Helper function to test that the results are scaled accordingly when the
  // inputs are scaled by powers of 10.
  auto test_scaling = [](double number, int digits, double expected_round,
                         double expected_trunc,
                         googlesql_base::SourceLocation loc =
                             googlesql_base::SourceLocation::current()) {
    testing::ScopedTrace tr(loc.file_name(), loc.line(), "test_scaling");
    double power_of_ten = 1.0;
    constexpr int kMax = 10;
    for (int i = 0; i < kMax; ++i, power_of_ten *= 10.0) {
      double input = number * power_of_ten;
      EXPECT_EQ(RoundDecimal(input, digits - i), expected_round * power_of_ten)
          << "RoundDecimal(" << input << ", " << digits - i << ")";
      EXPECT_EQ(TruncDecimal(input, digits - i), expected_trunc * power_of_ten)
          << "TruncDecimal(" << input << ", " << digits - i << ")";
      EXPECT_EQ(RoundDecimal(-input, digits - i),
                -expected_round * power_of_ten)
          << "RoundDecimal(" << -input << ", " << digits - i << ")";
      EXPECT_EQ(TruncDecimal(-input, digits - i),
                -expected_trunc * power_of_ten)
          << "TruncDecimal(" << -input << ", " << digits - i << ")";
    }
  };

  // Make sure that whole numbers don't change values through trunc / round.
  for (int digits = 0; digits < 50; ++digits) {
    for (double number = -10000; number <= 10000; ++number) {
      test_scaling(number, digits, number, number);
    }
  }

  // Check some fractional powers of 2.
  int64_t power_of_ten = 1;
  for (int digits = 0; digits < 10; ++digits, power_of_ten *= 10) {
    for (double number : {0.5, 0.25, 0.125, 0.0625}) {
      test_scaling(number, digits,
                   std::round(number * power_of_ten) / power_of_ten,
                   std::trunc(number * power_of_ten) / power_of_ten);
    }
  }

  // Test for b/328562866.
  for (int i = 0; i < 100000; ++i) {
    double id = static_cast<double>(i);
    EXPECT_EQ(RoundDecimal(id / 100000.0, 5), id / 100000.0);
  }

  // Tie.
  for (int i = 0; i < 100; ++i) {
    double input = static_cast<double>(i);
    test_scaling(input + 0.5, 0, input + 1.0, input);
  }

  struct SpecialCase {
    double input;
    int digits;
    double round_result;
    double trunc_result;
  } test_list[] = {{0.25, 1, 0.3, 0.2},
                   {0.75, 1, 0.8, 0.7},
                   {393525.0, -1, 393530.0, 393520.0}};

  for (auto [input, digits, round_result, trunc_result] : test_list) {
    test_scaling(input, digits, round_result, trunc_result);
  }
}

}  // namespace

}  // namespace util_math
}  // namespace googlesql
