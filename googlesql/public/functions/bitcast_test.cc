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

#include "googlesql/public/functions/bitcast.h"

#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "googlesql/public/functions/endianness.pb.h"
#include "gtest/gtest.h"
#include "absl/base/casts.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace functions {

const int32_t int32_min = std::numeric_limits<int32_t>::min();
const int32_t int32_max = std::numeric_limits<int32_t>::max();
const int64_t int64_min = std::numeric_limits<int64_t>::min();
const int64_t int64_max = std::numeric_limits<int64_t>::max();
const uint32_t uint32_max = std::numeric_limits<uint32_t>::max();
const uint64_t uint64_max = std::numeric_limits<uint64_t>::max();

const float float_max = std::numeric_limits<float>::max();
const float float_min = std::numeric_limits<float>::lowest();
const float float_min_positive = std::numeric_limits<float>::min();
const float float_min_negative = -std::numeric_limits<float>::min();
const float float_pos_inf = std::numeric_limits<float>::infinity();
const float float_neg_inf = -std::numeric_limits<float>::infinity();
const float float_nan = std::numeric_limits<float>::quiet_NaN();
const float float_neg_nan = absl::bit_cast<float>(0xffc00000u);

const double double_max = std::numeric_limits<double>::max();
const double double_min = std::numeric_limits<double>::lowest();
const double double_min_positive = std::numeric_limits<double>::min();
const double double_min_negative = -std::numeric_limits<double>::min();
const double double_pos_inf = std::numeric_limits<double>::infinity();
const double double_neg_inf = -std::numeric_limits<double>::infinity();
const double double_nan = std::numeric_limits<double>::quiet_NaN();
const double double_neg_nan = absl::bit_cast<double>(0xfff8000000000000ull);

template <typename TIN, typename TOUT>
void TestBitCast(const TIN& in, const TOUT& expected) {
  TOUT out = 0;
  absl::Status status;
  BitCast<TIN, TOUT>(in, &out, &status);
  EXPECT_EQ(expected, out);
}

TEST(BitCastTest, PrimitiveTypes) {
  // INT32 -> INT32
  TestBitCast<int32_t, int32_t>(static_cast<int32_t>(0),
                                static_cast<int32_t>(0));
  TestBitCast<int32_t, int32_t>(static_cast<int32_t>(int32_max),
                                static_cast<int32_t>(int32_max));
  TestBitCast<int32_t, int32_t>(static_cast<int32_t>(int32_min),
                                static_cast<int32_t>(int32_min));
  TestBitCast<int32_t, int32_t>(static_cast<int32_t>(3),
                                static_cast<int32_t>(3));
  TestBitCast<int32_t, int32_t>(static_cast<int32_t>(-3),
                                static_cast<int32_t>(-3));

  // UINT32 -> INT32
  TestBitCast<uint32_t, int32_t>(static_cast<uint32_t>(0),
                                 static_cast<int32_t>(0));
  TestBitCast<uint32_t, int32_t>(static_cast<uint32_t>(uint32_max),
                                 static_cast<int32_t>(-1));
  TestBitCast<uint32_t, int32_t>(static_cast<uint32_t>(3),
                                 static_cast<int32_t>(3));
  TestBitCast<uint32_t, int32_t>(static_cast<uint32_t>(uint32_max - 3),
                                 static_cast<int32_t>(-4));
  TestBitCast<uint32_t, int32_t>(static_cast<uint32_t>(uint32_max >> 1),
                                 static_cast<int32_t>(int32_max));

  // INT64 -> INT64
  TestBitCast<int64_t, int64_t>(static_cast<int64_t>(0),
                                static_cast<int64_t>(0));
  TestBitCast<int64_t, int64_t>(static_cast<int64_t>(int64_max),
                                static_cast<int64_t>(int64_max));
  TestBitCast<int64_t, int64_t>(static_cast<int64_t>(int64_min),
                                static_cast<int64_t>(int64_min));
  TestBitCast<int64_t, int64_t>(static_cast<int64_t>(3),
                                static_cast<int64_t>(3));
  TestBitCast<int64_t, int64_t>(static_cast<int64_t>(-3),
                                static_cast<int64_t>(-3));

  // UINT64 -> INT64
  TestBitCast<uint64_t, int64_t>(static_cast<uint64_t>(0),
                                 static_cast<int64_t>(0));
  TestBitCast<uint64_t, int64_t>(static_cast<uint64_t>(uint64_max),
                                 static_cast<int64_t>(-1));
  TestBitCast<uint64_t, int64_t>(static_cast<uint64_t>(3),
                                 static_cast<int64_t>(3));
  TestBitCast<uint64_t, int64_t>(static_cast<uint64_t>(uint64_max - 3),
                                 static_cast<int64_t>(-4));
  TestBitCast<uint64_t, int64_t>(static_cast<uint64_t>(uint64_max >> 1),
                                 static_cast<int64_t>(int64_max));

  // UINT32 -> UINT32
  TestBitCast<uint32_t, uint32_t>(static_cast<uint32_t>(0),
                                  static_cast<uint32_t>(0));
  TestBitCast<uint32_t, uint32_t>(static_cast<uint32_t>(uint32_max),
                                  static_cast<uint32_t>(uint32_max));
  TestBitCast<uint32_t, uint32_t>(static_cast<uint32_t>(3),
                                  static_cast<uint32_t>(3));

  // INT32 -> UINT32
  TestBitCast<int32_t, uint32_t>(static_cast<int32_t>(0),
                                 static_cast<uint32_t>(0));
  TestBitCast<int32_t, uint32_t>(static_cast<int32_t>(int32_max),
                                 static_cast<uint32_t>(int32_max));
  TestBitCast<int32_t, uint32_t>(static_cast<int32_t>(3),
                                 static_cast<uint32_t>(3));
  TestBitCast<int32_t, uint32_t>(static_cast<int32_t>(-3),
                                 static_cast<uint32_t>(-3));
  TestBitCast<int32_t, uint32_t>(static_cast<int32_t>(int32_min),
                                 static_cast<uint32_t>(int32_min));
  TestBitCast<int32_t, uint32_t>(static_cast<int32_t>(int32_min + 3),
                                 static_cast<uint32_t>(2147483651));

  // UINT64 -> UINT64
  TestBitCast<uint64_t, uint64_t>(static_cast<uint64_t>(0),
                                  static_cast<uint64_t>(0));
  TestBitCast<uint64_t, uint64_t>(static_cast<uint64_t>(uint64_max),
                                  static_cast<uint64_t>(uint64_max));
  TestBitCast<uint64_t, uint64_t>(static_cast<uint64_t>(3),
                                  static_cast<uint64_t>(3));

  // INT64 -> UINT64
  TestBitCast<int64_t, uint64_t>(static_cast<int64_t>(0),
                                 static_cast<uint64_t>(0));
  TestBitCast<int64_t, uint64_t>(static_cast<int64_t>(int64_max),
                                 static_cast<uint64_t>(int64_max));
  TestBitCast<int64_t, uint64_t>(static_cast<int64_t>(3),
                                 static_cast<uint64_t>(3));
  TestBitCast<int64_t, uint64_t>(static_cast<int64_t>(-3),
                                 static_cast<uint64_t>(-3));
  TestBitCast<int64_t, uint64_t>(static_cast<int64_t>(int64_min),
                                 static_cast<uint64_t>(int64_min));
  TestBitCast<int64_t, uint64_t>(
      static_cast<int64_t>(int64_min + 3),
      static_cast<uint64_t>(uint64_t{9223372036854775811u}));
}

struct BitCastToBytesTestCase {
  std::string test_name;
  std::variant<float, double, int32_t, uint32_t, int64_t, uint64_t> input;
  absl::string_view expected_bytes;
  Endianness endianness = Endianness::LITTLE;
};

class BitCastToBytesTest
    : public ::testing::TestWithParam<BitCastToBytesTestCase> {};

TEST_P(BitCastToBytesTest, ConvertsToBytes) {
  const auto& [test_name, input, expected_bytes, endianness] = GetParam();
  std::string s_out;
  absl::Status status;
  std::visit(
      [&](auto&& value) {
        EXPECT_TRUE(BitCastToBytes(value, &s_out, &status, endianness))
            << status;
      },
      input);
  EXPECT_EQ(expected_bytes, s_out);
}

INSTANTIATE_TEST_SUITE_P(
    BitCastToBytesTests, BitCastToBytesTest,
    ::testing::ValuesIn(std::vector<BitCastToBytesTestCase>{
        // FLOAT -> BYTES
        {"FloatZero", 0.0f, absl::string_view("\x00\x00\x00\x00", 4)},
        {"FloatNegZero", -0.0f, absl::string_view("\x00\x00\x00\x80", 4)},
        {"FloatOne", 1.0f, absl::string_view("\x00\x00\x80\x3f", 4)},
        {"FloatNegOne", -1.0f, absl::string_view("\x00\x00\x80\xbf", 4)},
        {"FloatMax", float_max, absl::string_view("\xff\xff\x7f\x7f", 4)},
        {"FloatMin", float_min, absl::string_view("\xff\xff\x7f\xff", 4)},
        {"FloatMinPositive", float_min_positive,
         absl::string_view("\x00\x00\x80\x00", 4)},
        {"FloatMinNegative", float_min_negative,
         absl::string_view("\x00\x00\x80\x80", 4)},
        {"FloatPosInf", float_pos_inf,
         absl::string_view("\x00\x00\x80\x7f", 4)},
        {"FloatNegInf", float_neg_inf,
         absl::string_view("\x00\x00\x80\xff", 4)},
        {"FloatNaN", float_nan, absl::string_view("\x00\x00\xc0\x7f", 4)},
        {"FloatNegNaN", float_neg_nan,
         absl::string_view("\x00\x00\xc0\xff", 4)},

        // DOUBLE -> BYTES
        {"DoubleZero", 0.0,
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8)},
        {"DoubleNegZero", -0.0,
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x80", 8)},
        {"DoubleOne", 1.0,
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\x3f", 8)},
        {"DoubleNegOne", -1.0,
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\xbf", 8)},
        {"DoubleMax", double_max,
         absl::string_view("\xff\xff\xff\xff\xff\xff\xef\x7f", 8)},
        {"DoubleMin", double_min,
         absl::string_view("\xff\xff\xff\xff\xff\xff\xef\xff", 8)},
        {"DoubleMinPositive", double_min_positive,
         absl::string_view("\x00\x00\x00\x00\x00\x00\x10\x00", 8)},
        {"DoubleMinNegative", double_min_negative,
         absl::string_view("\x00\x00\x00\x00\x00\x00\x10\x80", 8)},
        {"DoublePosInf", double_pos_inf,
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\x7f", 8)},
        {"DoubleNegInf", double_neg_inf,
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\xff", 8)},
        {"DoubleNaN", double_nan,
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf8\x7f", 8)},
        {"DoubleNegNaN", double_neg_nan,
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf8\xff", 8)},

        // INT32 / UINT32 / INT64 / UINT64 -> BYTES
        {"Int32Zero", int32_t{0}, absl::string_view("\x00\x00\x00\x00", 4)},
        {"Int32One", int32_t{1}, absl::string_view("\x01\x00\x00\x00", 4)},
        {"Int32NegOne", int32_t{-1}, absl::string_view("\xff\xff\xff\xff", 4)},
        {"Int32Max", int32_max, absl::string_view("\xff\xff\xff\x7f", 4)},
        {"Int32Min", int32_min, absl::string_view("\x00\x00\x00\x80", 4)},

        {"Uint32Zero", uint32_t{0}, absl::string_view("\x00\x00\x00\x00", 4)},
        {"Uint32One", uint32_t{1}, absl::string_view("\x01\x00\x00\x00", 4)},
        {"Uint32Max", uint32_max, absl::string_view("\xff\xff\xff\xff", 4)},

        {"Int64Zero", int64_t{0},
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8)},
        {"Int64One", int64_t{1},
         absl::string_view("\x01\x00\x00\x00\x00\x00\x00\x00", 8)},
        {"Int64NegOne", int64_t{-1},
         absl::string_view("\xff\xff\xff\xff\xff\xff\xff\xff", 8)},
        {"Int64Max", int64_max,
         absl::string_view("\xff\xff\xff\xff\xff\xff\xff\x7f", 8)},
        {"Int64Min", int64_min,
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x80", 8)},

        {"Uint64Zero", uint64_t{0},
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8)},
        {"Uint64One", uint64_t{1},
         absl::string_view("\x01\x00\x00\x00\x00\x00\x00\x00", 8)},
        {"Uint64Max", uint64_max,
         absl::string_view("\xff\xff\xff\xff\xff\xff\xff\xff", 8)},

        // Big Endian tests
        {"FloatOneBigEndian", 1.0f, absl::string_view("\x3f\x80\x00\x00", 4),
         Endianness::BIG},
        {"FloatPosInfBigEndian", float_pos_inf,
         absl::string_view("\x7f\x80\x00\x00", 4), Endianness::BIG},
        {"FloatNegInfBigEndian", float_neg_inf,
         absl::string_view("\xff\x80\x00\x00", 4), Endianness::BIG},
        {"FloatNaNBigEndian", float_nan,
         absl::string_view("\x7f\xc0\x00\x00", 4), Endianness::BIG},
        {"FloatMaxBigEndian", float_max,
         absl::string_view("\x7f\x7f\xff\xff", 4), Endianness::BIG},
        {"FloatMinBigEndian", float_min,
         absl::string_view("\xff\x7f\xff\xff", 4), Endianness::BIG},

        {"DoubleOneBigEndian", 1.0,
         absl::string_view("\x3f\xf0\x00\x00\x00\x00\x00\x00", 8),
         Endianness::BIG},
        {"DoublePosInfBigEndian", double_pos_inf,
         absl::string_view("\x7f\xf0\x00\x00\x00\x00\x00\x00", 8),
         Endianness::BIG},
        {"DoubleNegInfBigEndian", double_neg_inf,
         absl::string_view("\xff\xf0\x00\x00\x00\x00\x00\x00", 8),
         Endianness::BIG},
        {"DoubleNaNBigEndian", double_nan,
         absl::string_view("\x7f\xf8\x00\x00\x00\x00\x00\x00", 8),
         Endianness::BIG},
        {"DoubleMaxBigEndian", double_max,
         absl::string_view("\x7f\xef\xff\xff\xff\xff\xff\xff", 8),
         Endianness::BIG},
        {"DoubleMinBigEndian", double_min,
         absl::string_view("\xff\xef\xff\xff\xff\xff\xff\xff", 8),
         Endianness::BIG},

        {"Int32MaxBigEndian", int32_max,
         absl::string_view("\x7f\xff\xff\xff", 4), Endianness::BIG},
        {"Int32MinBigEndian", int32_min,
         absl::string_view("\x80\x00\x00\x00", 4), Endianness::BIG},
        {"Int64MaxBigEndian", int64_max,
         absl::string_view("\x7f\xff\xff\xff\xff\xff\xff\xff", 8),
         Endianness::BIG},
        {"Int64MinBigEndian", int64_min,
         absl::string_view("\x80\x00\x00\x00\x00\x00\x00\x00", 8),
         Endianness::BIG},
    }),
    [](const ::testing::TestParamInfo<BitCastToBytesTest::ParamType>& info) {
      return info.param.test_name;
    });

struct BitCastFromBytesTestCase {
  std::string test_name;
  absl::string_view input_bytes;
  std::variant<float, double, int32_t, uint32_t, int64_t, uint64_t> expected;
  Endianness endianness = Endianness::LITTLE;
};

class BitCastFromBytesTest
    : public ::testing::TestWithParam<BitCastFromBytesTestCase> {};

TEST_P(BitCastFromBytesTest, ConvertsFromBytes) {
  const auto& [test_name, input_bytes, expected, endianness] = GetParam();
  absl::Status status;
  std::visit(
      [&](auto&& expected_val) {
        using T = std::decay_t<decltype(expected_val)>;
        T out{};
        EXPECT_TRUE(BitCastFromBytes(input_bytes, &out, &status, endianness))
            << status;
        if constexpr (std::is_same_v<T, float>) {
          EXPECT_EQ(absl::bit_cast<uint32_t>(expected_val),
                    absl::bit_cast<uint32_t>(out));
        } else if constexpr (std::is_same_v<T, double>) {
          EXPECT_EQ(absl::bit_cast<uint64_t>(expected_val),
                    absl::bit_cast<uint64_t>(out));
        } else {
          EXPECT_EQ(expected_val, out);
        }
      },
      expected);
}

INSTANTIATE_TEST_SUITE_P(
    BitCastFromBytesTests, BitCastFromBytesTest,
    ::testing::ValuesIn(std::vector<BitCastFromBytesTestCase>{
        // BYTES -> FLOAT
        {"BytesToFloatZero", absl::string_view("\x00\x00\x00\x00", 4), 0.0f},
        {"BytesToFloatNegZero", absl::string_view("\x00\x00\x00\x80", 4),
         -0.0f},
        {"BytesToFloatOne", absl::string_view("\x00\x00\x80\x3f", 4), 1.0f},
        {"BytesToFloatNegOne", absl::string_view("\x00\x00\x80\xbf", 4), -1.0f},
        {"BytesToFloatMax", absl::string_view("\xff\xff\x7f\x7f", 4),
         float_max},
        {"BytesToFloatMin", absl::string_view("\xff\xff\x7f\xff", 4),
         float_min},
        {"BytesToFloatMinPositive", absl::string_view("\x00\x00\x80\x00", 4),
         float_min_positive},
        {"BytesToFloatMinNegative", absl::string_view("\x00\x00\x80\x80", 4),
         float_min_negative},
        {"BytesToFloatPosInf", absl::string_view("\x00\x00\x80\x7f", 4),
         float_pos_inf},
        {"BytesToFloatNegInf", absl::string_view("\x00\x00\x80\xff", 4),
         float_neg_inf},
        {"BytesToFloatNaN", absl::string_view("\x00\x00\xc0\x7f", 4),
         float_nan},

        // BYTES -> DOUBLE
        {"BytesToDoubleZero",
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8), 0.0},
        {"BytesToDoubleNegZero",
         absl::string_view("\x00\x00\x00\x00\x00\x00\x00\x80", 8), -0.0},
        {"BytesToDoubleOne",
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\x3f", 8), 1.0},
        {"BytesToDoubleNegOne",
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\xbf", 8), -1.0},
        {"BytesToDoubleMax",
         absl::string_view("\xff\xff\xff\xff\xff\xff\xef\x7f", 8), double_max},
        {"BytesToDoubleMin",
         absl::string_view("\xff\xff\xff\xff\xff\xff\xef\xff", 8), double_min},
        {"BytesToDoubleMinPositive",
         absl::string_view("\x00\x00\x00\x00\x00\x00\x10\x00", 8),
         double_min_positive},
        {"BytesToDoubleMinNegative",
         absl::string_view("\x00\x00\x00\x00\x00\x00\x10\x80", 8),
         double_min_negative},
        {"BytesToDoublePosInf",
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\x7f", 8),
         double_pos_inf},
        {"BytesToDoubleNegInf",
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf0\xff", 8),
         double_neg_inf},
        {"BytesToDoubleNaN",
         absl::string_view("\x00\x00\x00\x00\x00\x00\xf8\x7f", 8), double_nan},

        // BYTES -> INTEGERS
        {"BytesToInt32", absl::string_view("\x78\x56\x34\x12", 4),
         int32_t{0x12345678}},
        {"BytesToUint32", absl::string_view("\x78\x56\x34\x12", 4),
         uint32_t{0x12345678U}},
        {"BytesToInt64",
         absl::string_view("\x88\x77\x66\x55\x44\x33\x22\x11", 8),
         int64_t{0x1122334455667788LL}},
        {"BytesToUint64",
         absl::string_view("\x88\x77\x66\x55\x44\x33\x22\x11", 8),
         uint64_t{0x1122334455667788ULL}},

        // Big Endian tests
        {"BytesToFloatOneBigEndian", absl::string_view("\x3f\x80\x00\x00", 4),
         1.0f, Endianness::BIG},
        {"BytesToDoubleOneBigEndian",
         absl::string_view("\x3f\xf0\x00\x00\x00\x00\x00\x00", 8), 1.0,
         Endianness::BIG},
    }),
    [](const ::testing::TestParamInfo<BitCastFromBytesTest::ParamType>& info) {
      return info.param.test_name;
    });

}  // namespace functions
}  // namespace googlesql
