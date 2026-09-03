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

#include "googlesql/public/functions/math.h"

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "googlesql/common/multiprecision_int.h"
#include "googlesql/common/round_decimal.h"
#include "googlesql/public/functions/rounding_mode.pb.h"
#include "googlesql/public/numeric_value.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"

namespace googlesql {
namespace functions {
namespace {


// This function assumes that the FromType is same or wider than the ToType.
template <typename FromType, typename ToType>
static inline bool CastRounded(FromType in, ToType* out) {
  static_assert(std::is_floating_point_v<FromType>,
                "FromType must be floating point type");
  static_assert(std::is_floating_point_v<ToType>,
                "ToType must be floating point type");
  static_assert(std::numeric_limits<FromType>::lowest() <=
                        std::numeric_limits<ToType>::lowest() &&
                    std::numeric_limits<FromType>::max() >=
                        std::numeric_limits<ToType>::max(),
                "FromType must be wider than or equal to ToType");
  if ((in >= std::numeric_limits<ToType>::lowest() &&
       in <= std::numeric_limits<ToType>::max()) ||
      ABSL_PREDICT_FALSE(!std::isfinite(in))) {
    *out = static_cast<ToType>(in);
    return true;
  } else {
    return false;
  }
}

}  // anonymous namespace

template <>
bool RoundDecimal(double in, int64_t digits, double* out, absl::Status* error) {
  *out = googlesql::util_math::RoundDecimal(in, digits);
  if (std::isfinite(in) && !std::isfinite(*out)) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("ROUND(", in, ", ", digits, ")"), error);
  }
  return true;
}

template <>
bool RoundDecimal(float in, int64_t digits, float* out, absl::Status* error) {
  double rounded = googlesql::util_math::RoundDecimal(in, digits);
  if (!CastRounded<double, float>(rounded, out)) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("ROUND(", in, ", ", digits, ")"), error);
  }
  return true;
}

template <>
bool TruncDecimal(double in, int64_t digits, double* out, absl::Status* error) {
  *out = googlesql::util_math::TruncDecimal(in, digits);
  return true;
}

template <>
bool TruncDecimal(float in, int64_t digits, float* out, absl::Status* error) {
  // Because TruncDecimal always rounds towards zero, the absolute value of the
  // output is less than or equal to the absolute value of the input. Thus,
  // converting the result from double back to float cannot overflow for a
  // finite input, and we can safely cast without overflow checks.
  *out = static_cast<float>(googlesql::util_math::TruncDecimal(in, digits));
  return true;
}

double Pi() { return M_PI; }

template <>
bool Radians(double in, double* out, absl::Status* error) {
  static const double value_pi_over_180 = M_PI / 180.0;
  *out = in * value_pi_over_180;
  return internal::CheckFloatingPointError("RADIANS", in, *out, error);
}

template <>
bool Degrees(double in, double* out, absl::Status* error) {
  static const double value_180_over_pi = 180.0 / M_PI;
  *out = in * value_180_over_pi;
  return internal::CheckFloatingPointError("DEGREES", in, *out, error);
}

namespace {
template <typename T>
inline bool SetNumericResultOrError(const absl::StatusOr<T>& status_or_numeric,
                                    T* out, absl::Status* error) {
  if (ABSL_PREDICT_TRUE(status_or_numeric.ok())) {
    *out = status_or_numeric.value();
    return true;
  }
  error->Update(status_or_numeric.status());
  return false;
}
}  // namespace

template <>
bool Round(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Round(0), out, error);
}

template <>
bool RoundDecimal(NumericValue in, int64_t digits, NumericValue* out,
                  absl::Status* error) {
  return SetNumericResultOrError(in.Round(digits), out, error);
}

template <>
bool RoundDecimalWithRoundingMode(NumericValue in, int64_t digits,
                                  RoundingMode rounding_mode, NumericValue* out,
                                  absl::Status* error) {
  if (rounding_mode == RoundingMode::ROUND_HALF_EVEN) {
    return SetNumericResultOrError(in.Round(digits, true), out, error);
  } else {
    return SetNumericResultOrError(in.Round(digits, false), out, error);
  }
}

template <>
bool Trunc(NumericValue in, NumericValue* out, absl::Status* error) {
  *out = in.Trunc(0);
  return true;
}

template <>
bool TruncDecimal(NumericValue in, int64_t digits, NumericValue* out,
                  absl::Status* error) {
  *out = in.Trunc(digits);
  return true;
}

template <>
bool Ceil(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Ceiling(), out, error);
}

template <>
bool Floor(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Floor(), out, error);
}

template <>
bool Sqrt(NumericValue in, NumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Sqrt(), out, error);
}

template <>
bool Cbrt(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Cbrt(), out, error);
}

template <>
bool Pow(NumericValue in1, NumericValue in2, NumericValue* out,
         absl::Status* error) {
  return SetNumericResultOrError(in1.Power(in2), out, error);
}

template <>
bool Exp(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Exp(), out, error);
}

template <>
bool NaturalLogarithm(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Ln(), out, error);
}

template <>
bool DecimalLogarithm(NumericValue in, NumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Log10(), out, error);
}

template <>
bool Logarithm(NumericValue in1, NumericValue in2, NumericValue* out,
               absl::Status* error) {
  return SetNumericResultOrError(in1.Log(in2), out, error);
}

NumericValue Pi_Numeric() {
  return *NumericValue::FromPackedInt(3141592654ULL);
}

template <>
bool Radians(NumericValue in, NumericValue* out, absl::Status* error) {
  // Represents the 128-bit numerator 95024763027997044254193810947721847404
  // which is approximately 2^132 * (pi / 180)
  constexpr FixedInt<64, 2> scaled_pi_over_180 = FixedInt<64, 2>(
      std::array<uint64_t, 2>{0x762FB374A42E26DULL, 0x477D1A894A74E457ULL});
  constexpr uint N = 132;
  const auto status_or_numeric =
      in.MultiplyAndDivideByPowerOfTwo(scaled_pi_over_180, N);
  if (!status_or_numeric.ok()) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("RADIANS(", in.ToString(), ")"), error);
  }
  *out = status_or_numeric.value();
  return true;
}

template <>
bool Degrees(NumericValue in, NumericValue* out, absl::Status* error) {
  // Represents the number (180 / pi) * 2^121
  constexpr FixedInt<64, 2> scaled_180_over_pi = FixedInt<64, 2>(
      std::array<uint64_t, 2>{0x854BA9BFA0692BECULL, 0x729770698F07DEE1ULL});
  constexpr uint N = 121;
  const auto status_or_numeric =
      in.MultiplyAndDivideByPowerOfTwo(scaled_180_over_pi, N);
  if (!status_or_numeric.ok()) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("DEGREES(", in.ToString(), ")"), error);
  }
  *out = status_or_numeric.value();
  return true;
}

template <>
bool Ceil(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Ceiling(), out, error);
}

template <>
bool Floor(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Floor(), out, error);
}

template <>
bool Round(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Round(0), out, error);
}

template <>
bool RoundDecimal(BigNumericValue in, int64_t digits, BigNumericValue* out,
                  absl::Status* error) {
  return SetNumericResultOrError(in.Round(digits), out, error);
}
template <>
bool RoundDecimalWithRoundingMode(BigNumericValue in, int64_t digits,
                                  RoundingMode rounding_mode,
                                  BigNumericValue* out, absl::Status* error) {
  if (rounding_mode == RoundingMode::ROUND_HALF_EVEN) {
    return SetNumericResultOrError(in.Round(digits, true), out, error);
  } else {
    return SetNumericResultOrError(in.Round(digits, false), out, error);
  }
}

template <>
bool Trunc(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  *out = in.Trunc(0);
  return true;
}

template <>
bool TruncDecimal(BigNumericValue in, int64_t digits, BigNumericValue* out,
                  absl::Status* error) {
  *out = in.Trunc(digits);
  return true;
}

template <>
bool Sqrt(BigNumericValue in, BigNumericValue *out, absl::Status* error) {
  return SetNumericResultOrError(in.Sqrt(), out, error);
}

template <>
bool Cbrt(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Cbrt(), out, error);
}

template <>
bool Pow(BigNumericValue in1, BigNumericValue in2, BigNumericValue* out,
         absl::Status* error) {
  return SetNumericResultOrError(in1.Power(in2), out, error);
}

template <>
bool Exp(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  return SetNumericResultOrError(in.Exp(), out, error);
}

template <>
bool NaturalLogarithm(BigNumericValue in, BigNumericValue* out,
                      absl::Status* error) {
  return SetNumericResultOrError(in.Ln(), out, error);
}

template <>
bool DecimalLogarithm(BigNumericValue in, BigNumericValue* out,
                      absl::Status* error) {
  return SetNumericResultOrError(in.Log10(), out, error);
}

template <>
bool Logarithm(BigNumericValue in1, BigNumericValue in2, BigNumericValue* out,
               absl::Status* error) {
  return SetNumericResultOrError(in1.Log(in2), out, error);
}

BigNumericValue Pi_BigNumeric() {
  constexpr uint64_t lo = 0xAD0D16E77D576624ULL;
  constexpr uint64_t hi = 0xEC58DFA74641AF52ULL;
  return BigNumericValue::FromPackedLittleEndianArray({lo, hi, 0ULL, 0ULL});
}

template <>
bool Radians(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  // A 256-bit integer representing (pi / 180) * 2^260
  constexpr FixedInt<64, 4> scaled_pi_over_180 =
      FixedInt<64, 4>(std::array<uint64_t, 4>({
          0x728154DA64A64289ULL,
          0x805BD77A80DAF35CULL,
          0x0762FB374A42E26CULL,
          0x477D1A894A74E457ULL,
      }));
  constexpr int N = 260;

  const auto status_or_numeric =
      in.MultiplyAndDivideByPowerOfTwo(scaled_pi_over_180, N);
  if (!status_or_numeric.ok()) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("RADIANS(", in.ToString(), ")"), error);
  }
  *out = status_or_numeric.value();
  return true;
}

template <>
bool Degrees(BigNumericValue in, BigNumericValue* out, absl::Status* error) {
  constexpr FixedInt<64, 4> scaled_180_over_pi =
      FixedInt<64, 4>(std::array<uint64_t, 4>({
          0x66D13A14D89C06C9ULL,
          0x9A41512FBE5F816EULL,
          0x854BA9BFA0692BEBULL,
          0x729770698F07DEE1ULL,
      }));
  constexpr int N = 249;

  const auto status_or_numeric =
      in.MultiplyAndDivideByPowerOfTwo(scaled_180_over_pi, N);
  if (!status_or_numeric.ok()) {
    return internal::SetFloatingPointOverflow(
        absl::StrCat("DEGREES(", in.ToString(), ")"), error);
  }
  *out = status_or_numeric.value();
  return true;
}

}  // namespace functions
}  // namespace googlesql
