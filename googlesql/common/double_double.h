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

// Double-double and double-quad arithmetic.
//
// Double-double arithmetic is a technique for portably performing
// floating-point computations with more mantissa precision than an IEEE-754
// double can provide and more speed than a software-defined IEEE-754 quad can
// provide. Introduced by Dekker in 1971 [1], it involves representing a value
// as the uninterpreted sum of two doubles (hence a "double `double`").
// Efficient algorithms for a number of fundamental operations exist over this
// representation; however, the double double is not an IEEE-754 type, and using
// it requires some care.
//
// A double quad is the uninterpreted sum of two quads. Double-quad operations
// are quite slow, but they are sometimes useful for computations that require
// very high precision. Like the double double, the double quad is not an
// IEEE-754 type.
//
// This is not a complete double-double implementation. It has known bugs
// surrounding overflow, and parts of the API are missing. Only use it if you
// are sure you know what you're doing.
//
// Reference:
//   1. T.J. Dekker, "A Floating-Point Technique for Extending the Available
//      Precision" (1971), Numerische Mathematik 18, pages 224-242
//      (https://doi.org/10.1007/BF01397083).
#ifndef GOOGLESQL_COMMON_DOUBLE_DOUBLE_H_
#define GOOGLESQL_COMMON_DOUBLE_DOUBLE_H_

#include <cmath>
#include <limits>
#include <utility>

#include "absl/base/macros.h"
#include "absl/base/optimization.h"
#include "absl/types/compare.h"

namespace googlesql {
namespace util_math {

// A generic double-foo representation. Instantiated with double, it produces a
// double-double class. (Instantiated with float, it produces a double-float
// class, but this is mostly a curiosity on a system that supports doubles in
// hardware.)
//
// To avoid confusion, we follow the naming convention established in [1],
// referring to the template parameter as "half-precision" and the resulting
// class as "full-precision".
//
// The API of this class has only been implemented as far as necessary for
// downstreams. Omissions from the API should not be considered deliberate
// design decisions.
//
// This class is thread-compatible.
//
// Reference:
//   1. Alan H. Karp and Peter Markstein, "High-Precision Division and Square
//      Root" (1997), ACM Transactions on Mathematical Software 23(4),
//      pages 561-589 (https://doi.org/10.1145/279232.279237).
template <typename Half>
class Full final {
 public:
  static constexpr Full FromSum(Half a, Half b) {
    // Implement 2SUM algorithm from:
    // J.-M. Muller et. al., "Handbook of Floating-Point Arithmetic", 2nd ed.,
    // Section 4.3.2 (https://doi.org/10.1007/978-3-319-76526-6).
    Full z;
    z.hi_ = a + b;
    Half a1 = z.hi_ - b;
    Half b1 = z.hi_ - a1;
    Half delta_a = a - a1;
    Half delta_b = b - b1;
    z.lo_ = delta_a + delta_b;
    z.FixOverflow();
    return z;
  }

  // Compute the lower part of a full product using Dekker's product algorithm,
  // assuming that the high part of the product is already computed.
  // The result is normalized to guaranteed that |z.lo_| <= ulp(z.hi_)/2.
  // To be used when the FMA instructions are not available.
  static constexpr void ComputeDekkerProduct(Half a, Half b, Full& z) {
    // If z.hi_ has already overflowed, don't let any of the below possibly
    // turn this into NaN.
    if (ABSL_PREDICT_FALSE(!std::isfinite(z.hi_))) {
      z.lo_ = 0;
      return;
    }
    Full as = Split(a), bs = Split(b);
    Half hi_hi = as.hi_ * bs.hi_;
    // It's also possible for this to overflow even if a * b didn't.
    if (ABSL_PREDICT_FALSE(!std::isfinite(hi_hi))) {
      z.hi_ = hi_hi;  // Return the infinity or NaN we got.
      z.lo_ = 0;
      return;
    }
    z.lo_ = -z.hi_ + hi_hi + static_cast<Half>(as.hi_ * bs.lo_) +
            static_cast<Half>(as.lo_ * bs.hi_) +
            static_cast<Half>(as.lo_ * bs.lo_);
    // Fast2SumUnchecked assumptions will be violated if this has overflowed,
    // possibly producing a NaN.
    if (ABSL_PREDICT_FALSE(!std::isfinite(z.lo_))) {
      z.hi_ = z.lo_;  // Return the infinity or NaN we got.
      z.lo_ = 0;
      return;
    }
    // Make sure that |z.lo_| <= ulp(z.hi_)/2.
    // (ULP means unit in the last place, see http://shortn/_HsXZ06aKXC.)
    // Without this extra normalization step, |z.lo_| might be slightly larger
    // than ulp(z_hi_)/2 when underflow happens.
    z = Fast2SumUnchecked(z.hi_, z.lo_);
  }

  static constexpr Full FromProduct(Half a, Half b) {
    Full z;
    z.hi_ = a * b;
#if defined(FP_FAST_FMA) || defined(__aarch64__) || defined(__FMA__) || \
    defined(__CUDA_ARCH__)
    if constexpr (std::is_same_v<Half, float> || std::is_same_v<Half, double>) {
      z.lo_ = std::fma(a, b, -z.hi_);
    } else  // NOLINT(readability/braces)
#endif
    {
      // The precision of the full product is 2 * the precision of Half.
      // Without FMA instructions, computing the lower part of the full product
      // might be underflow if the least significant bit of the full product is
      // smaller than the smallest denormal value.  The least significant bit of
      // the full product is about:
      //   2^(-2 * precision of Half) * |full product|`.
      // So we can simply (a bit overkill) check if the absolute value of the
      // full product is at least 2^(-2 * precision of Half) * smallest denormal
      // value, or smallest normal value to have some wiggle room.  If it's not,
      // the smaller absolute value of a and b must be less than 1.  Hence we
      // can scale up the one with smaller absolute value by 2^(2 * precision of
      // Half) to prevent the intermediate computations from underflowing, and
      // then scale the final result down by the same amount.
      // Notice that we choose the scaling factor to be a power of 2 to avoid
      // rounding.
      // For float, precision(float) = 24, so the scaling factor is 2^48.
      // For double, precision(double) = 53, so the scaling factor is 2^106.
      // For 128-bit float with 113-bit precision, the scaling
      // factor is 2^226.
      constexpr Half scaling_up_factor = [] {
        if (std::is_same_v<Half, float>) return 0x1.0p48;
        if (std::is_same_v<Half, double>) return 0x1.0p106;
        // We assume everything else has 128-bit precision.
        return 0x1.0p226;
      }();

      if (std::fabs(z.hi_) >=
          scaling_up_factor * std::numeric_limits<Half>::min()) {
        ComputeDekkerProduct(a, b, z);
      } else {
        // The full product
        z.hi_ *= scaling_up_factor;
        if (std::fabs(a) < std::fabs(b)) {
          a *= scaling_up_factor;
        } else {
          b *= scaling_up_factor;
        }
        ComputeDekkerProduct(a, b, z);
        z.hi_ /= scaling_up_factor;
        z.lo_ /= scaling_up_factor;
      }
    }
    z.FixOverflow();
    return z;
  }

  // NOLINTNEXTLINE(google-explicit-constructor)
  constexpr Full(Half a) : hi_(a), lo_(Half{0.0}) {}
  constexpr explicit Full(Half a, Half b) : hi_(a), lo_(b) {
    // Replacement for std::isnan and std::abs as they are not constexpr.
    auto is_nan_constexpr = [](Half x) -> bool { return x != x; };
    auto abs_constexpr = [](Half x) -> Half { return (x < 0) ? -x : x; };
    ABSL_HARDENING_ASSERT(
        is_nan_constexpr(a) || is_nan_constexpr(b) ||
        (abs_constexpr(a) * std::numeric_limits<Half>::epsilon() * 0.5f >=
         abs_constexpr(b)));
  }
  constexpr explicit Full() : Full(Half{0.0}) {}

  constexpr Full(const Full&) = default;
  constexpr Full& operator=(const Full&) = default;

  constexpr Half hi() const { return hi_; }
  constexpr Half lo() const { return lo_; }
  constexpr explicit operator Half() const { return lo_ + hi_; }

  friend constexpr Full operator+(const Full& a, Half b) {
    Full z = FromSum(a.hi_, b);
    z.lo_ = z.lo_ + a.lo_;
    z.FixOverflow();
    return Fast2SumUnchecked(z.hi_, z.lo_);
  }

  friend constexpr Full operator+(Half a, const Full& b) { return b + a; }

  // Error bound:
  //      |out.hi + out.lo - (a.hi + a.lo + b.hi + b.lo)|
  //   <= 1.5 * 2^-52 * max(ulp(a.hi), ulp(b.hi))
  //   <= 1.5 * 2^-104 * max(|a.hi|, |b.hi|)
  friend constexpr Full operator+(const Full& a, const Full& b) {
    Full z = FromSum(a.hi_, b.hi_);
    z.lo_ += a.lo_ + b.lo_;
    z.FixOverflow();
    return Fast2SumUnchecked(z.hi_, z.lo_);
  }

  friend constexpr Full operator-(const Full& a) {
    return Full{-a.hi_, -a.lo_};
  }

  friend constexpr Full operator-(const Full& a, const Full& b) {
    return a + -b;
  }

  friend constexpr Full operator-(const Full& a, Half b) { return a + -b; }
  friend constexpr Full operator-(Half a, const Full& b) { return a + -b; }

  friend constexpr Full operator*(const Full& a, Half b) {
    Full z = FromProduct(a.hi_, b);
    z.lo_ = z.lo_ + a.lo_ * b;
    z.FixOverflow();
    return Fast2SumUnchecked(z.hi_, z.lo_);
  }

  friend constexpr Full operator*(Half a, const Full& b) { return b * a; }

  friend constexpr Full operator*(const Full& a, const Full& b) {
    Full z = FromProduct(a.hi_, b.hi_);
    z.lo_ = a.hi_ * b.lo_ + z.lo_ + a.lo_ * b.hi_;
    z.FixOverflow();
    return Fast2SumUnchecked(z.hi_, z.lo_);
  }

  friend constexpr bool operator==(const Full& a, const Full& b) {
    return a.hi_ == b.hi_ && a.lo_ == b.lo_;
  }

// Note: Some open-source / third_party projects still need to support C++17,
// and operator<=> is a C++20 feature.
#ifdef __cpp_impl_three_way_comparison
  friend constexpr absl::partial_ordering operator<=>(const Full& a,
                                                      const Full& b) {
    if (absl::partial_ordering hi_ordering = a.hi_ <=> b.hi_;
        hi_ordering != absl::partial_ordering::equivalent) {
      return hi_ordering;
    }
    return a.lo_ <=> b.lo_;
  }
#else
  friend constexpr bool operator!=(const Full& a, const Full& b) {
    return a.hi_ != b.hi_ || a.lo_ != b.lo_;
  }
  friend constexpr bool operator<(const Full& a, const Full& b) {
    return a.hi_ < b.hi_ || (a.hi_ == b.hi_ && a.lo_ < b.lo_);
  }
  friend constexpr bool operator<=(const Full& a, const Full& b) {
    return a.hi_ < b.hi_ || (a.hi_ == b.hi_ && a.lo_ <= b.lo_);
  }
  friend constexpr bool operator>(const Full& a, const Full& b) {
    return a.hi_ > b.hi_ || (a.hi_ == b.hi_ && a.lo_ > b.lo_);
  }
  friend constexpr bool operator>=(const Full& a, const Full& b) {
    return a.hi_ > b.hi_ || (a.hi_ == b.hi_ && a.lo_ >= b.lo_);
  }
#endif  // __cpp_impl_three_way_comparison

  constexpr Full& operator+=(const Full& b) { return *this = *this + b; }
  constexpr Full& operator-=(const Full& b) { return *this = *this - b; }
  constexpr Full& operator*=(const Full& b) { return *this = *this * b; }

  constexpr Full& normalize() { return *this = FromSum(hi_, lo_); }

  // Dekker's Fast2Sum algorithm:
  // If there is no overflow, and the exponent part of the first input is
  // greater than or equal to the exponent part of the second input, or either
  // of the inputs is zero, then the output is exact (assuming the default
  // rounding mode is round-to-nearest):
  //   z.hi_ + z.lo_ = a + b,
  // and
  //   |z.lo_| <= ulp(z.hi_)/2.
  static constexpr Full Fast2Sum(Half a, Half b) {
    using ::std::isnan;  // NOLINT(misc-include-cleaner)
    // This precondition might be violated benignly when calling from
    // operator+(Full, Full) or operator*().
    ABSL_HARDENING_ASSERT(isnan(a) || isnan(b) || a == Half{0.0} ||
                          b == Half{0.0} || ExponentOf(a) >= ExponentOf(b));
    return Fast2SumUnchecked(a, b);
  }

 private:
  // Extracts the binary exponent from a floating-point type.
  template <typename T>
  static constexpr int ExponentOf(T a) {
    using ::std::ilogb;  // ADL, since Half might be a custom float type
    return ilogb(a);
  }

  // When z.lo_ is not finite, z.hi_ is also not finite, and the sum
  // z.lo_ + z.hi_ might not be the same as z.hi_ since z.lo_ might be NaN
  // or Inf of the opposite sign of z.hi_.  So we set z.lo_ to 0 to maintain
  // that z.lo_ + z.hi_ = z.hi_ when overflow happens.
  constexpr void FixOverflow() {
    if (ABSL_PREDICT_FALSE(!std::isfinite(lo_))) {
      lo_ = 0;
    }
  }

  // Dekker's Fast2Sum algorithm:
  // If there is no overflow, the output is exact (assuming the default rounding
  // mode is round-to-nearest):
  //   z.hi_ + z.lo_ = a + b,
  // and
  //   |z.lo_| <= ulp(z.hi_)/2.
  static constexpr Full Fast2SumUnchecked(Half a, Half b) {
    Full z;
    z.hi_ = a + b;
    z.lo_ = b - static_cast<Half>(z.hi_ - a);
    z.FixOverflow();
    return z;
  }

  // Veltkamp's split.
  // This is actually ill-formed even though our compiler appears to accept it.
  // std::ldexp is not constexpr until c++23.
  // This comment can be removed once c++23 is supported.
  static constexpr Full Split(Half a) {
    // Splitting constant: 2^ceil(prec(Half) / 2) + 1.
    using ::std::ldexp;  // NOLINT(misc-include-cleaner)
    const Half kC_pow_2 =
        ldexp(Half{1.0}, (std::numeric_limits<Half>::digits + 1) / 2);
    const Half kC = kC_pow_2 + Half{1.0};
    const Half kOverflowScalingFactor = Half{0.5} / kC_pow_2;
    const Half kOverflowBound =
        std::numeric_limits<Half>::max() * kOverflowScalingFactor;

    Half scaling_factor = Half{1.0};

    if (ABSL_PREDICT_FALSE(std::fabs(a) >= kOverflowBound)) {
      scaling_factor = Half{1.0} / kOverflowScalingFactor;
      a *= kOverflowScalingFactor;
    }

    Full z;

    // This is a possible source of overflow. The algorithm is effectively
    // rounding z.hi_ to only have mantissa bits set in its upper half. This
    // can round up from a value that's very close to infinity, to infinity.
    Half t1 = kC * a;
    z.hi_ = t1 + static_cast<Half>(a - t1);
    z.lo_ = (a - z.hi_) * scaling_factor;
    z.hi_ *= scaling_factor;

    return z;
  }

  Half hi_, lo_;
};

template <typename Half>
constexpr std::pair<Full<Half>, Full<Half>> ExactProduct(Half a,
                                                         const Full<Half>& b) {
  return std::make_pair(Full<Half>::FromProduct(a, b.hi()),
                        Full<Half>::FromProduct(a, b.lo()));
}

using DoubleDouble = Full<double>;

}  // namespace util_math
}  // namespace googlesql

#endif  // GOOGLESQL_COMMON_DOUBLE_DOUBLE_H_
