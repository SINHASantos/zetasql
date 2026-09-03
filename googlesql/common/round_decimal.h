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

#ifndef GOOGLESQL_COMMON_ROUND_DECIMAL_H_
#define GOOGLESQL_COMMON_ROUND_DECIMAL_H_

#include <cstdint>

namespace googlesql {
namespace util_math {

// Simulates decimal rounding in the double domain. You can imagine that
//
//   RoundDecimal(in, digits) = std::round(in * 10^digits) / 10^digits
//
// except that `RoundDecimal` will not lose precision and is not vulnerable to
// overflow. More precisely, `RoundDecimal(in, digits)` interprets `in` as a
// real number, rounds it to the specified multiple of 10^digits (rounding to
// nearest with ties to even), and then finds the double that is closest to that
// result (again, rounding to nearest with ties to even).
//
// Because negative powers of ten cannot be represented as doubles, this
// function cannot always return an exact result. For example,
// RoundDecimal(12.345, 0) returns 12.0 exactly, but RoundDecimal(12.345, -1)
// does not return 12.3 but rather
//
//   12.300000000000000710542735760100185871124267578125​
//
// This is the double that is closest to 12.3 (and is in fact the value that
// Clang emits if you type "12.3" in C++, so `RoundDecimal(12.345, -1) == 12.3`
// evaluates to `true`), but it is not equal to 12.3 in the mathematical sense.
// If you find this concerning, consider changing your data model to avoid
// rounding to negative powers of ten.
double RoundDecimal(double in, int64_t digits);

// TruncDecimal(in, digits, ...) returns the double precision that is closest to
//   10^(-digits) * trunc( in * 10^(digits) )
double TruncDecimal(double in, int64_t digits);

}  // namespace util_math
}  // namespace googlesql

#endif  // GOOGLESQL_COMMON_ROUND_DECIMAL_H_
