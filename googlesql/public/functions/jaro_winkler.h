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

#ifndef GOOGLESQL_PUBLIC_FUNCTIONS_JARO_WINKLER_H_
#define GOOGLESQL_PUBLIC_FUNCTIONS_JARO_WINKLER_H_

#include <optional>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace functions {

// Returns the Jaro-Winkler similarity score between two strings `s0` and `s1`
// as a value in the range [0.0, 1.0].
//
// Strings are decoded and compared as UTF-8 Unicode code points. Returns an
// `absl::OutOfRangeError` if either input string contains invalid UTF-8.
//
// Optional parameters:
// - `prefix_scaling_factor`: Scaling factor `p` for the Winkler prefix boost.
//   Must be in [0.0, 0.25] range, defaults to 0.1.
// - `prefix_boost_threshold`: Jaro similarity threshold above which the
//   Winkler prefix boost is applied. Must be in [0.0, 1.0] range, defaults to
//   0.7.
//
// Returns `absl::OutOfRangeError` if either parameter is NaN or out of range.
absl::StatusOr<double> JaroWinklerSimilarity(
    absl::string_view s0, absl::string_view s1,
    std::optional<double> prefix_scaling_factor = std::nullopt,
    std::optional<double> prefix_boost_threshold = std::nullopt);

// Returns the Jaro-Winkler similarity score between two byte sequences `s0` and
// `s1` as a value in the range [0.0, 1.0].
//
// Sequences are compared byte-by-byte.
//
// Optional parameters:
// - `prefix_scaling_factor`: Scaling factor `p` for the Winkler prefix boost.
//   Must be in [0.0, 0.25] range, defaults to 0.1.
// - `prefix_boost_threshold`: Jaro similarity threshold above which the
//   Winkler prefix boost is applied. Must be in [0.0, 1.0] range, defaults to
//   0.7.
//
// Returns `absl::OutOfRangeError` if either parameter is NaN or out of range.
absl::StatusOr<double> JaroWinklerSimilarityBytes(
    absl::string_view s0, absl::string_view s1,
    std::optional<double> prefix_scaling_factor = std::nullopt,
    std::optional<double> prefix_boost_threshold = std::nullopt);

}  // namespace functions
}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_FUNCTIONS_JARO_WINKLER_H_
