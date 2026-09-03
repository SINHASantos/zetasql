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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <optional>
#include <vector>

#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "unicode/umachine.h"
#include "unicode/utf8.h"

namespace googlesql {
namespace functions {
namespace {

// Implementation of the Jaro-Winkler string similarity measure.
//
// Jaro-Winkler is a fuzzy/approximate string matching measure:
// comparing identical strings gives 1.0, comparing completely
// dissimilar strings gives 0.0.  First, the "Jaro counting" counts the
// number of matching items within a band and the number of transposed
// items, and forms a measure out of those, normalized by the string
// lengths.  Second, if the Jaro measure is high enough (default: 0.7 or
// higher), the Winkler correction boosts the measure for strings that
// match well at their beginnings (at most 4 characters, by default).
//
// Note that even as we speak here of "strings" and characters, we can
// instantiate the JaroWinkler class to many other sequences or
// collections, see below for the requirements.
//
// Sample usage:
//
// JaroWinkler<absl::string_view> jw;
// double sim = jw.Similarity("alf", "alfred");
//
// Links:
// https://en.wikipedia.org/wiki/Jaro-Winkler_distance
// https://www.census.gov/geo/msb/stand/strcmp.c
//
// Note that this implementation by design does not do any sort of
// "normalization" or "canonicalization" of the inputs, such as
// lowercasing, or Unicode compose/decompose/normalization.  Anything
// like that (probably including caching) is expected to be handled by
// whoever is using the templated JaroWinkler class.
struct JaroWinklerParam {
  JaroWinklerParam()
      : prefix_correction_threshold(0.7),
        prefix_correction_max_size(4),
        prefix_scaling_factor(0.1) {}
  // Jaro similarities lower than this threshold do not get
  // the Winkler prefix correction added to them.
  double prefix_correction_threshold;
  // For the Winkler prefix correction, consider at most this many
  // items from the beginning.
  int32_t prefix_correction_max_size;
  // The scaling factor for the prefix correction.
  double prefix_scaling_factor;
};

// The class/type T needs to support the following methods/operators:
// t.empty()
// t.size()
// t[i]
// and the t[i] need to support the == and != operators.
template <class T>
class JaroWinkler {
 public:
  explicit JaroWinkler(const JaroWinklerParam& param) : param_(param) {}

  JaroWinkler() : param_(DefaultParam()) {}

  static const JaroWinklerParam& DefaultParam() {
    static const JaroWinklerParam kJaroWinklerDefaultParam;
    return kJaroWinklerDefaultParam;
  }

  // JaroSimilarity() is the similarity [0.0, 1.0] between the strings
  // given the numbers of matching and transposed characters.  Usually
  // you will want to use the Similarity() method instead, which is
  // Jaro similarity with the Winkler prefix correction.
  double JaroSimilarity(const T& t1, const T& t2) const {
    // Sanity check: if one vector is empty, no match,
    // except that if both are empty, match.
    if (t1.empty()) {
      return t2.empty() ? 1.0 : 0.0;
    } else if (t2.empty()) {
      return 0.0;
    }

    JaroCounts counts = JaroCount(t1, t2);

    if (counts.matches == 0) return 0.0;

    return (counts.matches / static_cast<double>(t1.size()) +
            counts.matches / static_cast<double>(t2.size()) +
            (counts.matches - counts.transposes) /
                static_cast<double>(counts.matches)) /
           3.0;
  }

  // Similarity is JaroDistance with the Winkler prefix correction.
  double Similarity(const T& t1, const T& t2) const {
    double sim = JaroSimilarity(t1, t2);
    if (sim <= param_.prefix_correction_threshold) return sim;
    return sim + WinklerPrefixCorrection(t1, t2, sim);
  }

  // Distance is simply 1.0 - Similarity.
  double Distance(const T& t1, const T& t2) const {
    return 1.0 - Similarity(t1, t2);
  }

  // In case one needs to inspect the parameters.
  // (One can set them only in constructor time.)
  const JaroWinklerParam& param() const { return param_; }

 private:
  struct JaroCounts {
    int matches = 0;
    int transposes = 0;
  };

  JaroCounts JaroCount(const T& t1, const T& t2) const {
    JaroCounts counts;

    // Get the width of the band inside which we will look for matches
    // and transposes.  The width of the band is half of the length of
    // the longer string, minus one, but at least zero.
    int max_size = static_cast<int>(std::max(t1.size(), t2.size()));
    int search_range = max_size < 2 ? 0 : max_size / 2 - 1;

    // We need to record which at positions we have matched.
    std::vector<bool> m1(t1.size(), false);
    std::vector<bool> m2(t2.size(), false);

    // First count the matches.
    for (int i1 = 0; i1 < t1.size(); i1++) {
      int lo2 = std::max(0, i1 - search_range);
      int hi2 = std::min(static_cast<int>(i1 + search_range),
                         static_cast<int>(t2.size() - 1));
      for (int i2 = lo2; i2 <= hi2; i2++) {
        if (!m2[i2] && t1[i1] == t2[i2]) {
          m1[i1] = m2[i2] = true;
          counts.matches++;
          break;
        }
      }
    }

    if (counts.matches == 0) return counts;

    // Then count the transposes.  We first count "half-transposes",
    // and then divide by two.
    int i2 = 0;
    for (int i1 = 0; i1 < t1.size(); i1++) {
      if (m1[i1]) {
        int i3;
        for (i3 = i2; i3 < t2.size(); i3++) {
          if (m2[i3]) {
            i2 = i3 + 1;
            break;
          }
        }
        if (t1[i1] != t2[i3]) {
          counts.transposes++;
        }
      }
    }

    counts.transposes /= 2;
    return counts;
  }

  double WinklerPrefixCorrection(const T& t1, const T& t2,
                                 double jaro_similarity) const {
    int limit = std::min(static_cast<int>(param_.prefix_correction_max_size),
                         static_cast<int>(std::min(t1.size(), t2.size())));
    int prefix_size = 0;
    for (int i = 0; i < limit; i++) {
      if (t1[i] == t2[i]) {
        prefix_size++;
      } else {
        break;
      }
    }
    return prefix_size ? param_.prefix_scaling_factor * prefix_size *
                             (1.0 - jaro_similarity)
                       : 0.0;
  }

  JaroWinklerParam param_;  // Our parameters.
};

absl::Status ValidateJaroWinklerParams(
    std::optional<double> prefix_scaling_factor,
    std::optional<double> prefix_boost_threshold) {
  if (prefix_scaling_factor.has_value()) {
    double factor = *prefix_scaling_factor;
    if (std::isnan(factor) || factor < 0.0 || factor > 0.25) {
      return absl::OutOfRangeError(
          "The prefix_scaling_factor must be in [0, 0.25] range");
    }
  }
  if (prefix_boost_threshold.has_value()) {
    double threshold = *prefix_boost_threshold;
    if (std::isnan(threshold) || threshold < 0.0 || threshold > 1.0) {
      return absl::OutOfRangeError(
          "The prefix_boost_threshold must be in [0, 1] range");
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<char32_t>> GetUtf8CodePoints(absl::string_view s) {
  std::vector<char32_t> result;
  int32_t offset = 0;
  while (offset < s.size()) {
    UChar32 character;
    U8_NEXT(s, offset, s.size(), character);
    if (character < 0) {
      return absl::OutOfRangeError("Invalid UTF8 string");
    }
    result.push_back(character);
  }

  return result;
}

}  // namespace

absl::StatusOr<double> JaroWinklerSimilarity(
    absl::string_view s0, absl::string_view s1,
    std::optional<double> prefix_scaling_factor,
    std::optional<double> prefix_boost_threshold) {
  GOOGLESQL_RETURN_IF_ERROR(
      ValidateJaroWinklerParams(prefix_scaling_factor, prefix_boost_threshold));
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<char32_t> code_points0, GetUtf8CodePoints(s0));
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<char32_t> code_points1, GetUtf8CodePoints(s1));

  JaroWinklerParam param;
  if (prefix_scaling_factor.has_value()) {
    param.prefix_scaling_factor = *prefix_scaling_factor;
  }
  if (prefix_boost_threshold.has_value()) {
    param.prefix_correction_threshold = *prefix_boost_threshold;
  }
  JaroWinkler<std::vector<char32_t>> jw(param);
  return jw.Similarity(code_points0, code_points1);
}

absl::StatusOr<double> JaroWinklerSimilarityBytes(
    absl::string_view s0, absl::string_view s1,
    std::optional<double> prefix_scaling_factor,
    std::optional<double> prefix_boost_threshold) {
  GOOGLESQL_RETURN_IF_ERROR(
      ValidateJaroWinklerParams(prefix_scaling_factor, prefix_boost_threshold));
  JaroWinklerParam param;
  if (prefix_scaling_factor.has_value()) {
    param.prefix_scaling_factor = *prefix_scaling_factor;
  }
  if (prefix_boost_threshold.has_value()) {
    param.prefix_correction_threshold = *prefix_boost_threshold;
  }
  JaroWinkler<absl::string_view> jw(param);
  return jw.Similarity(s0, s1);
}

}  // namespace functions
}  // namespace googlesql
