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

#ifndef GOOGLESQL_PUBLIC_FUNCTIONS_BITCAST_H_
#define GOOGLESQL_PUBLIC_FUNCTIONS_BITCAST_H_

// This file implements basic bitcast operations. The following functions
// are defined:
//
//   bool BitCast(TIN in1, TOUT *out, absl::Status* error);
//
// Here TIN and TOUT can be one of the following types: int32, int64, uint32,
// uint64, float, double.

#include <cstdint>
#include <string>
#include <type_traits>

#include "googlesql/public/functions/endianness.pb.h"
#include "absl/base/casts.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace functions {

template <typename TIN, typename TOUT,
          std::enable_if_t<!std::is_constructible_v<absl::string_view, TIN> &&
                               !std::is_same_v<std::decay_t<TOUT>, std::string>,
                           int> = 0>
bool BitCast(TIN in, TOUT* out, absl::Status* error) {
  *out = absl::bit_cast<TOUT>(in);
  return true;
}

bool BitCast(absl::string_view in, float* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(absl::string_view in, double* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(absl::string_view in, int32_t* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(absl::string_view in, uint32_t* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(absl::string_view in, int64_t* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(absl::string_view in, uint64_t* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

template <typename TOUT>
inline bool BitCastFromBytes(absl::string_view in, TOUT* out,
                             absl::Status* error,
                             Endianness endianness = Endianness::LITTLE) {
  return BitCast(in, out, error, endianness);
}

bool BitCast(float in, std::string* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(double in, std::string* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(int32_t in, std::string* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(uint32_t in, std::string* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(int64_t in, std::string* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

bool BitCast(uint64_t in, std::string* out, absl::Status* error,
             Endianness endianness = Endianness::LITTLE);

template <typename TIN>
inline bool BitCastToBytes(TIN in, std::string* out, absl::Status* error,
                           Endianness endianness = Endianness::LITTLE) {
  return BitCast(in, out, error, endianness);
}

}  // namespace functions
}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_FUNCTIONS_BITCAST_H_
