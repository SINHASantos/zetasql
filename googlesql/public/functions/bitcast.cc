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

// Common bitcast operations implementation.

#include "googlesql/public/functions/bitcast.h"

#include <cstdint>
#include <string>
#include <type_traits>

#include "absl/base/casts.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/endian.h"

namespace googlesql {
namespace functions {

template <typename TOUT,
          std::enable_if_t<std::is_same_v<std::decay_t<TOUT>, float> ||
                               std::is_same_v<std::decay_t<TOUT>, double> ||
                               std::is_same_v<std::decay_t<TOUT>, int32_t> ||
                               std::is_same_v<std::decay_t<TOUT>, uint32_t> ||
                               std::is_same_v<std::decay_t<TOUT>, int64_t> ||
                               std::is_same_v<std::decay_t<TOUT>, uint64_t>,
                           int> = 0>
static bool BitCastFromBytesImpl(absl::string_view in, TOUT* out,
                                 absl::Status* error, Endianness endianness) {
  if (in.size() != sizeof(TOUT)) {
    if (error != nullptr) {
      absl::string_view type_name = "UNKNOWN";
      if constexpr (std::is_same_v<std::decay_t<TOUT>, float>) {
        type_name = "FLOAT";
      } else if constexpr (std::is_same_v<std::decay_t<TOUT>, double>) {
        type_name = "DOUBLE";
      } else if constexpr (std::is_same_v<std::decay_t<TOUT>, int32_t>) {
        type_name = "INT32";
      } else if constexpr (std::is_same_v<std::decay_t<TOUT>, uint32_t>) {
        type_name = "UINT32";
      } else if constexpr (std::is_same_v<std::decay_t<TOUT>, int64_t>) {
        type_name = "INT64";
      } else if constexpr (std::is_same_v<std::decay_t<TOUT>, uint64_t>) {
        type_name = "UINT64";
      }
      *error = absl::OutOfRangeError(
          absl::StrCat("Cannot bit_cast from BYTES of length ", in.size(),
                       " to ", type_name));
    }
    return false;
  }
  if (endianness == Endianness::BIG) {
    if constexpr (sizeof(TOUT) == 4) {
      uint32_t val = googlesql_base::BigEndian::Load32(in.data());
      *out = absl::bit_cast<TOUT>(val);
    } else {
      uint64_t val = googlesql_base::BigEndian::Load64(in.data());
      *out = absl::bit_cast<TOUT>(val);
    }
  } else {
    if constexpr (sizeof(TOUT) == 4) {
      uint32_t val = googlesql_base::LittleEndian::Load32(in.data());
      *out = absl::bit_cast<TOUT>(val);
    } else {
      uint64_t val = googlesql_base::LittleEndian::Load64(in.data());
      *out = absl::bit_cast<TOUT>(val);
    }
  }
  return true;
}

bool BitCast(absl::string_view in, float* out, absl::Status* error,
             Endianness endianness) {
  return BitCastFromBytesImpl(in, out, error, endianness);
}

bool BitCast(absl::string_view in, double* out, absl::Status* error,
             Endianness endianness) {
  return BitCastFromBytesImpl(in, out, error, endianness);
}

bool BitCast(absl::string_view in, int32_t* out, absl::Status* error,
             Endianness endianness) {
  return BitCastFromBytesImpl(in, out, error, endianness);
}

bool BitCast(absl::string_view in, uint32_t* out, absl::Status* error,
             Endianness endianness) {
  return BitCastFromBytesImpl(in, out, error, endianness);
}

bool BitCast(absl::string_view in, int64_t* out, absl::Status* error,
             Endianness endianness) {
  return BitCastFromBytesImpl(in, out, error, endianness);
}

bool BitCast(absl::string_view in, uint64_t* out, absl::Status* error,
             Endianness endianness) {
  return BitCastFromBytesImpl(in, out, error, endianness);
}

template <typename TIN,
          std::enable_if_t<std::is_same_v<std::decay_t<TIN>, float> ||
                               std::is_same_v<std::decay_t<TIN>, double> ||
                               std::is_same_v<std::decay_t<TIN>, int32_t> ||
                               std::is_same_v<std::decay_t<TIN>, uint32_t> ||
                               std::is_same_v<std::decay_t<TIN>, int64_t> ||
                               std::is_same_v<std::decay_t<TIN>, uint64_t>,
                           int> = 0>
static bool BitCastToBytesImpl(TIN in, std::string* out,
                               absl::Status* /*error*/, Endianness endianness) {
  out->resize(sizeof(TIN));
  if (endianness == Endianness::BIG) {
    if constexpr (sizeof(TIN) == 4) {
      uint32_t val = absl::bit_cast<uint32_t>(in);
      googlesql_base::BigEndian::Store32(out->data(), val);
    } else {
      uint64_t val = absl::bit_cast<uint64_t>(in);
      googlesql_base::BigEndian::Store64(out->data(), val);
    }
  } else {
    if constexpr (sizeof(TIN) == 4) {
      uint32_t val = absl::bit_cast<uint32_t>(in);
      googlesql_base::LittleEndian::Store32(out->data(), val);
    } else {
      uint64_t val = absl::bit_cast<uint64_t>(in);
      googlesql_base::LittleEndian::Store64(out->data(), val);
    }
  }
  return true;
}

bool BitCast(float in, std::string* out, absl::Status* error,
             Endianness endianness) {
  return BitCastToBytesImpl(in, out, error, endianness);
}

bool BitCast(double in, std::string* out, absl::Status* error,
             Endianness endianness) {
  return BitCastToBytesImpl(in, out, error, endianness);
}

bool BitCast(int32_t in, std::string* out, absl::Status* error,
             Endianness endianness) {
  return BitCastToBytesImpl(in, out, error, endianness);
}

bool BitCast(uint32_t in, std::string* out, absl::Status* error,
             Endianness endianness) {
  return BitCastToBytesImpl(in, out, error, endianness);
}

bool BitCast(int64_t in, std::string* out, absl::Status* error,
             Endianness endianness) {
  return BitCastToBytesImpl(in, out, error, endianness);
}

bool BitCast(uint64_t in, std::string* out, absl::Status* error,
             Endianness endianness) {
  return BitCastToBytesImpl(in, out, error, endianness);
}

}  // namespace functions
}  // namespace googlesql
