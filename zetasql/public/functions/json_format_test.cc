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

// Tests for JSON formatting functions.

#include "zetasql/public/functions/json_format.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/map_util.h"

namespace zetasql {
namespace functions {
namespace {

using ::zetasql_base::testing::StatusIs;

TEST(JsonFormatTest, Compliance) {
  const std::vector<FunctionTestCall> tests = GetFunctionTestsToJsonString();

  for (const FunctionTestCall& test : tests) {
    if (std::any_of(test.params.params().begin(), test.params.params().end(),
                    [](const Value& param) { return param.is_null(); })) {
      continue;
    }
    const Value input_value = test.params.param(0);
    const bool pretty_print = test.params.params().size() == 2
                                  ? test.params.param(1).bool_value()
                                  : false;
    SCOPED_TRACE(absl::Substitute("$0('$1', '$2')", test.function_name,
                                  input_value.ShortDebugString(),
                                  pretty_print));

    JsonPrettyPrinter pretty_printer(pretty_print, PRODUCT_INTERNAL);
    JSONParsingOptions json_parsing_options =
        JSONParsingOptions{.canonicalize_zero = true};
    if (zetasql_base::ContainsKey(test.params.required_features(),
                         FEATURE_JSON_STRICT_NUMBER_PARSING)) {
      json_parsing_options.wide_number_mode =
          JSONParsingOptions::WideNumberMode::kExact;
    }
    std::string actual_output;
    auto actual_status = JsonFromValue(input_value, &pretty_printer,
                                       &actual_output, json_parsing_options);

    if (test.params.status().ok()) {
      ZETASQL_ASSERT_OK(actual_status);
      EXPECT_EQ(test.params.result().string_value(), actual_output);

      actual_output = "dummy.prefix";
      ZETASQL_ASSERT_OK(JsonFromValue(input_value, &pretty_printer, &actual_output,
                              json_parsing_options));
      EXPECT_EQ("dummy.prefix" + test.params.result().string_value(),
                actual_output);
    } else {
      EXPECT_EQ(test.params.status().code(), actual_status.code());
    }
  }
}

TEST(JsonFormatTest, LargeOutput) {
  // Output larger than pretty_printer.max_json_output_size_bytes() is
  // disallowed by JsonFromValue. The output size includes quotes around
  // strings.
  JsonPrettyPrinter pretty_printer(/*pretty_print=*/false, PRODUCT_INTERNAL);
  EXPECT_EQ(kDefaultMaxJsonStringSizeBytes,
            pretty_printer.max_json_output_size_bytes());

  const int64_t max_json_output_size = 10;
  pretty_printer.set_max_json_output_size_bytes(max_json_output_size);
  EXPECT_EQ(max_json_output_size, pretty_printer.max_json_output_size_bytes());

  {
    std::string tmp;
    ZETASQL_EXPECT_OK(JsonFromValue(
        values::String(std::string(max_json_output_size - 2, '.')),
        &pretty_printer, &tmp));
  }

  {
    std::string tmp;
    EXPECT_THAT(JsonFromValue(
                    values::String(std::string(max_json_output_size - 1, '.')),
                    &pretty_printer, &tmp),
                StatusIs(absl::StatusCode::kOutOfRange));
  }

  {
    std::string tmp;
    EXPECT_THAT(
        JsonFromValue(
            values::Array(
                types::StringArrayType(),
                {values::String(std::string(max_json_output_size / 2, '.')),
                 values::String(std::string(max_json_output_size / 2, '.'))}),
            &pretty_printer, &tmp),
        StatusIs(absl::StatusCode::kOutOfRange));
  }

  {
    std::string tmp = "....";
    EXPECT_THAT(JsonFromValue(values::String("foobar"), &pretty_printer, &tmp),
                StatusIs(absl::StatusCode::kOutOfRange));

    pretty_printer.set_max_json_output_size_bytes(max_json_output_size * 2);
    tmp = "....";
    ZETASQL_EXPECT_OK(JsonFromValue(values::String("foobar"), &pretty_printer, &tmp));
  }
}

void CanonicalizeZeroLegacyTest(Value input_value,
                                const std::string& expected_result_value) {
  JsonPrettyPrinter pretty_printer(/*pretty_print=*/false, PRODUCT_INTERNAL);
  JSONParsingOptions json_parsing_options =
      JSONParsingOptions{.canonicalize_zero = false};
  std::string actual_output;
  auto actual_status = JsonFromValue(input_value, &pretty_printer,
                                     &actual_output, json_parsing_options);

  ZETASQL_ASSERT_OK(actual_status);
  actual_output = "dummy.prefix";
  ZETASQL_ASSERT_OK(JsonFromValue(input_value, &pretty_printer, &actual_output,
                          json_parsing_options));
  EXPECT_EQ("dummy.prefix" + expected_result_value, actual_output);
}

TEST(JsonFormatTest, CanonicalizeZeroLegacyFloat0Test) {
  CanonicalizeZeroLegacyTest(values::Float(-0.0), "-0");
}

TEST(JsonFormatTest, CanonicalizeZeroLegacyFloatNonZeroTest) {
  CanonicalizeZeroLegacyTest(values::Float(5), "5");
}

TEST(JsonFormatTest, CanonicalizeZeroLegacyDouble0Test) {
  CanonicalizeZeroLegacyTest(values::Double(-0.0), "-0");
}

TEST(JsonFormatTest, CanonicalizeZeroLegacyDoubleNonZeroTest) {
  CanonicalizeZeroLegacyTest(values::Double(5), "5");
}

}  // namespace
}  // namespace functions
}  // namespace zetasql
