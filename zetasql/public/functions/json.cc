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

#include "zetasql/public/functions/json.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/common/errors.h"
#include "zetasql/common/int_ops_util.h"
#include "zetasql/public/functions/convert.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/functions/json_internal.h"
#include "zetasql/public/functions/to_json.h"
#include "zetasql/public/json_value.h"
#include "absl/base/optimization.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace {
using json_internal::JSONPathExtractor;
using json_internal::StrictJSONPathIterator;
using json_internal::StrictJSONPathToken;
using json_internal::ValidJSONPathIterator;
}  // namespace

JsonPathEvaluator::~JsonPathEvaluator() = default;

JsonPathEvaluator::JsonPathEvaluator(
    std::unique_ptr<ValidJSONPathIterator> itr,
    bool enable_special_character_escaping_in_values,
    bool enable_special_character_escaping_in_keys)
    : path_iterator_(std::move(itr)),
      enable_special_character_escaping_in_values_(
          enable_special_character_escaping_in_values),
      enable_special_character_escaping_in_keys_(
          enable_special_character_escaping_in_keys) {}

// static
absl::StatusOr<std::unique_ptr<JsonPathEvaluator>> JsonPathEvaluator::Create(
    absl::string_view json_path, bool sql_standard_mode,
    bool enable_special_character_escaping_in_values,
    bool enable_special_character_escaping_in_keys) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValidJSONPathIterator> itr,
                   ValidJSONPathIterator::Create(json_path, sql_standard_mode));
  // Scan tokens as json_path may not persist beyond this call.
  itr->Scan();
  return absl::WrapUnique(new JsonPathEvaluator(
      std::move(itr), enable_special_character_escaping_in_values,
      enable_special_character_escaping_in_keys));
}

absl::Status JsonPathEvaluator::Extract(
    absl::string_view json, std::string* value, bool* is_null,
    std::optional<std::function<void(absl::Status)>> issue_warning) const {
  JSONPathExtractor parser(json, path_iterator_.get());
  parser.set_special_character_escaping(
      enable_special_character_escaping_in_values_);
  parser.set_special_character_key_escaping(
      enable_special_character_escaping_in_keys_);
  parser.set_escaping_needed_callback(&escaping_needed_callback_);
  value->clear();
  parser.Extract(value, is_null, issue_warning);
  if (parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

std::optional<JSONValueConstRef> JsonPathEvaluator::Extract(
    JSONValueConstRef input) const {
  bool first_token = true;
  for (path_iterator_->Rewind(); !path_iterator_->End(); ++(*path_iterator_)) {
    const ValidJSONPathIterator::Token& token = *(*path_iterator_);

    if (first_token) {
      // The JSONPath "$.a[1].b" will result in the following list of tokens:
      // "", "a", "1", "b". The first token is always the empty token
      // corresponding to the whole JSON document, and we don't need to do
      // anything in that iteration. There can be other empty tokens after
      // the first one (empty keys are valid).
      first_token = false;
      continue;
    }

    if (input.IsObject()) {
      std::optional<JSONValueConstRef> optional_member =
          input.GetMemberIfExists(token);
      if (!optional_member.has_value()) {
        return std::nullopt;
      }
      input = optional_member.value();
    } else if (input.IsArray()) {
      int64_t index;
      if (!absl::SimpleAtoi(token, &index) || index < 0 ||
          index >= input.GetArraySize()) {
        return std::nullopt;
      }
      input = input.GetArrayElement(index);
    } else {
      // The path is not present in the JSON object.
      return std::nullopt;
    }
  }

  return input;
}

absl::Status JsonPathEvaluator::ExtractScalar(absl::string_view json,
                                              std::string* value,
                                              bool* is_null) const {
  json_internal::JSONPathExtractScalar scalar_parser(json,
                                                     path_iterator_.get());
  value->clear();
  scalar_parser.Extract(value, is_null);
  if (scalar_parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

std::optional<std::string> JsonPathEvaluator::ExtractScalar(
    JSONValueConstRef input) const {
  std::optional<JSONValueConstRef> optional_json = Extract(input);
  if (!optional_json.has_value() || optional_json->IsNull() ||
      optional_json->IsObject() || optional_json->IsArray()) {
    return std::nullopt;
  }

  if (optional_json->IsString()) {
    // ToString() adds extra quotes and escapes special characters,
    // which we don't want.
    return optional_json->GetString();
  }

  return optional_json->ToString();
}

absl::Status JsonPathEvaluator::ExtractArray(
    absl::string_view json, std::vector<std::string>* value, bool* is_null,
    std::optional<std::function<void(absl::Status)>> issue_warning) const {
  json_internal::JSONPathArrayExtractor array_parser(json,
                                                     path_iterator_.get());
  array_parser.set_special_character_escaping(
      enable_special_character_escaping_in_values_);
  array_parser.set_special_character_key_escaping(
      enable_special_character_escaping_in_keys_);
  value->clear();
  array_parser.ExtractArray(value, is_null, issue_warning);
  if (array_parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

std::optional<std::vector<JSONValueConstRef>> JsonPathEvaluator::ExtractArray(
    JSONValueConstRef input) const {
  std::optional<JSONValueConstRef> json = Extract(input);
  if (!json.has_value() || json->IsNull() || !json->IsArray()) {
    return std::nullopt;
  }

  return json->GetArrayElements();
}

absl::Status JsonPathEvaluator::ExtractStringArray(
    absl::string_view json, std::vector<std::optional<std::string>>* value,
    bool* is_null) const {
  json_internal::JSONPathStringArrayExtractor array_parser(
      json, path_iterator_.get());
  value->clear();
  array_parser.ExtractStringArray(value, is_null);
  if (array_parser.StoppedDueToStackSpace()) {
    return MakeEvalError() << "JSON parsing failed due to deeply nested "
                              "array/struct. Maximum nesting depth is "
                           << JSONPathExtractor::kMaxParsingDepth;
  }
  return absl::OkStatus();
}

std::optional<std::vector<std::optional<std::string>>>
JsonPathEvaluator::ExtractStringArray(JSONValueConstRef input) const {
  std::optional<std::vector<JSONValueConstRef>> json_array =
      ExtractArray(input);
  if (!json_array.has_value()) {
    return std::nullopt;
  }

  std::vector<std::optional<std::string>> results;
  results.reserve(json_array->size());
  for (JSONValueConstRef element : *json_array) {
    if (element.IsArray() || element.IsObject()) {
      return std::nullopt;
    }

    if (element.IsNull()) {
      results.push_back(std::nullopt);
    } else if (element.IsString()) {
      // ToString() adds extra quotes and escapes special characters,
      // which we don't want.
      results.push_back(element.GetString());
    } else {
      results.push_back(element.ToString());
    }
  }
  return results;
}

std::string ConvertJSONPathTokenToSqlStandardMode(
    absl::string_view json_path_token) {
  // See json_internal.cc for list of characters that don't need escaping.
  static const LazyRE2 kSpecialCharsPattern = {R"([^\p{L}\p{N}\d_\-\:\s])"};
  static const LazyRE2 kDoubleQuotesPattern = {R"(")"};

  if (!RE2::PartialMatch(json_path_token, *kSpecialCharsPattern)) {
    // No special characters. Can be field access or array element access.
    // Note that '$[0]' is equivalent to '$.0'.
    return std::string(json_path_token);
  } else if (absl::StrContains(json_path_token, "\"")) {
    // We need to escape double quotes in the json_path_token because the SQL
    // standard mode use them to wrap around json_path_token with special
    // characters.
    std::string escaped(json_path_token);
    // Two backslashes are needed in the replacement string because \<digit>
    // is used for group matching.
    RE2::GlobalReplace(&escaped, *kDoubleQuotesPattern, R"(\\")");
    return absl::StrCat("\"", escaped, "\"");
  } else {
    // Special characters but no double quotes.
    return absl::StrCat("\"", json_path_token, "\"");
  }
}

absl::StatusOr<std::string> ConvertJSONPathToSqlStandardMode(
    absl::string_view json_path) {
  // See json_internal.cc for list of characters that don't need escaping.
  static const LazyRE2 kSpecialCharsPattern = {R"([^\p{L}\p{N}\d_\-\:\s])"};
  static const LazyRE2 kDoubleQuotesPattern = {R"(")"};

  ZETASQL_ASSIGN_OR_RETURN(auto iterator, json_internal::ValidJSONPathIterator::Create(
                                      json_path, /*sql_standard_mode=*/false));

  std::string new_json_path = "$";

  // First token is empty.
  ++(*iterator);

  for (; !iterator->End(); ++(*iterator)) {
    // Token is unescaped.
    absl::string_view token = **iterator;
    if (token.empty()) {
      // Special case: empty token needs to be escaped.
      absl::StrAppend(&new_json_path, ".\"\"");
    } else if (!RE2::PartialMatch(token, *kSpecialCharsPattern)) {
      // No special characters. Can be field access or array element access.
      // Note that '$[0]' is equivalent to '$.0'.
      absl::StrAppend(&new_json_path, ".", token);
    } else if (absl::StrContains(token, "\"")) {
      // We need to escape double quotes in the token because the SQL standard
      // mode use them to wrap around token with special characters.
      std::string escaped(token);
      // Two backslashes are needed in the replacement string because \<digit>
      // is used for group matching.
      RE2::GlobalReplace(&escaped, *kDoubleQuotesPattern, R"(\\")");
      absl::StrAppend(&new_json_path, ".\"", escaped, "\"");
    } else {
      // Special characters but no double quotes.
      absl::StrAppend(&new_json_path, ".\"", token, "\"");
    }
  }

  ZETASQL_RET_CHECK_OK(json_internal::IsValidJSONPath(new_json_path,
                                              /*sql_standard_mode=*/true));

  return new_json_path;
}

absl::StatusOr<std::string> MergeJSONPathsIntoSqlStandardMode(
    absl::Span<const std::string> json_paths) {
  if (json_paths.empty()) {
    return absl::OutOfRangeError("Empty JSONPaths.");
  }

  std::string merged_json_path = "$";

  for (absl::string_view json_path : json_paths) {
    if (json_internal::IsValidJSONPath(json_path, /*sql_standard_mode=*/true)
            .ok()) {
      // Remove the "$" prefix.
      absl::StrAppend(&merged_json_path, json_path.substr(1));
    } else {
      // Convert to SQL standard mode first.
      ZETASQL_ASSIGN_OR_RETURN(std::string sql_standard_json_path,
                       ConvertJSONPathToSqlStandardMode(json_path));

      absl::StrAppend(&merged_json_path, sql_standard_json_path.substr(1));
    }
  }

  ZETASQL_RET_CHECK_OK(json_internal::IsValidJSONPath(merged_json_path,
                                              /*sql_standard_mode=*/true));

  return merged_json_path;
}

absl::StatusOr<int64_t> ConvertJsonToInt64(JSONValueConstRef input) {
  if (input.IsInt64()) {
    return input.GetInt64();
  }

  // There must be no fractional part if provided double as input
  if (input.IsDouble()) {
    double input_as_double = input.GetDouble();
    int64_t output;
    if (LossLessConvertDoubleToInt64(input_as_double, &output)) {
      return output;
    }
    return MakeEvalError() << "The provided JSON number: " << input_as_double
                           << " cannot be converted to an integer";
  }

  return MakeEvalError() << "The provided JSON input is not an integer";
}

absl::StatusOr<bool> ConvertJsonToBool(JSONValueConstRef input) {
  if (!input.IsBoolean()) {
    return MakeEvalError() << "The provided JSON input is not a boolean";
  }
  return input.GetBoolean();
}

absl::StatusOr<std::string> ConvertJsonToString(JSONValueConstRef input) {
  if (!input.IsString()) {
    return MakeEvalError() << "The provided JSON input is not a string";
  }
  return input.GetString();
}

absl::StatusOr<double> ConvertJsonToDouble(JSONValueConstRef input,
                                           WideNumberMode wide_number_mode,
                                           ProductMode product_mode) {
  if (input.IsDouble()) {
    return input.GetDouble();
  }

  if (input.IsInt64()) {
    int64_t value = input.GetInt64();
    if (wide_number_mode == functions::WideNumberMode::kExact &&
        (value < kMinLosslessInt64ValueForJson ||
         value > kMaxLosslessInt64ValueForJson)) {
      std::string function_name =
          product_mode == PRODUCT_EXTERNAL ? "FLOAT64" : "DOUBLE";
      return MakeEvalError()
             << "JSON number: " << value << " cannot be converted to "
             << function_name << " without loss of precision";
    }
    return double{static_cast<double>(value)};
  }
  if (input.IsUInt64()) {
    uint64_t value = input.GetUInt64();
    if (wide_number_mode == functions::WideNumberMode::kExact &&
        value > static_cast<uint64_t>(kMaxLosslessInt64ValueForJson)) {
      std::string function_name =
          product_mode == PRODUCT_EXTERNAL ? "FLOAT64" : "DOUBLE";
      return MakeEvalError()
             << "JSON number: " << value << " cannot be converted to "
             << function_name << " without loss of precision";
    }
    return double{static_cast<double>(value)};
  }

  return MakeEvalError() << "The provided JSON input is not a number";
}

absl::StatusOr<std::string> GetJsonType(JSONValueConstRef input) {
  if (input.IsNumber()) {
    return "number";
  }
  if (input.IsString()) {
    return "string";
  }
  if (input.IsBoolean()) {
    return "boolean";
  }
  if (input.IsObject()) {
    return "object";
  }
  if (input.IsArray()) {
    return "array";
  }
  if (input.IsNull()) {
    return "null";
  }
  ZETASQL_RET_CHECK_FAIL()
      << "Invalid JSON value that doesn't belong to any known JSON type";
}

template <typename FromType, typename ToType>
static std::optional<ToType> ConvertNumericToNumeric(FromType val) {
  absl::Status status;
  ToType out;
  if (!Convert(val, &out, &status)) {
    return std::nullopt;
  }
  return out;
}

template <typename Type>
static std::optional<std::string> ConvertNumericToString(Type val) {
  absl::Status status;
  std::string out;
  if (!NumericToString(val, &out, &status, /*canonicalize_zero=*/true)) {
    return std::nullopt;
  }
  return out;
}

template <typename Type>
static std::optional<Type> ConvertStringToNumeric(absl::string_view val) {
  absl::Status status;
  Type out;
  if (!StringToNumeric(val, &out, &status)) {
    return std::nullopt;
  }
  return out;
}

absl::StatusOr<std::optional<bool>> LaxConvertJsonToBool(
    JSONValueConstRef input) {
  if (input.IsBoolean()) {
    return input.GetBoolean();
  } else if (input.IsInt64()) {
    return input.GetInt64() != 0;
  } else if (input.IsUInt64()) {
    return input.GetUInt64() != 0;
  } else if (input.IsDouble()) {
    return input.GetDouble() != 0;
  } else if (input.IsString()) {
    return ConvertStringToNumeric<bool>(input.GetString());
  }
  return std::nullopt;
}

absl::StatusOr<std::optional<int64_t>> LaxConvertJsonToInt64(
    JSONValueConstRef input) {
  if (input.IsBoolean()) {
    return input.GetBoolean() ? 1 : 0;
  } else if (input.IsInt64()) {
    return input.GetInt64();
  } else if (input.IsUInt64()) {
    return ConvertNumericToNumeric<uint64_t, int64_t>(input.GetUInt64());
  } else if (input.IsDouble()) {
    return ConvertNumericToNumeric<double, int64_t>(input.GetDouble());
  } else if (input.IsString()) {
    BigNumericValue big_numeric_value;
    absl::Status status;
    int64_t out;
    if (!StringToNumeric(input.GetString(), &big_numeric_value, &status) ||
        !Convert(big_numeric_value, &out, &status)) {
      return std::nullopt;
    }
    return out;
  }
  return std::nullopt;
}

absl::StatusOr<std::optional<double>> LaxConvertJsonToFloat64(
    JSONValueConstRef input) {
  if (input.IsInt64()) {
    return ConvertNumericToNumeric<int64_t, double>(input.GetInt64());
  } else if (input.IsUInt64()) {
    return ConvertNumericToNumeric<uint64_t, double>(input.GetUInt64());
  } else if (input.IsDouble()) {
    return input.GetDouble();
  } else if (input.IsString()) {
    return ConvertStringToNumeric<double>(input.GetString());
  }
  return std::nullopt;
}

absl::StatusOr<std::optional<std::string>> LaxConvertJsonToString(
    JSONValueConstRef input) {
  if (input.IsBoolean()) {
    return ConvertNumericToString<bool>(input.GetBoolean());
  } else if (input.IsInt64()) {
    return ConvertNumericToString<int64_t>(input.GetInt64());
  } else if (input.IsUInt64()) {
    return ConvertNumericToString<uint64_t>(input.GetUInt64());
  } else if (input.IsDouble()) {
    return ConvertNumericToString<double>(input.GetDouble());
  } else if (input.IsString()) {
    return input.GetString();
  }
  return std::nullopt;
}

absl::StatusOr<JSONValue> JsonArray(absl::Span<const Value> args,
                                    const LanguageOptions& language_options,
                                    bool canonicalize_zero) {
  JSONValue json;
  JSONValueRef json_ref = json.GetRef();
  json_ref.SetToEmptyArray();
  if (args.empty()) {
    return json;
  }

  json_ref.GetArrayElement(args.size() - 1);
  for (size_t i = 0; i < args.size(); ++i) {
    const Value& arg = args[i];
    JSONValueRef ref = json_ref.GetArrayElement(i);
    ZETASQL_ASSIGN_OR_RETURN(JSONValue value,
                     ToJson(arg, /*stringify_wide_numbers=*/false,
                            language_options, canonicalize_zero));
    ref.Set(std::move(value));
  }
  return json;
}

absl::StatusOr<bool> JsonObjectBuilder::Add(absl::string_view key,
                                            const Value& value) {
  if (!keys_set_.insert(key).second) {
    // Duplicate key, simply return.
    return false;
  }
  JSONValueRef ref = result_.GetRef().GetMember(key);
  ZETASQL_ASSIGN_OR_RETURN(JSONValue json_value,
                   ToJson(value, /*stringify_wide_numbers=*/false, options_,
                          canonicalize_zero_));
  ref.Set(std::move(json_value));
  return true;
}

JSONValue JsonObjectBuilder::Build() {
  JSONValue result = std::move(result_);
  Reset();
  return result;
}

void JsonObjectBuilder::Reset() {
  result_ = JSONValue();
  result_.GetRef().SetToEmptyObject();
  keys_set_.clear();
}

absl::StatusOr<JSONValue> JsonObject(absl::Span<const absl::string_view> keys,
                                     absl::Span<const Value*> values,
                                     JsonObjectBuilder& builder) {
  if (keys.size() != values.size()) {
    return MakeEvalError() << "The number of keys and values must match";
  }

  for (size_t i = 0; i < keys.size(); ++i) {
    auto status = builder.Add(keys[i], *values[i]).status();
    if (!status.ok()) {
      builder.Reset();
      return status;
    }
  }
  return builder.Build();
}

absl::StatusOr<bool> JsonRemove(JSONValueRef input,
                                StrictJSONPathIterator& path_iterator) {
  path_iterator.Rewind();

  // First token is always empty.
  ++path_iterator;

  if (path_iterator.End()) {
    // `path` is '$'
    return MakeEvalError() << "The JSONPath cannot be '$'";
  }

  for (; !path_iterator.End(); ++path_iterator) {
    const StrictJSONPathToken& token = *path_iterator;

    if (const std::string* key = token.MaybeGetObjectKey();
        input.IsObject() && key != nullptr) {
      if (path_iterator.NoSuffixToken()) {
        auto success = input.RemoveMember(*key);
        ZETASQL_RET_CHECK_OK(success.status());
        return *success;
      }
      if (std::optional<JSONValueRef> member = input.GetMemberIfExists(*key);
          member.has_value()) {
        input = *member;
        continue;
      }
    } else if (const int64_t* index = token.MaybeGetArrayIndex();
               input.IsArray() && index != nullptr) {
      if (path_iterator.NoSuffixToken()) {
        auto success = input.RemoveArrayElement(*index);
        ZETASQL_RET_CHECK_OK(success.status());
        return *success;
      }
      if (*index >= 0 && *index < input.GetArraySize()) {
        input = input.GetArrayElement(static_cast<size_t>(*index));
        continue;
      }
    }
    // Nonexistent member, invalid array index or type mismatch. Do nothing and
    // exit.
    return false;
  }

  // This should never be reached.
  ZETASQL_RET_CHECK_FAIL();
}

namespace {

// How to add elements to the array.
enum class AddType {
  // Insert the element(s) at the index in the array.
  kInsert = 0,
  // Append the element(s) at the end of the array.
  kAppend,
};

absl::Status JsonAddArrayElement(JSONValueRef input,
                                 StrictJSONPathIterator& path_iterator,
                                 const Value& value,
                                 const LanguageOptions& language_options,
                                 bool canonicalize_zero, bool add_each_element,
                                 AddType add_type) {
  path_iterator.Rewind();
  // First token is always empty.
  ++path_iterator;

  // Only contains a value for kInsert.
  std::optional<int64_t> index_to_insert;

  for (; !path_iterator.End(); ++path_iterator) {
    const StrictJSONPathToken& token = *path_iterator;
    if (add_type == AddType::kInsert && path_iterator.NoSuffixToken()) {
      // This is the last token. It has to be an array index for inserts.
      if (const int64_t* index = token.MaybeGetArrayIndex(); index != nullptr) {
        index_to_insert = *index;
      }
      // For inserts, the last token indicates the position in the array to
      // insert the value, so do not go down the JSON tree. The next iteration
      // will exit the loop.
      continue;
    }

    if (const std::string* key = token.MaybeGetObjectKey();
        input.IsObject() && key != nullptr) {
      if (std::optional<JSONValueRef> member = input.GetMemberIfExists(*key);
          member.has_value()) {
        input = *member;
        continue;
      }
    } else if (const int64_t* index = token.MaybeGetArrayIndex();
               input.IsArray() && index != nullptr) {
      if (*index >= 0 && *index < input.GetArraySize()) {
        input = input.GetArrayElement(*index);
        continue;
      }
    }
    // Inexistent member, invalid array index or type mismatch. Do nothing and
    // exit.
    return absl::OkStatus();
  }

  ZETASQL_RET_CHECK(path_iterator.End());

  if (!input.IsArray()) {
    // Do nothing.
    return absl::OkStatus();
  }

  if (add_type == AddType::kInsert && !index_to_insert.has_value()) {
    // Do nothing in that case.
    return absl::OkStatus();
  }

  // If the value to be inserted in an array and add_each_element is true, the
  // function adds each element separately instead of a single JSON array value.
  ZETASQL_RET_CHECK(value.is_valid());
  if (add_each_element && value.type()->IsArray()) {
    std::vector<JSONValue> elements;
    elements.reserve(value.num_elements());
    for (const Value& element : value.elements()) {
      ZETASQL_ASSIGN_OR_RETURN(
          JSONValue e,
          functions::ToJson(element, /*stringify_wide_numbers=*/false,
                            language_options, canonicalize_zero));
      elements.push_back(std::move(e));
    }
    if (add_type == AddType::kInsert) {
      ZETASQL_RET_CHECK_OK(
          input.InsertArrayElements(std::move(elements), *index_to_insert));
    } else {
      ZETASQL_RET_CHECK_OK(input.AppendArrayElements(std::move(elements)));
    }
  } else {
    ZETASQL_ASSIGN_OR_RETURN(JSONValue e,
                     functions::ToJson(value, /*stringify_wide_numbers=*/false,
                                       language_options, canonicalize_zero));
    if (add_type == AddType::kInsert) {
      ZETASQL_RET_CHECK_OK(input.InsertArrayElement(std::move(e), *index_to_insert));
    } else {
      ZETASQL_RET_CHECK_OK(input.AppendArrayElement(std::move(e)));
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status JsonInsertArrayElement(JSONValueRef input,
                                    StrictJSONPathIterator& path_iterator,
                                    const Value& value,
                                    const LanguageOptions& language_options,
                                    bool canonicalize_zero,
                                    bool insert_each_element) {
  return JsonAddArrayElement(input, path_iterator, value, language_options,
                             canonicalize_zero, insert_each_element,
                             AddType::kInsert);
}

absl::Status JsonAppendArrayElement(JSONValueRef input,
                                    StrictJSONPathIterator& path_iterator,
                                    const Value& value,
                                    const LanguageOptions& language_options,
                                    bool canonicalize_zero,
                                    bool append_each_element) {
  return JsonAddArrayElement(input, path_iterator, value, language_options,
                             canonicalize_zero, append_each_element,
                             AddType::kAppend);
}

absl::Status JsonSet(JSONValueRef input, StrictJSONPathIterator& path_iterator,
                     const Value& value,
                     const LanguageOptions& language_options,
                     bool canonicalize_zero) {
  // Ensure we always start from the beginning of the path.
  path_iterator.Rewind();
  // First token is always empty (no-op).
  ++path_iterator;

  // The input path is '$'. This implies that we replace the entire value.
  if (path_iterator.End()) {
    ZETASQL_ASSIGN_OR_RETURN(JSONValue converted_value,
                     functions::ToJson(value, /*stringify_wide_numbers=*/false,
                                       language_options, canonicalize_zero));
    input.Set(std::move(converted_value));
    return absl::OkStatus();
  }

  // Walk down the JSON tree.
  //
  // Cases for each token in path:
  // 1) If token in path exists in current JSON element, continue processing
  //    the JSON subtree with next the path token.
  // 2) If the token in path does not exist or has a value of JSON 'null' in
  //    JSON element, insert it and continue inserting the rest of the path.
  //   (Recursive creation)
  // 3) If there is a type mismatch, this is not a valid Set operation so
  //    ignore operation and return early.
  for (; !path_iterator.End(); ++path_iterator) {
    const StrictJSONPathToken& token = *path_iterator;
    if (auto* key = token.MaybeGetObjectKey();
        (input.IsObject() || input.IsNull()) && key != nullptr) {
      input = input.GetMember(*key);
      continue;
    } else if (auto* index = token.MaybeGetArrayIndex();
               (input.IsArray() || input.IsNull()) && index != nullptr) {
      // Negative indexes should have thrown an error during path validation.
      if (ABSL_PREDICT_FALSE(*index < 0)) {
        return MakeEvalError()
               << "Negative indexes are not supported in JSON paths.";
      }
      // If `index` is larger than the length of the JSON array, it
      // is automatically resized with null elements.
      input = input.GetArrayElement(*index);
      continue;
    }
    // Type mismatch, ignore operation and return early.
    return absl::OkStatus();
  }

  ZETASQL_ASSIGN_OR_RETURN(JSONValue converted_value,
                   functions::ToJson(value, /*stringify_wide_numbers=*/false,
                                     language_options, canonicalize_zero));
  input.Set(std::move(converted_value));
  return absl::OkStatus();
}

}  // namespace functions
}  // namespace zetasql
