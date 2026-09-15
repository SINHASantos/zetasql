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

#include "googlesql/public/graph_dml_parse_util.h"

#include <memory>

#include "googlesql/parser/parser.h"
#include "googlesql/public/language_options.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/status_macros.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace {

// Helper function to parse a statement and return the AST root node.
absl::StatusOr<std::unique_ptr<ParserOutput>> ParseQuery(
    absl::string_view query) {
  ParserOptions parser_options{LanguageOptions()};

  std::unique_ptr<ParserOutput> output;
  GOOGLESQL_RETURN_IF_ERROR(ParseStatement(query, parser_options, &output));
  return output;
}

TEST(GraphDmlParseUtilTest, HasGraphDmlReturnsTrueForInsert) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ParserOutput> result1,
      ParseQuery("GRAPH Fin MATCH (a) INSERT (a)-[:Edge]->(:Node)"));
  EXPECT_THAT(HasGraphDml(result1->node()), absl_testing::IsOkAndHolds(true));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ParserOutput> result2,
      ParseQuery("GRAPH Fin MATCH (a) INSERT (a)-[:Edge]->(:Node) RETURN a"));
  EXPECT_THAT(HasGraphDml(result2->node()), absl_testing::IsOkAndHolds(true));
}

TEST(GraphDmlParseUtilTest, HasGraphDmlReturnsFalseForPureMatch) {
  // Identical query structure but without the terminal INSERT operator.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ParserOutput> result,
                       ParseQuery("GRAPH Fin MATCH (a) RETURN a"));
  EXPECT_THAT(HasGraphDml(result->node()), absl_testing::IsOkAndHolds(false));
}

TEST(GraphDmlParseUtilTest, HasGraphDmlReturnsTrueForSet) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ParserOutput> result,
                       ParseQuery("GRAPH Fin MATCH (a) SET a.prop = 1"));
  EXPECT_THAT(HasGraphDml(result->node()), absl_testing::IsOkAndHolds(true));
}

TEST(GraphDmlParseUtilTest, HasGraphDmlReturnsTrueForRemove) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ParserOutput> result,
                       ParseQuery("GRAPH Fin MATCH (a) REMOVE a.prop"));
  EXPECT_THAT(HasGraphDml(result->node()), absl_testing::IsOkAndHolds(true));
}

TEST(GraphDmlParseUtilTest, HasGraphDmlReturnsTrueForDelete) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ParserOutput> result1,
                       ParseQuery("GRAPH Fin MATCH (a) DELETE a"));
  EXPECT_THAT(HasGraphDml(result1->node()), absl_testing::IsOkAndHolds(true));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ParserOutput> result2,
                       ParseQuery("GRAPH Fin MATCH (a) NODETACH DELETE a"));
  EXPECT_THAT(HasGraphDml(result2->node()), absl_testing::IsOkAndHolds(true));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ParserOutput> result3,
                       ParseQuery("GRAPH Fin MATCH (a) DETACH DELETE a"));
  EXPECT_THAT(HasGraphDml(result3->node()), absl_testing::IsOkAndHolds(true));
}

}  // namespace
}  // namespace googlesql
