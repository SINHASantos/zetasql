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

#include "googlesql/common/measure_analysis_utils.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/common/internal_value.h"
#include "googlesql/base/testing/status_matchers.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/annotation/collation.h"
#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/testdata/sample_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"

namespace googlesql {
namespace {
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

class AddMeasureColumnsToTableTest : public ::testing::Test {
 protected:
  AddMeasureColumnsToTableTest() {
    options_.mutable_language()->EnableMaximumLanguageFeatures();
    options_.mutable_language()->EnableLanguageFeature(FEATURE_ENABLE_MEASURES);
    options_.mutable_language()->EnableLanguageFeature(FEATURE_DERIVED_MEASURE);

    base_table_ = std::make_unique<SimpleTable>(
        "BaseTable",
        std::vector<const Column*>{
            new SimpleColumn("BaseTable", "x", type_factory_.get_int64()),
            new SimpleColumn("BaseTable", "y", type_factory_.get_int64())},
        /*take_ownership=*/true);

    catalog_ =
        std::make_unique<SampleCatalog>(options_.language(), &type_factory_);
  }

  void ExpectColumnExpressionAST(const Table& table,
                                 const std::string& col_name,
                                 absl::string_view expected_ast) {
    const Column* col = table.FindColumnByName(col_name);
    ASSERT_NE(col, nullptr) << "Column " << col_name << " not found";
    ASSERT_TRUE(col->GetExpression().has_value())
        << "Column " << col_name << " has no expression";
    EXPECT_EQ(col->GetExpression()->GetResolvedExpression()->DebugString(),
              expected_ast);
  }

  void ExpectColumnRowIdentity(const Table& table, const std::string& col_name,
                               const std::vector<int>& expected_row_ids) {
    const Column* col = table.FindColumnByName(col_name);
    ASSERT_NE(col, nullptr) << "Column " << col_name << " not found";
    ASSERT_TRUE(col->GetExpression().has_value())
        << "Column " << col_name << " has no expression";
    auto row_ids = col->GetExpression()->RowIdentityColumns();
    ASSERT_TRUE(row_ids.has_value())
        << "Column " << col_name << " has no row identity columns";
    EXPECT_EQ(*row_ids, expected_row_ids);
  }

  TypeFactory type_factory_;
  std::unique_ptr<SimpleTable> base_table_;
  std::unique_ptr<SampleCatalog> catalog_;
  AnalyzerOptions options_;
};

// Tests that we can successfully add a single, standard measure that only
// references base table columns.
TEST_F(AddMeasureColumnsToTableTest, AddSingleMeasure) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 1);
  EXPECT_EQ(base_table_->NumColumns(), 3);  // x, y, m1

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");
}

// Tests that we can successfully add a derived measure that references a
// previously defined measure in the same batch (left-to-right dependency).
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureSuccess) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "AGG(m1) + 1"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *base_table_, "m2",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="m1")
+-Literal(type=INT64, value=1)
)");
}

// Tests that a constant aggregate is resolved inline inside the derived
// measure.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureWithConstantAggregate) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "m1", .expression = "AGG(b) + SUM(1)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="b")
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
  +-Literal(type=INT64, value=1)
)");
}

// Tests that aggregates inside a subquery are shielded and not extracted.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureWithSubquery) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "m1",
       .expression = "AGG(b) + (SELECT SUM(x) FROM UNNEST([1, 2]) AS x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="b")
+-SubqueryExpr
  +-type=INT64
  +-subquery_type=SCALAR
  +-subquery=
    +-ProjectScan
      +-column_list=[$aggregate.$agg1#2]
      +-input_scan=
        +-AggregateScan
          +-column_list=[$aggregate.$agg1#2]
          +-input_scan=
          | +-ArrayScan
          |   +-column_list=[$array.x#1]
          |   +-array_expr_list=
          |   | +-Literal(type=ARRAY<INT64>, value=[1, 2])
          |   +-element_column_list=[$array.x#1]
          +-aggregate_list=
            +-$agg1#2 :=
              +-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
                +-ColumnRef(type=INT64, column=$array.x#1)
)");
}

// Tests that we reject measure definitions that reference measures defined
// later in the batch (forward references). Order must be left-to-right.
TEST_F(AddMeasureColumnsToTableTest, ForwardReferenceFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m2", .expression = "m1 + 1"},
      {.name = "m1", .expression = "SUM(x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Unrecognized name: m1 [at 1:1]"));
}

TEST_F(AddMeasureColumnsToTableTest,
       AddMeasureWithDuplicateNameOfExistingColumnFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "x", .expression = "SUM(x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Duplicate column in BaseTable: x")));
}

TEST_F(AddMeasureColumnsToTableTest, AddDuplicateMeasureNamesFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m1", .expression = "SUM(y)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Duplicate column in BaseTable: m1")));
}

// Tests that we reject measure definitions where a measure is nested inside
// another standard aggregate function (e.g., SUM(AGG(measure))).
TEST_F(AddMeasureColumnsToTableTest, MeasureInsideStandardAggregateFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(AGG(m1) GROUP BY x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(
      status_or_outputs.status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "AGG cannot be nested inside another aggregate function. [at 1:5]"));
}

// Tests that we can successfully specify a derived measure that is a sum
// of two other measures.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureTwoMeasuresSuccess) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(y)"},
      {.name = "m3", .expression = "AGG(m1) + AGG(m2)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");
  ExpectColumnExpressionAST(
      *base_table_, "m2",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="y")
)");
  ExpectColumnExpressionAST(
      *base_table_, "m3",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="m1")
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
  +-ExpressionColumn(type=MEASURE<INT64>, name="m2")
)");
}

// Tests that we can successfully specify a derived measure using a conditional
// expression (IF) referencing other measures.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureConditionalSuccess) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(y)"},
      {.name = "m3", .expression = "IF(TRUE, AGG(m1), AGG(m2))"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");
  ExpectColumnExpressionAST(
      *base_table_, "m2",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="y")
)");
  ExpectColumnExpressionAST(
      *base_table_, "m3",
      R"(FunctionCall(GoogleSQL:if(BOOL, INT64, INT64) -> INT64)
+-Literal(type=BOOL, value=true)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="m1")
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
  +-ExpressionColumn(type=MEASURE<INT64>, name="m2")
)");
}

// Tests that we can successfully add a derived measure to a value table.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureToValueTableSuccess) {
  std::vector<StructType::StructField> fields = {
      {"a", type_factory_.get_int64()}, {"b", type_factory_.get_int64()}};
  const StructType* struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory_.MakeStructType(fields, &struct_type));
  auto value_table = std::make_unique<SimpleTable>("ValueTable", struct_type);

  std::vector<MeasureColumnDef> measures = {
      {.name = "measure_sum_a",
       .expression = "SUM(a)",
       .is_pseudo_column = true},
      {.name = "derived_measure_sum_a_plus_one",
       .expression = "AGG(measure_sum_a) + 1",
       .is_pseudo_column = true},
  };

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto outputs, AddMeasureColumnsToTable(
                                         *value_table, measures, type_factory_,
                                         *catalog_->catalog(), options_));
  EXPECT_EQ(outputs.size(), 2);

  ExpectColumnExpressionAST(
      *value_table, "measure_sum_a",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-GetStructField
  +-type=INT64
  +-expr=
  | +-ExpressionColumn(type=STRUCT<a INT64, b INT64>, name="value")
  +-field_idx=0
)");

  ExpectColumnExpressionAST(
      *value_table, "derived_measure_sum_a_plus_one",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="measure_sum_a")
+-Literal(type=INT64, value=1)
)");
}

// Tests that we can successfully add a derived measure with non-measure
// aggregations resolved inline.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureWithStandardAggregate) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(y) + AGG(m1)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);
  EXPECT_EQ(base_table_->NumColumns(), 4);

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *base_table_, "m2",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
| +-ExpressionColumn(type=INT64, name="y")
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
  +-ExpressionColumn(type=MEASURE<INT64>, name="m1")
)");
}

// Tests that we can successfully add a derived measure with standard aggregates
// that has constant arguments.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithStandardAggregateConstantArg) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "d", .expression = "AGG(b) + SUM(1)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "b",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *base_table_, "d",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="b")
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
  +-Literal(type=INT64, value=1)
)");
}

// Tests that we can successfully add a derived measure with standard aggregates
// and literals.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithStandardAggregateAndLiteral) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "d", .expression = "AGG(b) + SUM(y) + 1"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "b",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *base_table_, "d",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
| +-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| | +-ExpressionColumn(type=MEASURE<INT64>, name="b")
| +-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
|   +-ExpressionColumn(type=INT64, name="y")
+-Literal(type=INT64, value=1)
)");
}

// Tests that we reject derived measures that contain subquery aggregations,
// because we don't support extracting them as implicit measures.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithSubqueryAggregationFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "d", .expression = "AGG(b) + (SELECT SUM(y) FROM UNNEST([1]))"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  // We expect this to fail during post-rewrite validation because the subquery
  // aggregation is not extracted and thus remains in the rewritten expression.
  EXPECT_THAT(
      status_or_outputs.status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "Expression columns in a measure expression can only be referenced "
          "within an aggregate function call: "
          "AGG(b) + (SELECT SUM(y) FROM UNNEST([1]))"));
}

// Tests that derived measures with standard aggregates inherit the grain
// locking keys specified for the derived measure.
TEST_F(AddMeasureColumnsToTableTest,
       DerivedMeasureWithStandardAggregateGrainLockingKeys) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2",
       .expression = "SUM(y) + AGG(m1)",
       .row_identity_column_indices = std::vector<int>{0}},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *base_table_, "m2",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
| +-ExpressionColumn(type=INT64, name="y")
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
  +-ExpressionColumn(type=MEASURE<INT64>, name="m1")
)");

  ExpectColumnRowIdentity(*base_table_, "m2", {0});
}

// Tests that we reject measure definitions with non-measure aggregations that
// contain measures via subqueries.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithImplicitDerivedMeasureFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2",
       .expression = "SUM((SELECT AGG(m1) FROM UNNEST([1]))) + AGG(m1)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Nested AGG calls under another aggregate function call "
                       "are not supported: "
                       "SUM((SELECT AGG(m1) FROM UNNEST([1]))) + AGG(m1)"));
}

// Tests that an evaluated measure reference AGG(m) is forbidden inside another
// aggregate function if it is contained within a subquery.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureWithSubqueryFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM((SELECT AGG(m1) FROM UNNEST([1])))"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Nested AGG calls under another aggregate function call "
                       "are not supported: "
                       "SUM((SELECT AGG(m1) FROM UNNEST([1])))"));
}

// Tests that an evaluated measure reference AGG(m) is forbidden inside a
// conditional subquery.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithConditionalSubqueryFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2",
       .expression = "MAX(IF(TRUE, (SELECT AGG(m1) FROM UNNEST([1])), 0))"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Nested AGG calls under another aggregate function call "
                       "are not supported: "
                       "MAX(IF(TRUE, (SELECT AGG(m1) FROM UNNEST([1])), 0))"));
}

// Tests that we reject measure definitions where an evaluated measure reference
// AGG(m) appears as a child of another aggregate function (nested aggregation).
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithNestedAggregationFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(AGG(m1) GROUP BY x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(
      status_or_outputs.status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "AGG cannot be nested inside another aggregate function. [at 1:5]"));
}

// Tests that we reject measure definitions where an evaluated measure reference
// AGG(m) appears as a child of another aggregate function in a complex
// expression.
TEST_F(AddMeasureColumnsToTableTest, AddDerivedMeasureWithComplexNestingFails) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(x) + SUM(AGG(m1) GROUP BY x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(
      status_or_outputs.status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "AGG cannot be nested inside another aggregate function. [at 1:14]"));
}

// Tests that we don't extract implicit measures for a base measure definition
// with multiple non-measure aggregates.
TEST_F(AddMeasureColumnsToTableTest,
       BaseMeasureWithMultipleAggregatesNoExtraction) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x) + SUM(y)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  // There should be only 1 output (the measure 'b' itself), no synthetic
  // measures.
  EXPECT_EQ(status_or_outputs.value().size(), 1);
  EXPECT_EQ(base_table_->NumColumns(), 3);  // x, y, b

  ExpectColumnExpressionAST(
      *base_table_, "b",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
| +-ExpressionColumn(type=INT64, name="x")
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
  +-ExpressionColumn(type=INT64, name="y")
)");
}

// Tests that we successfully resolve derived measures referencing another
// derived measure and containing multiple non-measure aggregates inline.
TEST_F(AddMeasureColumnsToTableTest,
       DerivedMeasureReferencingDerivedMeasureSuccess) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "d", .expression = "AGG(b) + 1"},
      {.name = "m", .expression = "SUM(x) + AGG(d) + AVG(x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 3);
  EXPECT_EQ(base_table_->NumColumns(), 5);

  ExpectColumnExpressionAST(
      *base_table_, "b",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *base_table_, "d",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="b")
+-Literal(type=INT64, value=1)
)");

  ExpectColumnExpressionAST(
      *base_table_, "m",
      R"(FunctionCall(GoogleSQL:$add(DOUBLE, DOUBLE) -> DOUBLE)
+-Cast(INT64 -> DOUBLE)
| +-FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
|   +-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
|   | +-ExpressionColumn(type=INT64, name="x")
|   +-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
|     +-ExpressionColumn(type=MEASURE<INT64>, name="d")
+-AggregateFunctionCall(GoogleSQL:avg(INT64) -> DOUBLE)
  +-ExpressionColumn(type=INT64, name="x")
)");
}

// Tests that we can successfully add a derived measure to a proto value table.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureToProtoValueTableSuccess) {
  const google::protobuf::Descriptor* descriptor =
      google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(
          "googlesql_test.MessageWithNulls");
  ASSERT_NE(descriptor, nullptr);
  const Type* proto_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory_.MakeProtoType(descriptor, &proto_type));
  auto value_table = std::make_unique<SimpleTable>("ValueTable", proto_type);

  std::vector<MeasureColumnDef> measures = {
      {.name = "measure_sum_i1",
       .expression = "SUM(i1)",
       .is_pseudo_column = true},
      {.name = "derived_measure_sum_i1_plus_one",
       .expression = "AGG(measure_sum_i1) + 1",
       .is_pseudo_column = true},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *value_table, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *value_table, "measure_sum_i1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-GetProtoField
  +-type=INT64
  +-expr=
  | +-ExpressionColumn(type=PROTO<googlesql_test.MessageWithNulls>, name="value")
  +-field_descriptor=i1
  +-default_value=NULL
)");

  ExpectColumnExpressionAST(
      *value_table, "derived_measure_sum_i1_plus_one",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="measure_sum_i1")
+-Literal(type=INT64, value=1)
)");
}

// Tests that we reject measure definitions where a field reference in a struct
// value table is ambiguous (e.g., duplicate field names in struct).
TEST_F(AddMeasureColumnsToTableTest, AmbiguousStructFieldFails) {
  std::vector<StructType::StructField> fields = {
      {"a", type_factory_.get_int64()}, {"a", type_factory_.get_int64()}};
  const StructType* struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory_.MakeStructType(fields, &struct_type));
  auto value_table = std::make_unique<SimpleTable>("ValueTable", struct_type);

  std::vector<MeasureColumnDef> measures = {
      {.name = "measure_sum_a",
       .expression = "SUM(a)",
       .is_pseudo_column = true},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *value_table, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(
      status_or_outputs.status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "Field a is ambiguous in value table: ValueTable of type: "
               "STRUCT<a INT64, a INT64>"));
}

// Tests that we reject measure definitions where a column name in a value table
// is ambiguous because it matches both a struct field and a pseudo column.
TEST_F(AddMeasureColumnsToTableTest, AmbiguousColumnNameInValueTableFails) {
  std::vector<StructType::StructField> fields = {
      {"a", type_factory_.get_int64()}};
  const StructType* struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory_.MakeStructType(fields, &struct_type));

  // Create a table and add a pseudo column 'a' to it, which conflicts with the
  // struct field 'a'.
  auto value_table = std::make_unique<SimpleTable>("ValueTable", struct_type);
  auto pseudo_col = std::make_unique<SimpleColumn>(
      "ValueTable", "a", type_factory_.get_int64(), /*is_pseudo_column=*/true);
  GOOGLESQL_ASSERT_OK(value_table->AddColumn(pseudo_col.release(), /*is_owned=*/true));

  std::vector<MeasureColumnDef> measures = {
      {.name = "measure_sum_a",
       .expression = "SUM(a)",
       .is_pseudo_column = true},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *value_table, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(
      status_or_outputs.status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "Column `a` is ambiguous in value table `ValueTable` for measure "
          "expression: SUM(a)"));
}

// Tests that implicit measures inherit table-level row identity columns if
// none are explicitly specified in the measure definition.
TEST_F(AddMeasureColumnsToTableTest,
       DerivedMeasureWithStandardAggregateInheritsTableRowIdentity) {
  auto table_with_row_ids = std::make_unique<SimpleTable>(
      "TableWithRowIds",
      std::vector<const Column*>{
          new SimpleColumn("TableWithRowIds", "x", type_factory_.get_int64()),
          new SimpleColumn("TableWithRowIds", "y", type_factory_.get_int64())},
      /*take_ownership=*/true);
  GOOGLESQL_ASSERT_OK(table_with_row_ids->SetRowIdentityColumns(
      {0}));  // Set 'x' as row identity

  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "SUM(y) + AGG(m1)"},
  };

  auto status_or_outputs =
      AddMeasureColumnsToTable(*table_with_row_ids, measures, type_factory_,
                               *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *table_with_row_ids, "m1",
      R"(AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
+-ExpressionColumn(type=INT64, name="x")
)");

  ExpectColumnExpressionAST(
      *table_with_row_ids, "m2",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
| +-ExpressionColumn(type=INT64, name="y")
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
  +-ExpressionColumn(type=MEASURE<INT64>, name="m1")
)");

  const Column* col = table_with_row_ids->FindColumnByName("m2");
  ASSERT_NE(col, nullptr);
  ASSERT_TRUE(col->GetExpression().has_value());
  EXPECT_FALSE(col->GetExpression()->RowIdentityColumns().has_value());
}

// Verifies that resolving a derived measure fails when
// FEATURE_DERIVED_MEASURE is disabled.
TEST_F(AddMeasureColumnsToTableTest, DerivedMeasureFeatureDisabledFails) {
  options_.mutable_language()->DisableLanguageFeature(FEATURE_DERIVED_MEASURE);
  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(x)"},
      {.name = "m2", .expression = "AGG(m1) + 1"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  EXPECT_THAT(status_or_outputs.status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Measure expressions referencing other "
                                 "measures are not supported")));
}

// The multi-level aggregation is resolved inline inside the derived measure.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithMultiLevelAggregation) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "m1", .expression = "AGG(b) + SUM(AVG(x) GROUP BY x)"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(FunctionCall(GoogleSQL:$add(DOUBLE, DOUBLE) -> DOUBLE)
+-Cast(INT64 -> DOUBLE)
| +-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
|   +-ExpressionColumn(type=MEASURE<INT64>, name="b")
+-AggregateFunctionCall(GoogleSQL:sum(DOUBLE) -> DOUBLE)
  +-ColumnRef(type=DOUBLE, column=$aggregate.$agg1#2)
  +-group_by_list=
  | +-$groupbymod#1 := ExpressionColumn(type=INT64, name="x")
  +-group_by_aggregate_list=
    +-$agg1#2 :=
      +-AggregateFunctionCall(GoogleSQL:avg(INT64) -> DOUBLE)
        +-ExpressionColumn(type=INT64, name="x")
)");
}

// The SUM(<subquery>) is resolved inline inside the derived measure.
TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithSubqueryInsideAggregation) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "m1",
       .expression = "AGG(b) + SUM((SELECT SUM(x) FROM UNNEST([1])))"},
  };

  auto status_or_outputs = AddMeasureColumnsToTable(
      *base_table_, measures, type_factory_, *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  EXPECT_EQ(status_or_outputs.value().size(), 2);

  ExpectColumnExpressionAST(
      *base_table_, "m1",
      R"(FunctionCall(GoogleSQL:$add(INT64, INT64) -> INT64)
+-AggregateFunctionCall(GoogleSQL:AGG(MEASURE<INT64>) -> INT64)
| +-ExpressionColumn(type=MEASURE<INT64>, name="b")
+-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
  +-SubqueryExpr
    +-type=INT64
    +-subquery_type=SCALAR
    +-subquery=
      +-ProjectScan
        +-column_list=[$aggregate.$agg1#2]
        +-input_scan=
          +-AggregateScan
            +-column_list=[$aggregate.$agg1#2]
            +-input_scan=
            | +-ArrayScan
            |   +-column_list=[$array.$unnest1#1]
            |   +-array_expr_list=
            |   | +-Literal(type=ARRAY<INT64>, value=[1])
            |   +-element_column_list=[$array.$unnest1#1]
            +-aggregate_list=
              +-$agg1#2 :=
                +-AggregateFunctionCall(GoogleSQL:sum(INT64) -> INT64)
                  +-ExpressionColumn(type=INT64, name="x")
)");
}

TEST_F(AddMeasureColumnsToTableTest,
       AddDerivedMeasureWithSubqueryDerivedMeasure) {
  std::vector<MeasureColumnDef> measures = {
      {.name = "b", .expression = "SUM(x)"},
      {.name = "m1", .expression = "SUM(x) + (SELECT AGG(b) FROM UNNEST([1]))"},
  };

  EXPECT_THAT(AddMeasureColumnsToTable(*base_table_, measures, type_factory_,
                                       *catalog_->catalog(), options_),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Expression columns in a measure expression can only be "
                       "referenced within an aggregate function call: "
                       "SUM(x) + (SELECT AGG(b) FROM UNNEST([1]))"));
}

TEST_F(AddMeasureColumnsToTableTest, AddMeasureWithCollationAnnotation) {
  options_.mutable_language()->EnableLanguageFeature(FEATURE_COLLATION_SUPPORT);
  options_.mutable_language()->EnableLanguageFeature(
      FEATURE_ANNOTATION_FRAMEWORK);

  const AnnotationMap* collation_ci_map = nullptr;
  {
    std::unique_ptr<AnnotationMap> map =
        AnnotationMap::Create(type_factory_.get_string());
    map->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(collation_ci_map,
                         type_factory_.TakeOwnership(std::move(map)));
  }

  auto table_with_annotation = std::make_unique<SimpleTable>(
      "AnnotatedTable",
      std::vector<const Column*>{new SimpleColumn(
          "AnnotatedTable", "str_col",
          AnnotatedType(type_factory_.get_string(), collation_ci_map))},
      /*take_ownership=*/true);

  std::vector<MeasureColumnDef> measures = {
      {.name = "m_ci", .expression = "MAX(str_col)"},
  };

  auto status_or_outputs =
      AddMeasureColumnsToTable(*table_with_annotation, measures, type_factory_,
                               *catalog_->catalog(), options_);
  GOOGLESQL_ASSERT_OK(status_or_outputs.status());
  ASSERT_EQ(status_or_outputs.value().size(), 1);

  const Column* m_ci_col = table_with_annotation->FindColumnByName("m_ci");
  ASSERT_NE(m_ci_col, nullptr);
  EXPECT_TRUE(m_ci_col->GetType()->IsMeasureType());
  ASSERT_NE(m_ci_col->GetTypeAnnotationMap(), nullptr);
  const StructAnnotationMap* struct_map =
      m_ci_col->GetTypeAnnotationMap()->AsStructMap();
  ASSERT_NE(struct_map, nullptr);
  ASSERT_NE(struct_map->field(0), nullptr);
  EXPECT_TRUE(CollationAnnotation::ExistsIn(struct_map->field(0)));
}

// Tests that adding a measure to a struct value table correctly propagates
// field-level type annotations from the struct value column.
TEST_F(AddMeasureColumnsToTableTest,
       AddMeasureToStructValueTableWithFieldAnnotations) {
  options_.mutable_language()->EnableLanguageFeature(FEATURE_COLLATION_SUPPORT);
  options_.mutable_language()->EnableLanguageFeature(
      FEATURE_ANNOTATION_FRAMEWORK);

  const StructType* struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(
      type_factory_.MakeStructType({{"str_field", type_factory_.get_string()},
                                    {"int_field", type_factory_.get_int64()}},
                                   &struct_type));

  const AnnotationMap* struct_annotation_map = nullptr;
  {
    std::unique_ptr<AnnotationMap> struct_map =
        AnnotationMap::Create(struct_type);
    struct_map->AsStructMap()
        ->mutable_field(0)
        ->SetAnnotation<CollationAnnotation>(SimpleValue::String("und:ci"));
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(struct_annotation_map,
                         type_factory_.TakeOwnership(std::move(struct_map)));
  }

  auto value_table = std::make_unique<SimpleTable>(
      "StructValueTable",
      std::vector<const Column*>{
          new SimpleColumn("StructValueTable", "value",
                           AnnotatedType(struct_type, struct_annotation_map))},
      /*take_ownership=*/true);
  value_table->set_is_value_table(true);

  std::vector<MeasureColumnDef> measures = {
      {.name = "measure_max_str",
       .expression = "MAX(str_field)",
       .is_pseudo_column = true},
  };

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto outputs, AddMeasureColumnsToTable(
                                         *value_table, measures, type_factory_,
                                         *catalog_->catalog(), options_));
  EXPECT_EQ(outputs.size(), 1);

  const Column* measure_col = value_table->FindColumnByName("measure_max_str");
  ASSERT_NE(measure_col, nullptr);
  EXPECT_TRUE(measure_col->GetType()->IsMeasureType());
  ASSERT_NE(measure_col->GetTypeAnnotationMap(), nullptr);
  const StructAnnotationMap* struct_map =
      measure_col->GetTypeAnnotationMap()->AsStructMap();
  ASSERT_NE(struct_map, nullptr);
  ASSERT_NE(struct_map->field(0), nullptr);
  EXPECT_TRUE(CollationAnnotation::ExistsIn(struct_map->field(0)));

  ExpectColumnExpressionAST(
      *value_table, "measure_max_str",
      R"(AggregateFunctionCall(GoogleSQL:max(STRING) -> STRING)
+-type_annotation_map={Collation:"und:ci"}
+-GetStructField
| +-type=STRING
| +-type_annotation_map={Collation:"und:ci"}
| +-expr=
| | +-ExpressionColumn(type=STRUCT<str_field STRING, int_field INT64>, type_annotation_map=<{Collation:"und:ci"},_>, name="value")
| +-field_idx=0
+-collation_list=[und:ci]
)");
}

// Tests that the captured struct field names are the same as the original
// column names, with the same case.
TEST_F(AddMeasureColumnsToTableTest,
       UpdateTableRowsWithMeasureValuesCapturedColumnNames) {
  auto upper_table = std::make_unique<SimpleTable>(
      "UpperTable",
      std::vector<const Column*>{
          new SimpleColumn("UpperTable", "IdCol", type_factory_.get_int64()),
          new SimpleColumn("UpperTable", "ValCol", type_factory_.get_int64())},
      /*take_ownership=*/true);
  GOOGLESQL_ASSERT_OK(upper_table->SetRowIdentityColumns({0}));

  std::vector<MeasureColumnDef> measures = {
      {.name = "m1", .expression = "SUM(ValCol)"},
  };

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_outputs,
      AddMeasureColumnsToTable(*upper_table, measures, type_factory_,
                               *catalog_->catalog(), options_));

  const StructType* input_row_type = nullptr;
  GOOGLESQL_ASSERT_OK(
      type_factory_.MakeStructType({{"IdCol", type_factory_.get_int64()},
                                    {"ValCol", type_factory_.get_int64()}},
                                   &input_row_type));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      Value row_val,
      Value::MakeStruct(input_row_type, {Value::Int64(1), Value::Int64(10)}));

  const ArrayType* array_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory_.MakeArrayType(input_row_type, &array_type));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Value array_val,
                       Value::MakeArray(array_type, {row_val}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      Value updated_array_val,
      UpdateTableRowsWithMeasureValues(array_val, upper_table.get(), measures,
                                       /*table_level_row_identity_columns=*/{0},
                                       &type_factory_, options_.language()));
  ASSERT_EQ(updated_array_val.num_elements(), 1);
  Value updated_row = updated_array_val.element(0);

  const StructType* updated_row_type = updated_row.type()->AsStruct();
  ASSERT_NE(updated_row_type, nullptr);
  ASSERT_EQ(updated_row_type->num_fields(), 3);
  EXPECT_EQ(updated_row_type->field(0).name, "IdCol");
  EXPECT_EQ(updated_row_type->field(1).name, "ValCol");
  EXPECT_EQ(updated_row_type->field(2).name, "m1");

  Value measure_val = updated_row.field(2);
  ASSERT_TRUE(measure_val.type()->IsMeasureType());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Value captured_struct,
                       InternalValue::GetMeasureAsStructValue(measure_val));
  const StructType* captured_struct_type = captured_struct.type()->AsStruct();
  ASSERT_NE(captured_struct_type, nullptr);

  // The captured struct should have fields with the same case as the original
  // columns.
  ASSERT_EQ(captured_struct_type->num_fields(), 2);
  EXPECT_EQ(captured_struct_type->field(0).name, "IdCol");
  EXPECT_EQ(captured_struct_type->field(1).name, "ValCol");
}

}  // namespace
}  // namespace googlesql
