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

#include "zetasql/analyzer/rewriters/measure_type_rewriter.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/analyzer/rewriters/measure_collector.h"
#include "zetasql/analyzer/rewriters/measure_reference_rewrite_util.h"
#include "zetasql/analyzer/rewriters/measure_source_scan_rewrite_util.h"
#include "zetasql/analyzer/rewriters/measure_type_rewriter_util.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class MeasureTypeRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    if (!options.language().LanguageFeatureEnabled(
            FEATURE_MULTILEVEL_AGGREGATION)) {
      return absl::UnimplementedError(
          "Multi-level aggregation is needed to rewrite measures.");
    }
    if (!options.language().LanguageFeatureEnabled(FEATURE_GROUP_BY_STRUCT)) {
      return absl::UnimplementedError(
          "Grouping by STRUCT types is needed to rewrite measures.");
    }

    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(/*max_seen_col_id=*/0,
                                 *options.id_string_pool(),
                                 *options.column_id_sequence_number());

    // Step 0: Find unsupported query shapes, and return an error if any are
    // found.
    ZETASQL_RETURN_IF_ERROR(HasUnsupportedQueryShape(input.get()));

    const Function* any_value_fn = nullptr;
    ZETASQL_RET_CHECK_OK(catalog.FindFunction({"any_value"}, &any_value_fn,
                                      options.find_options()));
    FunctionCallBuilder function_call_builder(options, catalog, type_factory);

    // The high-level rewrite algorithm is as follows:
    //
    // Step 1: Identify the measure columns that are AGG'ed, i.e., there is an
    // AGG(m) function call. Only the AGG'ed measure columns need to be
    // rewritten.
    //
    // Step 2: Identify the measure source columns, and rewrite their source
    // scans to replace the measure source columns with their computed closures.
    // Note only AGG'ed measure columns are rewritten.
    //
    // Step 3: Rewrite the measure column references.
    //
    // For each AGG(m) function call, we replace it with its measure definition
    // expression rewritten with multi-level aggregation to do grain locking.
    // The referenced columns are replaced with the corresponding struct field
    // access to the closure struct. For example, consider a measure column
    // m := SUM(col) with closure struct column s. We rewrite AGG(m) to
    //
    // ```
    // SUM(
    //   ANY_VALUE(s.referenced_columns.col)
    //   WHERE s IS NOT NULL
    //   GROUP BY s.key_columns
    // )
    // ```
    //
    // For other measure column references, we replace them with the
    // corresponding struct closure columns to propagate the struct closure.

    MeasureCollector measure_collector(column_factory);

    // Step 1: Find AGG(measure) calls and mark measures as AGG'ed.
    ZETASQL_RETURN_IF_ERROR(MarkAggedMeasures(input.get(), measure_collector));

    // Step 2: Identify the measure source columns.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> node,
                     AddClosures(measure_collector, std::move(input),
                                 type_factory, column_factory));

    // All required measure information has been collected. Perform the final
    // correctness check.
    ZETASQL_RETURN_IF_ERROR(measure_collector.Validate());

    // Step 3: Rewrite the measure column references.
    ZETASQL_ASSIGN_OR_RETURN(node, RewriteMeasureColumns(
                               std::move(node), measure_collector, any_value_fn,
                               function_call_builder, options.language(),
                               column_factory, type_factory));
    return node;
  }

  std::string Name() const override { return "MeasureTypeRewriter"; }
};

const Rewriter* GetMeasureTypeRewriter() {
  static const Rewriter* rewriter = new MeasureTypeRewriter();
  return rewriter;
}

}  // namespace zetasql
