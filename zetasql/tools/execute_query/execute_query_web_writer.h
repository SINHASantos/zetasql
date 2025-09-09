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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WEB_WRITER_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WEB_WRITER_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/tools/execute_query/execute_query_writer.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "external/mstch/mstch/include/mstch/mstch.hpp"

namespace zetasql {

// Writer for an ExecuteQuery call that populates template params used for the
// HTML output.
class ExecuteQueryWebWriter : public ExecuteQueryWriter {
 public:
  explicit ExecuteQueryWebWriter(mstch::map &template_params)
      : template_params_(template_params) {
    // The `statements` param is an array of maps, containing the `error`
    // and `result_*` keys for each statmement.
    template_params_["statements"] = mstch::array();
  }

  ExecuteQueryWebWriter(const ExecuteQueryWebWriter &) = delete;
  ExecuteQueryWebWriter &operator=(const ExecuteQueryWebWriter &) = delete;

  absl::Status statement_text(absl::string_view statement) override {
    current_statement_params_["statement_text"] = std::string(statement);
    return absl::OkStatus();
  }

  absl::Status log(absl::string_view message) override {
    absl::StrAppend(&log_messages_, message, "\n");
    current_statement_params_["result_log"] = std::string(log_messages_);
    got_results_ = true;
    return absl::OkStatus();
  }

  absl::Status parsed(absl::string_view parse_debug_string) override {
    current_statement_params_["result_parsed"] =
        std::string(parse_debug_string);
    got_results_ = true;
    return absl::OkStatus();
  }

  absl::Status unparsed(absl::string_view unparse_string) override {
    current_statement_params_["result_unparsed"] = std::string(unparse_string);
    got_results_ = true;
    return absl::OkStatus();
  }

  absl::Status resolved(const ResolvedNode& ast) override;

  absl::Status unanalyze(absl::string_view unanalyze_string) override {
    current_statement_params_["result_unanalyzed"] =
        std::string(unanalyze_string);
    got_results_ = true;
    return absl::OkStatus();
  }

  absl::Status explained(const ResolvedNode &ast,
                         absl::string_view explain) override {
    current_statement_params_["result_explained"] = std::string(explain);
    got_results_ = true;
    return absl::OkStatus();
  }

  absl::Status executed(absl::string_view output) override {
    current_statement_params_["result_executed"] = true;
    current_statement_params_["result_executed_text"] = std::string(output);
    got_results_ = true;
    return absl::OkStatus();
  }

  absl::Status executed(const ResolvedNode &ast,
                        std::unique_ptr<EvaluatorTableIterator> iter) override;

  absl::Status executed_multi(
      const ResolvedNode& ast,
      std::vector<absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>>
          results) override;

  absl::Status ExecutedExpression(const ResolvedNode &ast,
                                  const Value &value) override;

  absl::Status StartStatement(bool is_first) override {
    if (!is_first) {
      FlushStatement(/*at_end=*/false);
    }
    return absl::OkStatus();
  }

  void FlushStatement(bool at_end, std::string error_msg = "") {
    if (GotResults()) {
      current_statement_params_["result"] = true;
    }
    if (!error_msg.empty()) {
      current_statement_params_["error"] = error_msg;
    }

    bool has_content = GotResults() || !error_msg.empty();
    if (has_content) {
      if (!at_end) {
        current_statement_params_["not_is_last"] = true;
      }

      // This would be preferred, but I can't get it work on these boost
      // variant types, so I'm maintaining the array separately and copying it
      // back into the map every time it gets updated.
      //   template_params_["statements"]).push_back(current_statement_params_);
      statement_params_array_.push_back(current_statement_params_);
      template_params_["statements"] = mstch::array(statement_params_array_);

      // Show the input statements when there is more than one statement,
      // so it's easy to match output to input statements.
      if (statement_params_array_.size() > 1) {
        template_params_["show_statement_text"] = true;
      }
    }

    got_results_ = false;
    current_statement_params_.clear();
    log_messages_.clear();
  }

  bool GotResults() const { return got_results_; }

 private:

  mstch::map &template_params_;
  mstch::map current_statement_params_;
  mstch::array statement_params_array_;
  std::string log_messages_;
  bool got_results_{false};
};

}  // namespace zetasql

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WEB_WRITER_H_
