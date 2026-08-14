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

#ifndef GOOGLESQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WRITER_H_
#define GOOGLESQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WRITER_H_

#include <iosfwd>
#include <memory>
#include <ostream>
#include <string>

#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace googlesql {

class ExecuteQueryWriter {
 public:
  virtual ~ExecuteQueryWriter() = default;

  // Write the text of the statement, if desired by the subclass.
  // By default, this is a no-op.
  virtual absl::Status statement_text(absl::string_view statement) {
    return absl::OkStatus();
  }

  // Write textual logging messages. A newline will be added on the end.
  // This can be called multiple times for the same statement.
  virtual absl::Status log(absl::string_view message) {
    return WriteOperationString("log", message);
  }

  virtual absl::Status parsed(absl::string_view parse_debug_string) {
    return WriteOperationString("parsed", parse_debug_string);
  }
  // Note: This is being abused in some cases to send text directly as output.
  // This doesn't work as expected in web mode.  At most one of those outputs
  // shows up and it goes in the "Unparsed" section.
  virtual absl::Status unparsed(absl::string_view unparse_string) {
    return WriteOperationString("unparsed", unparse_string);
  }

  // Called once per statement with the initial resolved AST from the analyzer.
  virtual absl::Status resolved(const ResolvedNode& ast) {
    return absl::UnimplementedError(
        "ExecuteQueryWriter::resolved is not implemented");
  }

  // Called after each rewrite step if enabled.
  virtual absl::Status rewritten(absl::string_view rewriter_name,
                                 const ResolvedNode& ast) {
    return absl::OkStatus();
  }
  virtual absl::Status unanalyze(absl::string_view unanalyze_string) {
    return WriteOperationString("unanalyze", unanalyze_string);
  }

  virtual absl::Status explained(const ResolvedNode& ast,
                                 absl::string_view explain) {
    return absl::UnimplementedError(
        "ExecuteQueryWriter::explained is not implemented");
  }

  // This is used for commands that directly produce output as a string.
  virtual absl::Status executed(absl::string_view output) {
    return WriteOperationString("executed", output);
  }
  // This is used for commands that produce a table as output.
  // `iter` will be nullptr for ResolvedTerminalQueryStmt (from `|> FINISH`).
  // Subclasses should also log a "No result table" message for that case.
  virtual absl::Status executed(const ResolvedNode& ast,
                                std::unique_ptr<EvaluatorTableIterator> iter) {
    if (iter == nullptr) {
      return log("No result table");
    }
    return absl::UnimplementedError(
        "ExecuteQueryWriter::executed is not implemented");
  }

  // This is used for commands that produce multiple tables as output.
  // As in `executed`, some iterators can be nullptr.
  virtual absl::Status executed_multi(
      const ResolvedNode& ast,
      std::vector<absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>>
          results) {
    return absl::UnimplementedError(
        "ExecuteQueryWriter::executed_multi is not implemented");
  }

  virtual absl::Status ExecutedExpression(const ResolvedNode& ast,
                                          const Value& value) {
    return absl::UnimplementedError(
        "ExecuteQueryWriter::executed is not implemented");
  }

  // Called at the start of a statement.  `is_first` is true for the first one.
  virtual absl::Status StartStatement(bool is_first) {
    return absl::OkStatus();
  }

  virtual void FlushStatement(bool at_end, std::string error_msg) = 0;

 protected:
  virtual absl::Status WriteOperationString(absl::string_view operation_name,
                                            absl::string_view str) {
    return absl::UnimplementedError(
        absl::StrCat("ExecuteQueryWriter does not implement ", operation_name));
  }
};

absl::Status PrintResults(std::unique_ptr<EvaluatorTableIterator> iter,
                          std::ostream& out, bool use_box_glyphs = false);

// Writes a human-readable representation of the query result to an output
// stream.
class ExecuteQueryStreamWriter : public ExecuteQueryWriter {
 public:
  explicit ExecuteQueryStreamWriter(std::ostream&, bool use_box_glyphs = false,
                                    bool linear_resolved_ast = false);
  ExecuteQueryStreamWriter(const ExecuteQueryStreamWriter&) = delete;
  ExecuteQueryStreamWriter& operator=(const ExecuteQueryStreamWriter&) = delete;

  absl::Status resolved(const ResolvedNode& ast) override;
  absl::Status rewritten(absl::string_view rewriter_name,
                         const ResolvedNode& ast) override;
  absl::Status explained(const ResolvedNode& ast,
                         absl::string_view explain) override;
  absl::Status executed(const ResolvedNode& ast,
                        std::unique_ptr<EvaluatorTableIterator> iter) override;

  absl::Status executed_multi(
      const ResolvedNode& ast,
      std::vector<absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>>
          results) override;

  absl::Status ExecutedExpression(const ResolvedNode& ast,
                                  const Value& value) override;

  void FlushStatement(bool at_end, std::string error_msg) override;

 protected:
  absl::Status WriteOperationString(absl::string_view operation_name,
                                    absl::string_view str) override {
    stream_ << str << '\n';
    return absl::OkStatus();
  }

 private:
  std::ostream& stream_;
  bool use_box_glyphs_;
  bool linear_resolved_ast_;
};

}  // namespace googlesql

#endif  // GOOGLESQL_TOOLS_EXECUTE_QUERY_EXECUTE_QUERY_WRITER_H_
