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

#ifndef GOOGLESQL_PUBLIC_SQL_TVF_H_
#define GOOGLESQL_PUBLIC_SQL_TVF_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/error_helpers.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/parse_resume_location.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

// This file is includes interfaces and classes related to NON-templated SQL
// TVFs.  Currently, the only class implemented is SQLTableValuedFunction.
// We may want to add a generic interface at some point (for example,
// SQLTableValuedFunctionInterface) like we have for SQL Functions, but we
// don't have a need for that interface yet since there is only a single
// implementation.
//
// Note: Templated SQL TVF objects can be found in templated_sql_tvf.h.

namespace googlesql {

// Shared interface for all SQL TVFs, concrete and templated.
// Even concrete TVFs may need to be re-resolved: when arguments have
// different annotations from args defined at creation time.
class SQLTableValuedFunctionInterface : public TableValuedFunction {
 public:
  SQLTableValuedFunctionInterface(
      const std::vector<std::string>& function_name_path,
      const FunctionSignature& signature,
      const TableValuedFunctionOptions& tvf_options)
      : TableValuedFunction(function_name_path, /*group=*/"",
                            std::vector<FunctionSignature>{signature},
                            /*anonymization_info=*/nullptr, tvf_options) {}

  SQLTableValuedFunctionInterface(
      const std::vector<std::string>& function_name_path,
      const FunctionSignature& signature,
      const TableValuedFunctionOptions& tvf_options,
      std::unique_ptr<AnonymizationInfo> anonymization_info)
      : TableValuedFunction(function_name_path, /*group=*/"",
                            std::vector<FunctionSignature>{signature},
                            std::move(anonymization_info), tvf_options) {}

  virtual const std::vector<std::string>& GetArgumentNames() const = 0;

  // Templated TVFs always re-resolve the body for each invocation.
  // Concrete TVFs re-resolve if annotations differ from the definition, to
  // attach the annotated body for this invocation. Otherwise, just returns the
  // cached resolved body.
  absl::Status Resolve(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& input_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* tvf_signature) const final {
    return ResolveInternal(analyzer_options, input_arguments,
                           concrete_signature, catalog, type_factory,
                           tvf_signature);
  }

  // If set, `resolution_catalog_` is used during the Resolve() call,
  // particularly if re-resolution is needed in order to propagate annotations
  // for this particular invocation (as they may differ from the annotations
  // propagated during the initial resolution at the TVF creation.)
  // In that case, the TVF acts like a templated TVF, where this catalog is used
  // to re-resolve the TVF expression and overrides the `catalog` argument
  // to Resolve().
  // Used for TVFs inside modules (which resolve against the module catalog in
  // which the TVF is defined).
  void set_resolution_catalog(Catalog* resolution_catalog) {
    resolution_catalog_ = resolution_catalog;
  }

  Catalog* resolution_catalog() const { return resolution_catalog_; }

 protected:
  virtual ParseResumeLocation GetParseResumeLocation() const = 0;

  virtual bool allow_query_parameters() const { return false; }

  // This is a helper method when parsing or analyzing the table function's
  // SQL expression body.  If 'status' is OK, also returns OK. Otherwise,
  // returns a new error forwarding any nested errors in 'status' obtained
  // from this nested parsing or analysis.
  // TODO: Remove ErrorMessageOptions, once we consistently always save
  // these status objects with payload, and only produce the mode-versioned
  // status when fetched through FindXXX() calls?
  absl::Status ForwardNestedResolutionAnalysisError(
      const absl::Status& status, ErrorMessageOptions options) const;

  // Returns a new error reporting a failed expectation of the sql_body_
  // (for example, if it is a CREATE TABLE instead of a SELECT statement).
  // If 'message' is not empty, appends it to the end of the error string.
  absl::Status MakeTVFQueryAnalysisError(absl::string_view message = "") const;

  // Performs some quick sanity checks on the function signature before starting
  // nested analysis.
  absl::Status CheckIsValid() const;

  // Analyzes the body of this function in context of the arguments provided for
  // a specific call.
  //
  // If 'resolution_catalog_' is non-NULL, then the TVF expression is resolved
  // against 'resolution_catalog_', and the 'catalog' argument is ignored.
  // Otherwise the TVF expression is resolved against 'catalog'.
  //
  // If this analysis succeeds, returns the output schema of
  // this TVF call in 'output_tvf_call', which is guaranteed to be a
  // TemplatedSQLTVFSignature.
  virtual absl::Status ResolveInternal(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& input_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* tvf_signature) const;

  // If non-NULL, this catalog will override the catalog passed to `Resolve()`
  // when it is called to resolve the TVF expression for given arguments.
  Catalog* resolution_catalog_ = nullptr;
};

// The TemplatedSQLTVF::Resolve method returns an instance of this class. It
// contains the fully-resolved query inside the function body after processing
// it in the context of all the provided input arguments.
class TemplatedSQLTVFSignature : public TVFSignature {
 public:
  // Represents a TVF call that returns 'output_schema'. Takes ownership of
  // 'resolved_templated_query'.
  TemplatedSQLTVFSignature(
      const std::vector<TVFInputArgumentType>& input_arguments,
      const TVFRelation& output_schema,
      const TVFSignatureOptions& tvf_signature_options,
      std::unique_ptr<const ResolvedQueryStmt> resolved_templated_query,
      const std::vector<std::string>& arg_name_list)
      : TVFSignature(input_arguments, output_schema, tvf_signature_options),
        resolved_templated_query_(std::move(resolved_templated_query)),
        arg_name_list_(arg_name_list) {}

  TemplatedSQLTVFSignature(const TemplatedSQLTVFSignature&) = delete;
  TemplatedSQLTVFSignature& operator=(const TemplatedSQLTVFSignature&) = delete;
  ~TemplatedSQLTVFSignature() override = default;

  // This contains the fully-resolved function body in context of the actual
  // concrete types of the provided arguments to the function call.
  //
  // The returned pointer will be invalid after calling the
  // `set_resolved_templated_query`.
  const ResolvedQueryStmt* resolved_templated_query() const {
    return resolved_templated_query_.get();
  }

  // Replaces the resolved function body.
  //
  // The returned pointer from `resolved_templated_query` will be invalid after
  // calling this method.
  void set_resolved_templated_query(
      std::unique_ptr<const ResolvedQueryStmt> resolved_templated_query) {
    resolved_templated_query_ = std::move(resolved_templated_query);
  }

  // The list of names of all the function arguments, in the same order that
  // they appear in the function signature.
  const std::vector<std::string>& GetArgumentNames() const {
    return arg_name_list_;
  }

  // Returns a copy of this TVF signature without the
  // `resolved_templated_query_`.
  std::shared_ptr<TemplatedSQLTVFSignature> CopyWithoutResolvedTemplatedQuery()
      const;

 private:
  std::unique_ptr<const ResolvedQueryStmt> resolved_templated_query_;
  const std::vector<std::string> arg_name_list_;
};

// The SQLTableValuedFunction is a sub-class of TableValuedFunction for
// *non-templated* table functions (functions whose arguments are strongly
// typed, i.e., does not include arguments having type ANY or ARBITRARY) whose
// implementation is defined with statement like:
//
// 'CREATE TABLE FUNCTION ... AS <sql query>'
//
// This class is marked `final` because the behavior of a SQL function is fully
// determined by its definition (otherwise, inliners wouldn't be consistent or
// correct). If any virtual functions are added to the superclass, we want to
// avoid any subclasses potentially changing behaviors.
//
// The current implementation only supports a single table function signature.
// TODO - Extend this implementation to support multiple different
// signatures (differing number and/or names of arguments).
class SQLTableValuedFunction final : public SQLTableValuedFunctionInterface {
 public:
  // Creates a SQLTableValuedFunction from the resolved
  // <create_tvf_statement>.  Returns an error if the
  // SQLTableValuedFunction could not be successfully created (for
  // example if the <create_tvf_statement> is not for a non-templated SQL TVF).
  //
  // Does not take ownership of <create_tvf_statement>, which must outlive
  // this class.
  static absl::Status Create(
      const ::googlesql::ResolvedCreateTableFunctionStmt* create_tvf_statement,
      std::unique_ptr<SQLTableValuedFunction>* simple_sql_tvf);

  // Creates a SQLTableValuedFunction from the resolved
  // <create_tvf_statement> and <tvf_options>.  Returns an error if the
  // SQLTableValuedFunction could not be successfully created (for
  // example if the <create_tvf_statement> is not for a non-templated SQL TVF).
  //
  // Does not take ownership of <create_tvf_statement>, which must outlive
  // this class.
  static absl::Status Create(
      const ::googlesql::ResolvedCreateTableFunctionStmt* create_tvf_statement,
      TableValuedFunctionOptions tvf_options,
      std::unique_ptr<SQLTableValuedFunction>* simple_sql_tvf);

  // Optimization: If argument annotations are not different from the
  // definition, skips resolution and returns a signature with the
  // `actual_arguments`, and a result schema from `tvf_schema_`.
  absl::Status ResolveInternal(
      const AnalyzerOptions* analyzer_options,
      const std::vector<TVFInputArgumentType>& actual_arguments,
      const FunctionSignature& concrete_signature, Catalog* catalog,
      TypeFactory* type_factory,
      std::shared_ptr<TVFSignature>* tvf_signature) const final;

  // Returns the associated CREATE TABLE FUNCTION statement.
  const ResolvedCreateTableFunctionStmt* ResolvedStatement() const {
    return create_tvf_statement_;
  }

  const std::vector<std::string>& GetArgumentNames() const override {
    return create_tvf_statement_->argument_name_list();
  }

  const ResolvedScan* query() const { return create_tvf_statement_->query(); }

 private:
  // Constructor for valid table functions.
  explicit SQLTableValuedFunction(
      const ResolvedCreateTableFunctionStmt* create_tvf_statement,
      TableValuedFunctionOptions tvf_options)
      : SQLTableValuedFunctionInterface(create_tvf_statement->name_path(),
                                        create_tvf_statement->signature(),
                                        tvf_options),
        tvf_schema_(GetQueryOutputSchema(*create_tvf_statement)),
        create_tvf_statement_(create_tvf_statement) {}

  // Returns a TVFRelation with the names and Types of columns returned by the
  // query related to <create_tvf_statement>.
  static TVFRelation GetQueryOutputSchema(
      const ResolvedCreateTableFunctionStmt& create_tvf_statement);

  ParseResumeLocation GetParseResumeLocation() const override {
    return ParseResumeLocation::FromStringView(create_tvf_statement_->code());
  }

  bool allow_query_parameters() const final { return false; }

  // Instantiates an empty function signature.
  TVFRelation tvf_schema_;
  const ResolvedCreateTableFunctionStmt* create_tvf_statement_ = nullptr;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_SQL_TVF_H_
