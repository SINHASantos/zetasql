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

#ifndef GOOGLESQL_PUBLIC_TEMPLATED_SQL_TVF_H_
#define GOOGLESQL_PUBLIC_TEMPLATED_SQL_TVF_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/error_helpers.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/public/parse_resume_location.h"
#include "googlesql/public/sql_tvf.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/type.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "absl/base/macros.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

// This file includes interfaces and classes related to templated SQL
// TVFs.  It includes classes to represent TemplatedSQLTVFs and their
// signature (TemplatedTVFSignature).
//
// Note: NON-templated SQL TVF objects can be found in sql_tvf.h.

namespace googlesql {

class AnalyzerOptions;
class TableValuedFunctionProto;

// This represents a templated table-valued function with a SQL body. The
// Resolve method of this class parses and resolves this SQL body when the
// function is called, in the context of the actual provided 'input_arguments'.
//
// The purpose of this class is to help support statements of the form
// "CREATE TABLE FUNCTION <name>(<arguments>) AS <query>", where the <arguments>
// may have templated types like "ANY TYPE" or "ANY TABLE". In this case,
// GoogleSQL cannot resolve the <query> right away and must defer this work
// until later when the function is called with concrete argument types.
//
// The current implementation only supports a single table function signature.
// TODO - Support multiple signatures in a TemplatedSQLTVF.
class TemplatedSQLTVF : public SQLTableValuedFunctionInterface {
 public:
  // Constructs a new templated SQL TVF named <function_name_path>, with a
  // single signature in <signature>. The <arg_name_list> should contain exactly
  // one string for each argument name in <signature>, indicating the name of
  // the argument. The <parse_resume_location> contains the source location
  // and string contents of the table function's SQL query body.
  TemplatedSQLTVF(const std::vector<std::string>& function_name_path,
                  const FunctionSignature& signature,
                  const std::vector<std::string>& arg_name_list,
                  const ParseResumeLocation& parse_resume_location,
                  TableValuedFunctionOptions tvf_options = {})
      : TemplatedSQLTVF(function_name_path, signature, arg_name_list,
                        parse_resume_location,
                        /*anonymization_info=*/nullptr, tvf_options) {}

  // Constructs a new templated SQL TVF named <function_name_path>, with a
  // single signature in <signature>. The <arg_name_list> should contain exactly
  // one string for each argument name in <signature>, indicating the name of
  // the argument. The <parse_resume_location> contains the source location
  // and string contents of the table function's SQL query body. The
  // <anonymization_info> contains anonymization properties, such as the user id
  // column name.
  TemplatedSQLTVF(const std::vector<std::string>& function_name_path,
                  const FunctionSignature& signature,
                  const std::vector<std::string>& arg_name_list,
                  const ParseResumeLocation& parse_resume_location,
                  std::unique_ptr<AnonymizationInfo> anonymization_info,
                  TableValuedFunctionOptions tvf_options)
      : SQLTableValuedFunctionInterface(function_name_path, signature,
                                        tvf_options,
                                        std::move(anonymization_info)),
        arg_name_list_(arg_name_list),
        parse_resume_location_(parse_resume_location) {}

  ~TemplatedSQLTVF() override = default;

  const std::vector<std::string>& GetArgumentNames() const override {
    return arg_name_list_;
  }

  // If set, <resolution_catalog_> is used during the Resolve() call in order
  // to resolve the TVF expression (and overrides the <catalog> argument
  // to Resolve()).  Used for templated TVFs inside modules (which resolve
  // against the module catalog in which the TVF is defined).
  void set_resolution_catalog(Catalog* resolution_catalog) {
    resolution_catalog_ = resolution_catalog;
  }

  void set_allow_query_parameters(bool allow) {
    allow_query_parameters_ = allow;
  }

  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         TableValuedFunctionProto* proto) const override;

  static absl::Status Deserialize(
      const TableValuedFunctionProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory, std::unique_ptr<TableValuedFunction>* result);

  ParseResumeLocation GetParseResumeLocation() const override {
    return parse_resume_location_;
  }

 private:
  bool allow_query_parameters() const final { return allow_query_parameters_; }

  // The list of names of all the function arguments, in the same order that
  // they appear in the function signature.
  std::vector<std::string> arg_name_list_;

  // Indicates the table function's original SQL query body inside the
  // CREATE TABLE FUNCTION statement that declared this function, starting
  // after the AS keyword.
  ParseResumeLocation parse_resume_location_;

  // If true, the analyzer allows query parameters within the SQL function body.
  bool allow_query_parameters_ = false;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_TEMPLATED_SQL_TVF_H_
