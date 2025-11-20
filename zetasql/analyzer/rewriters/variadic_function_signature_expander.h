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

#ifndef ZETASQL_ANALYZER_REWRITERS_VARIADIC_FUNCTION_SIGNATURE_EXPANDER_H_
#define ZETASQL_ANALYZER_REWRITERS_VARIADIC_FUNCTION_SIGNATURE_EXPANDER_H_

#include <memory>
#include <string>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Rewriter that expands function signatures for variadic functions like
// MAP_INSERT, MAP_INSERT_OR_REPLACE, MAP_REPLACE, and MAP_DELETE to prepare
// them for inlining via REWRITE_BUILTIN_FUNCTION_INLINER.
class VariadicFunctionSignatureExpander : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& analyzer_output_properties) const override;

  std::string Name() const override {
    return "VariadicFunctionSignatureExpander";
  }
};

// Return a pointer to the singleton VariadicFunctionSignatureExpander.
const Rewriter* GetVariadicFunctionSignatureExpander();

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_VARIADIC_FUNCTION_SIGNATURE_EXPANDER_H_
