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

#ifndef GOOGLESQL_PARSER_PARSER_INTERNAL_H_
#define GOOGLESQL_PARSER_PARSER_INTERNAL_H_

#include <memory>
#include <vector>

#include "googlesql/common/warning_sink.h"
#include "googlesql/parser/ast_node.h"
#include "googlesql/parser/macros/macro_catalog.h"
#include "googlesql/parser/parse_tree.h"
#include "googlesql/parser/parser_mode.h"
#include "googlesql/parser/parser_runtime_info.h"
#include "googlesql/parser/statement_properties.h"
#include "googlesql/parser/token_stream.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/id_string.h"
#include "googlesql/public/language_options.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/arena.h"

namespace googlesql {
namespace parser {

// Parses `input` in mode `mode`, starting at byte offset `start_byte_offset`.
// Returns the output tree in `output`, or returns an annotated error.
//
// Memory allocation:
// - Identifiers are allocated from `id_string_pool`.
// - ASTNodes are allocated from `arena`.
// - ASTNodes still need to be deleted before the memory pools are destroyed.
//   Ownership of all allocated ASTNodes except for the root output is
//   returned in `other_allocated_ast_nodes`.
// The caller should keep `id_string_pool` and `arena` alive until all the
// returned ASTNodes have been deallocated.
//
// If mode is `kNextStatementKind`, then the next statement kind is returned
// in `ast_statement_properties`, and statement level hints are returned in
// `output`. In this mode, `statement_end_byte_offset` is *not* set.
//
// If mode is kNextStatement, the byte offset past the current statement's
// closing semicolon is returned in `statement_end_byte_offset`. If the
// statement did not end in a semicolon, then `statement_end_byte_offset` is
// set to -1, and the input was guaranteed to be parsed to the end.
//
// If mode is `kStatement`, then `statement_end_byte_offset` is not set.
absl::Status ParseInternal(
    ParserMode mode, absl::string_view filename, absl::string_view input,
    int start_byte_offset, IdStringPool* id_string_pool, googlesql_base::UnsafeArena* arena,
    const LanguageOptions& language_options,
    MacroExpansionMode macro_expansion_mode, Catalog* catalog,
    std::unique_ptr<ASTNode>* output, ParserRuntimeInfo& runtime_info,
    WarningSink& warning_sink,
    std::vector<std::unique_ptr<ASTNode>>* other_allocated_ast_nodes,
    ASTStatementProperties* ast_statement_properties,
    int* statement_end_byte_offset);

}  // namespace parser
}  // namespace googlesql

#endif  // GOOGLESQL_PARSER_PARSER_INTERNAL_H_
