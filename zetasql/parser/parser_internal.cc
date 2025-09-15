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

#include "zetasql/parser/parser_internal.h"

#include <cctype>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/timer_util.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/common/warning_sink.h"
#include "zetasql/parser/ast_node.h"
#include "zetasql/parser/ast_node_factory.h"
#include "zetasql/parser/keywords.h"
#include "zetasql/parser/lookahead_transformer.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser_mode.h"
#include "zetasql/parser/parser_runtime_info.h"
#include "zetasql/parser/statement_properties.h"
#include "zetasql/parser/textmapper_lexer_adapter.h"
#include "zetasql/parser/tm_parser.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/parser/token_with_location.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/proto/logging.pb.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_builder.h"  
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"



ABSL_FLAG(
    bool, zetasql_parser_strip_errors, false,
    "Simplify ZetaSQL parser error messages to just \"Syntax Error\". Using "
    "this flag in tests ensures that tests will not be written assuming an "
    "exact parser error message. This allows ZetaSQL changes to improve "
    "error messages later without breaking user tests.");

namespace zetasql {
namespace parser {

namespace internal {

absl::Status MakeSyntaxError(const ParseLocationRange& location,
                             absl::string_view msg) {
  absl::Status status = absl::InvalidArgumentError(msg);
  zetasql::internal::AttachPayload(
      &status, location.start().ToInternalErrorLocation());
  return status;
}

absl::Status MakeSyntaxError(const std::optional<ParseLocationRange>& location,
                             absl::string_view msg) {
  ZETASQL_RET_CHECK(location.has_value())
      << "MakeSyntaxError called with nullopt location";
  return MakeSyntaxError(*location, msg);
}

absl::Status ValidateNoWhitespace(absl::string_view left,
                                  const ParseLocationRange& left_loc,
                                  absl::string_view right,
                                  const ParseLocationRange& right_loc) {
  if (!left_loc.IsAdjacentlyFollowedBy(right_loc)) {
    return MakeSyntaxError(
        left_loc, absl::StrCat("Syntax error: Unexpected whitespace between \"",
                               left, "\" and \"", right, "\""));
  }
  return absl::OkStatus();
}

absl::Status ErrorIfUnparenthesizedNotExpression(ASTNode* rhs_expr) {
  const ASTUnaryExpression* expr = rhs_expr->GetAsOrNull<ASTUnaryExpression>();
  if (expr != nullptr && !expr->parenthesized() &&
      expr->op() == ASTUnaryExpression::NOT) {
    // TODO: nbales - Make this error message actionable by suggesting parens.
    return MakeSqlErrorAtStart(rhs_expr->location())
           << "Syntax error: Unexpected keyword NOT";
  }
  return absl::OkStatus();
}

}  // namespace internal

// Bison parser return values.
//
// Source:
// https://www.gnu.org/software/bison/manual/html_node/Parser-Function.html#Parser-Function

static std::string GetParserModeName(ParserMode mode) {
  switch (mode) {
    case ParserMode::kExpression:
      return "expression";
    case ParserMode::kType:
      return "type";
    case ParserMode::kSubpipeline:
      return "subpipeline";
    case ParserMode::kStatement:
    case ParserMode::kNextStatement:
    case ParserMode::kNextScriptStatement:
    case ParserMode::kNextStatementKind:
      return "statement";
    case ParserMode::kScript:
      return "script";
    case ParserMode::kMacroBody:
      return "macro";
    case ParserMode::kTokenizer:
    case ParserMode::kTokenizerPreserveComments:
      ABSL_LOG(FATAL) << "CleanUpBisonError called in tokenizer mode";
  }
}

// The longest literal value that we're willing to echo in an error message.
// This value is in bytes, not characters. It includes the quotes. The value was
// chosen so that "Unexpected string literal" messages typically still fit on
// one line. (Not an 80-column line, but a realistic line in a query UI.)
//
// When you change this limit, also adjust the test literals in
// parser_errors.test.
static constexpr int kMaxErrorLiteralLength = 50;

static std::string ShortenStringLiteralForError(absl::string_view literal) {
  if (literal.size() > kMaxErrorLiteralLength) {
    // Shorten by inserting "..." before the final quotes.
    int num_end_quotes = 1;
    if (absl::StartsWith(literal, "\"\"\"") ||
        absl::StartsWith(literal, "\'\'\'") ||
        absl::StartsWithIgnoreCase(literal, "r\"\"\"") ||
        absl::StartsWithIgnoreCase(literal, "r\'\'\'")) {
      num_end_quotes = 3;
    }
    int excerpt_size = kMaxErrorLiteralLength - num_end_quotes;
    // If we can't remove at least four bytes in addition to the quotes, then
    // we're not narrowing the output at all. If that happens, just keep the
    // original. The net effect is that all narrowed literals have the same
    // length in bytes, but we'll let some literals through that are just over
    // the limit. (Note that the constant size is in bytes, not characters. This
    // is not that important.)
    if (excerpt_size >
        static_cast<int64_t>(literal.size()) - num_end_quotes - 4) {
      return std::string(literal);
    }
    // Don't cut the string off in the middle of a multibyte character.
    while (!IsWellFormedUTF8(literal.substr(0, excerpt_size))) {
      --excerpt_size;
    }
    return absl::StrCat(
        literal.substr(0, excerpt_size), "...",
        absl::ClippedSubstr(literal, literal.size() - num_end_quotes,
                            num_end_quotes));
  }
  return std::string(literal);
}

static std::string ShortenBytesLiteralForError(absl::string_view literal) {
  if (literal.size() < kMaxErrorLiteralLength) {
    return std::string(literal);
  }
  // Shorten by inserting "..." before the final quotes.
  int num_end_quotes = 1;
  if (absl::StartsWithIgnoreCase(literal, "b\"\"\"") ||
      absl::StartsWithIgnoreCase(literal, "rb\"\"\"") ||
      absl::StartsWithIgnoreCase(literal, "br\"\"\"") ||
      absl::StartsWithIgnoreCase(literal, "b\'\'\'") ||
      absl::StartsWithIgnoreCase(literal, "rb\'\'\'") ||
      absl::StartsWithIgnoreCase(literal, "br\'\'\'")) {
    num_end_quotes = 3;
  }
  const int excerpt_size = kMaxErrorLiteralLength - num_end_quotes;
  // If we can't remove at least four bytes in addition to the quotes, then
  // we're not narrowing the output at all. If that happens, just keep the
  // original. The net effect is that all narrowed literals have the same
  // length in bytes, but we'll let some literals through that are just over
  // the limit.
  if (excerpt_size >
      static_cast<int64_t>(literal.size()) - num_end_quotes - 4) {
    return std::string(literal);
  }
  return absl::StrCat(
      literal.substr(0, excerpt_size), "...",
      absl::ClippedSubstr(literal, literal.size() - num_end_quotes,
                          num_end_quotes));
}

// Generates an error message for a Bison syntax error in `parser_status`. The
// other arguments should match those passed into `ParseInternal()`. It is
// required that 'parser_status' is the actual error produced by the parser for
// the given inputs.
static absl::Status GenerateImprovedBisonSyntaxError(
    const LanguageOptions& language_options, const absl::Status& parse_status,
    ParserMode mode, absl::string_view input, int start_offset,
    MacroExpansionMode macro_expansion_mode,
    const macros::MacroCatalog* macro_catalog, zetasql_base::UnsafeArena* arena) {
  // Bison error messages are always of the form "syntax error, unexpected X,
  // expecting Y", where Y may be of the form "A" or "A or B" or "A or B or C".
  // It will use $end to indicate "end of input". We don't want to have the text
  // X because we can generate a better description ourselves. However, we do
  // want to have the expectations from 'bison_error_message', because they may
  // be useful.
  static LazyRE2 re_expectations = {
      "syntax error, unexpected .*, expecting (.*)"};
  // If there's no match, then 'expectations_string' will be empty.
  std::string expectations_string;
  RE2::FullMatch(parse_status.message(), *re_expectations,
                 &expectations_string);

  const auto& user_facing_kw_images =
      GetUserFacingImagesForSpecialKeywordsMap();

  // Transform the individual expectations, because Bison gives some weird
  // output for some of them.
  std::vector<std::string> expectations =
      absl::StrSplit(expectations_string, " or ", absl::SkipEmpty());
  for (std::string& expectation : expectations) {
    // Wrap the single-character operators, "+=" and "-=" in quotes.
    if ((expectation.size() == 1 && !isalpha(expectation[0])) ||
        expectation == "+=" || expectation == "-=") {
      expectation = absl::StrCat("\"", expectation, "\"");
    }

    // The only time we ever see MACRO_BODY_TOKEN in the expected set is when
    // the macro name is missing in a definition. For a better error message,
    // just replace it with "macro name".
    if (expectation == "MACRO_BODY_TOKEN") {
      expectation = "macro name";
    }

    // We use some special tokens for lexical disambiguation. The labels we
    // give those in the parser are not necessarily user friendly or what we
    // want to show in error messages. Here we r-map those labels back to what
    // the user will find most understandable.
    if (const auto found = user_facing_kw_images.find(expectation);
        found != user_facing_kw_images.end()) {
      expectation = found->second;
    }

    // These are a single token in the Bison tokenizer but we treat them as two
    // tokens externally.
    if (expectation == "\".*\"") {
      expectation = "\".\"";
    } else if (expectation == "\"@{\"") {
      expectation = "\"@\"";
    } else if (expectation == "@@") {
      expectation = "\"@@\"";
    }
    // If it looks like an uppercased keyword, say it's a keyword. All other
    // things such as string literals are described in lower case in the Bison
    // parser's token names.
    static LazyRE2 re_keyword = {"[A-Z]+"};
    if (RE2::FullMatch(expectation, *re_keyword)) {
      expectation = absl::StrCat("keyword ", expectation);
    }
  }
  // Deduplicate expectations. Transformations may have introduced duplicates.
  std::set<std::string> expectations_set(expectations.begin(),
                                         expectations.end());

  // Remove an expectation that is sure to raise question marks with most users.
  // Positional parameters ("?") are only there for some specific client
  // libraries outside Google.
  expectations_set.erase("\"?\"");
  // When the user has entered another token, they don't intend to end the
  // statement, so giving that as an alternative option makes little sense.
  // Bison also tends to give it as the only option when there is an unexpected
  // token in the middle of an expression, even though there are other possible
  // continuations. This may be triggered by the fact that we use operator
  // precedence parsing.
  expectations_set.erase("$end");

  // Avoid suggesting pipe characters since the syntax might not be enabled,
  // and to avoid adding this on every error that looks for a ")" after a query.
  expectations_set.erase("|>");

  // Avoid suggesting FROM every place a query might show up.
  // This is usually a bad suggestion anyway.
  expectations_set.erase("keyword FROM");

  // Avoid suggesting OR everywhere an expression might show up.
  // OR is generally the lowest precedence operator and it appears separate in
  // the highest rule for `expression` in the grammar, whereas other operators
  // are grouped in a more specific rule, such as
  // `expression_with_prec_higher_than_and`. Consequently, when an unexpected
  // token is encountered after an `expression`, the expected set is a lot
  // smaller because all other operators are in more specific rules. In several
  // instances, the expected set is small enough so bison displays it, and OR
  // shows up (since an `expression` can continue with OR to form a larger
  // `expression`). However, there is nothing special about it to single it out
  // and suggest the user continue their expression with an OR any time there is
  // some syntax error.
  expectations_set.erase("keyword OR");

  // Removes the "+=" and "-=" from the expectations set if the language feature
  // is not enabled.
  if (!language_options.LanguageFeatureEnabled(
          FEATURE_ENABLE_ALTER_ARRAY_OPTIONS)) {
    expectations_set.erase("\"+=\"");
    expectations_set.erase("\"-=\"");
  }

  ZETASQL_RET_CHECK(zetasql::internal::HasPayloadWithType<InternalErrorLocation>(
      parse_status));
  InternalErrorLocation internal_error_location =
      zetasql::internal::GetPayload<InternalErrorLocation>(parse_status);
  ParseLocationPoint error_location =
      ParseLocationPoint::FromInternalErrorLocation(internal_error_location);

  // TODO: Make this conditional on the language features that are
  // enabled, and remove other elements from the expectations that are not
  // supported by the enabled language features.

  // Re-parse the input so that we can get the token at the error location. We
  // start the tokenizer in kTokenizer mode because we don't need to get a bogus
  // token at the start to indicate the statement type. That token interferes
  // with errors at offset 0.
  StackFrame::StackFrameFactory stack_frame_factory;
  ZETASQL_ASSIGN_OR_RETURN(
      auto tokenizer,
      LookaheadTransformer::Create(
          ParserMode::kTokenizerPreserveComments, error_location.filename(),
          input, start_offset, language_options, macro_expansion_mode,
          macro_catalog, arena, stack_frame_factory));
  ParseLocationRange token_location;
  Token token = Token::UNAVAILABLE;
  while (token != Token::EOI) {
    ParseLocationPoint last_token_location_end = token_location.end();
    ZETASQL_RETURN_IF_ERROR(tokenizer->GetNextToken(&token_location, &token));
    // Bison always returns parse errors at token boundaries, so this should
    // never happen.
    ZETASQL_RET_CHECK_GE(error_location.GetByteOffset(),
                 token_location.start().GetByteOffset());
    if (token == Token::EOI || error_location.GetByteOffset() ==
                                   token_location.start().GetByteOffset()) {
      const absl::string_view token_text = token_location.GetTextFrom(input);
      std::string actual_token_description;
      if (token == Token::EOI) {
        // The error location was at end-of-input, so this is an
        // unexpected-end-of error. Format with a better string, and move its
        // location to the end of the last token.
        actual_token_description =
            absl::StrCat("end of ", GetParserModeName(mode));
        if (last_token_location_end.IsValid()) {
          error_location = last_token_location_end;
        } else {
          // There was not even a comment, the input is just whitespace. Move
          // the error to skip it all.
          error_location = token_location.start();
          error_location.SetByteOffset(start_offset);
        }
      } else if (token == Token::KW_OVER) {
        // When the OVER keyword is used in the wrong place, we tell the user
        // exactly where it can be used.
        return MakeSqlErrorAtPoint(error_location)
               << "Syntax error: OVER keyword must follow a function call";
      } else if (const KeywordInfo* keyword_info =
                     GetKeywordInfoForToken(token)) {
        actual_token_description =
            absl::StrCat("keyword ", keyword_info->keyword());
      } else if (token == Token::STRING_LITERAL) {
        // Escape physical newlines, to avoid multi-line error messages. (Note
        // that this is technically incorrect for raw string literals.)
        std::string escaped_token_text = std::string(token_text);
        absl::StrReplaceAll({{"\r", "\\r"}}, &escaped_token_text);
        absl::StrReplaceAll({{"\n", "\\n"}}, &escaped_token_text);
        actual_token_description =
            absl::StrCat("string literal ",
                         ShortenStringLiteralForError(escaped_token_text));
      } else if (token == Token::BYTES_LITERAL) {
        // Escape physical newlines, to avoid multi-line error messages. (Note
        // that this is technically incorrect for raw bytes literals.)
        std::string escaped_token_text = std::string(token_text);
        absl::StrReplaceAll({{"\r", "\\r"}}, &escaped_token_text);
        absl::StrReplaceAll({{"\n", "\\n"}}, &escaped_token_text);
        actual_token_description = absl::StrCat(
            "bytes literal ", ShortenBytesLiteralForError(escaped_token_text));
      } else if (token == Token::INTEGER_LITERAL) {
        actual_token_description =
            absl::StrCat("integer literal \"", token_text, "\"");
      } else if (token == Token::FLOATING_POINT_LITERAL) {
        actual_token_description =
            absl::StrCat("floating point literal \"", token_text, "\"");
      } else if (token == Token::IDENTIFIER) {
        if (token_text[0] == '`') {
          // Don't put extra quotes around an already-backquoted identifier.
          actual_token_description = absl::StrCat("identifier ", token_text);
        } else {
          actual_token_description =
              absl::StrCat("identifier \"", token_text, "\"");
        }
      } else if (token == Token::SEMICOLON) {
        // The ";" token includes trailing whitespace, and we don't want to
        // echo that back.
        actual_token_description = "\";\"";
      } else if (token == Token::KW_OPEN_HINT ||
                 token == Token::KW_OPEN_INTEGER_HINT ||
                 token == Token::OPEN_INTEGER_PREFIX_HINT) {
        // This is a single token for "@{", but we want to expose this as "@"
        // externally.
        actual_token_description = "\"@\"";
      } else if (token == Token::KW_DOUBLE_AT) {
        actual_token_description = "\"@@\"";
      } else {
        actual_token_description = absl::StrCat("\"", token_text, "\"");
      }
      if (!expectations_set.empty()) {
        return MakeSqlErrorAtPoint(error_location)
               << "Syntax error: Expected "
               << absl::StrJoin(expectations_set, " or ") << " but got "
               << actual_token_description;
      }
      return MakeSqlErrorAtPoint(error_location)
             << "Syntax error: Unexpected " << actual_token_description;
    }
  }
  ABSL_LOG(ERROR) << "Syntax error location not found in input";
  return MakeSqlErrorAtPoint(error_location) << parse_status.message();
}

// Dispatch to the appropriate parser input method based on the mode.
static absl::Status ParseByMode(ParserMode mode, Lexer& lexer, Parser& parser) {
  switch (mode) {
    case ParserMode::kStatement:
      return parser.ParseSqlStatement(lexer);
    case ParserMode::kScript:
      return parser.ParseScript(lexer);
    case ParserMode::kNextStatement:
      return parser.ParseNextStatement(lexer);
    case ParserMode::kNextScriptStatement:
      return parser.ParseNextScriptStatement(lexer);
    case ParserMode::kNextStatementKind:
      return parser.ParseNextStatementKind(lexer);
    case ParserMode::kExpression:
      return parser.ParseStandaloneExpression(lexer);
    case ParserMode::kType:
      return parser.ParseStandaloneType(lexer);
    case ParserMode::kSubpipeline:
      return parser.ParseStandaloneSubpipeline(lexer);
    case ParserMode::kTokenizerPreserveComments:
    case ParserMode::kTokenizer:
    case ParserMode::kMacroBody:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected mode: " << static_cast<int>(mode);
  }
}

// Initializes fields on the nodes out of 'allocated_ast_nodes' and moves them
// out into 'other_allocated_ast_nodes', separating 'output_node' which goes
// into 'output'.
static absl::Status InitNodes(
    absl::string_view input, const ASTNode* output_node,
    std::vector<std::unique_ptr<ASTNode>>& allocated_ast_nodes,
    std::unique_ptr<ASTNode>* output,
    std::vector<std::unique_ptr<ASTNode>>& other_allocated_ast_nodes) {
  // Make sure InitFields() is called for all ASTNodes that were created.
  // We don't use the result of InitFields() in the grammar itself, so we
  // don't need to do this during parsing.
  for (const auto& ast_node : allocated_ast_nodes) {
    if (ast_node->Is<ASTMacroBody>()) {
      ASTMacroBody* macro_body = ast_node->GetAsOrNull<ASTMacroBody>();
      ZETASQL_RET_CHECK(macro_body != nullptr);
      ZETASQL_RET_CHECK(macro_body->location().IsValid());
      macro_body->set_image(
          std::string{macro_body->location().GetTextFrom(input)});
    }
    ZETASQL_RETURN_IF_ERROR(ast_node->InitFields());
  }

  if (output != nullptr && output_node != nullptr) {
    *output = nullptr;
    // Move 'output_node' out of 'allocated_ast_nodes' and into 'output'.
    for (int64_t i = allocated_ast_nodes.size() - 1; i >= 0; --i) {
      if ((allocated_ast_nodes)[i].get() == output_node) {
        *output = std::move(allocated_ast_nodes[i]);
        // There's no need to erase the entry in allocated_ast_nodes_.
        break;
      }
    }
    ZETASQL_RET_CHECK_EQ(output->get(), output_node);
  }
  other_allocated_ast_nodes = std::move(allocated_ast_nodes);
  return absl::OkStatus();
}

static absl::Status MaybeStripParserError(absl::Status status) {
  if (absl::IsInvalidArgument(status) &&
      absl::GetFlag(FLAGS_zetasql_parser_strip_errors)) {
    return absl::InvalidArgumentError("Syntax error");
  }
  return status;
}

absl::Status ParseInternal(
    ParserMode mode, absl::string_view filename, absl::string_view input,
    int start_byte_offset, IdStringPool* id_string_pool, zetasql_base::UnsafeArena* arena,
    const LanguageOptions& language_options,
    MacroExpansionMode macro_expansion_mode,
    const macros::MacroCatalog* macro_catalog, std::unique_ptr<ASTNode>* output,
    ParserRuntimeInfo& runtime_info, WarningSink& warning_sink,
    std::vector<std::unique_ptr<ASTNode>>* other_allocated_ast_nodes,
    ASTStatementProperties* ast_statement_properties,
    int* statement_end_byte_offset) {
  // The primary logic of the function is wrapped in a lambda to ensure all
  // return paths are handled by MaybeStripParserError while allowing natural
  // control flow within the function body.
  return MaybeStripParserError([&]() -> absl::Status {
    absl::Status parse_status;
    {  // Scope of the timer.
      auto parser_timer =
          MakeScopedTimerStarted(&runtime_info.parser_timed_value());

      Lexer lexer(mode, filename, input, start_byte_offset, language_options,
                  macro_expansion_mode, macro_catalog, arena);

      ASTNode* output_node = nullptr;
      ASTNodeFactory node_factory(arena, id_string_pool);
      Parser parser(lexer.tokenizer(), language_options, node_factory,
                    warning_sink, macro_expansion_mode, &output_node,
                    ast_statement_properties, statement_end_byte_offset);

      parse_status = ParseByMode(mode, lexer, parser);
      // We want to continue if there was a parsing error or a successful parse.
      // Anything else, such as kInternal or kResourceExhausted, should be
      // returned.
      if (!(parse_status.ok() || absl::IsInvalidArgument(parse_status))) {
        return parse_status;
      }

      // The tokenizer's error overrides the parser's error.
      ZETASQL_RETURN_IF_ERROR(lexer.tokenizer().GetOverrideError());

      runtime_info.add_lexical_tokens(lexer.tokenizer().num_lexical_tokens());
      // When a multi-statement input ends with a semicolon, ParseNext.* will
      // return `statement_end_byte_offset` at the semi-colon. What we want is
      // the end of the input so that we don't try to parse any trailing
      // whitespace as another statement.
      if (statement_end_byte_offset != nullptr && lexer.tokenizer().IsAtEoi()) {
        *statement_end_byte_offset = static_cast<int>(input.size());
      }

      std::vector<std::unique_ptr<ASTNode>> allocated_ast_nodes =
          std::move(node_factory).ReleaseAllocatedASTNodes();
      if (parse_status.ok()) {
        return InitNodes(input, output_node, allocated_ast_nodes, output,
                         *other_allocated_ast_nodes);
      }
    }  // End of the timer scope.

    // TODO: b/376552156 -- Remove re-parsing from error message generation then
    //                      let timer scope run to the end of the function.

    // Bison returns error messages that start with "syntax error, ". The parser
    // logic itself will return an empty error message if it wants to generate
    // a simple "Unexpected X" error.
    if (absl::StartsWith(parse_status.message(), "syntax error, ")) {
      // This was a Bison-generated syntax error. Generate a message that is to
      // our own liking.
      return GenerateImprovedBisonSyntaxError(
          language_options, parse_status, mode, input, start_byte_offset,
          macro_expansion_mode, macro_catalog, arena);
    }
    return parse_status;
  }());
}

}  // namespace parser
}  // namespace zetasql
