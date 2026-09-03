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

#include "googlesql/parser/textmapper_lexer_adapter.h"

#include <utility>

#include "googlesql/parser/tm_token.h"
#include "googlesql/parser/token_stream.h"
#include "absl/base/attributes.h"
#include "absl/status/statusor.h"

namespace googlesql::parser {

TextMapperLexerAdapter::TextMapperLexerAdapter(TokenStream* input)
    : input_(input) {}

[[nodiscard]] Token TextMapperLexerAdapter::Next() {
  auto status_or_token = input_->GetNextToken();
  if (!status_or_token.ok()) {
    // The TextMapper lexer interface requires returning a Token enum. Returning
    // INVALID_TOKEN signals to the TextMapper parser that an error occurred.
    // The detailed error status is preserved within the TokenStream, which the
    // calling code must check after a parsing error.
    return Token::INVALID_TOKEN;
  }
  last_token_ = std::move(*status_or_token);
  return last_token_.kind;
}

}  // namespace googlesql::parser
