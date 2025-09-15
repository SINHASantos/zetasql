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

#include "zetasql/parser/textmapper_lexer_adapter.h"

#include <algorithm>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/parser/lookahead_transformer.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/parser_mode.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/public/language_options.h"
#include "absl/base/attributes.h"
#include "zetasql/base/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql::parser {

TextMapperLexerAdapter::TextMapperLexerAdapter(
    ParserMode mode, absl::string_view filename, absl::string_view input,
    int start_offset, const LanguageOptions& language_options,
    MacroExpansionMode macro_expansion_mode,
    const macros::MacroCatalog* macro_catalog, zetasql_base::UnsafeArena* arena) {
  absl::StatusOr<std::unique_ptr<LookaheadTransformer>> lexer =
      LookaheadTransformer::Create(mode, filename, input, start_offset,
                                   language_options, macro_expansion_mode,
                                   macro_catalog, arena, stack_frame_factory_);
  ZETASQL_DCHECK_OK(lexer.status());
  tokenizer_ = std::shared_ptr<LookaheadTransformer>(std::move(*lexer));
  tokenizer_->watching_lexers_.push_back(this);
}

TextMapperLexerAdapter::TextMapperLexerAdapter(
    const TextMapperLexerAdapter& other)
    : queued_tokens_(other.queued_tokens_), tokenizer_(other.tokenizer_) {
  tokenizer_->watching_lexers_.push_back(this);
}

TextMapperLexerAdapter::~TextMapperLexerAdapter() {
  tokenizer_->watching_lexers_.erase(
      std::find(tokenizer_->watching_lexers_.begin(),
                tokenizer_->watching_lexers_.end(), this));
}

ABSL_MUST_USE_RESULT Token TextMapperLexerAdapter::Next() {
  if (!queued_tokens_.empty()) {
    last_token_ = queued_tokens_.front();
    queued_tokens_.pop_front();
    return last_token_.kind;
  }

  last_token_.kind =
      tokenizer_->GetNextToken(&last_token_.text, &last_token_.location);

  for (TextMapperLexerAdapter* stub : tokenizer_->watching_lexers_) {
    if (stub != this) {
      stub->queued_tokens_.push_back(last_token_);
    }
  }

  return last_token_.kind;
}

}  // namespace zetasql::parser
