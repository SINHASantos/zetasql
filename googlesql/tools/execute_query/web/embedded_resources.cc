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

#include "googlesql/tools/execute_query/web/embedded_resources.h"

#include <string>

#include "googlesql/tools/execute_query/web/init.js.h"
#include "googlesql/tools/execute_query/web/local_storage_manager.js.h"
#include "googlesql/tools/execute_query/web/page_body.html.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "googlesql/tools/execute_query/web/page_template.html.h"
#include "googlesql/tools/execute_query/web/rewrite_steps.js.h"
#include "googlesql/tools/execute_query/web/splitter.js.h"
#include "googlesql/tools/execute_query/web/style.css.h"
#include "googlesql/tools/execute_query/web/table.html.h"
#include "googlesql/tools/execute_query/web/theme.js.h"

namespace googlesql {

const QueryWebTemplates& QueryWebTemplates::Default() {
  static const QueryWebTemplates* default_templates = new QueryWebTemplates();
  return *default_templates;
}

QueryWebTemplates::QueryWebTemplates()
    : page_template_(embedded_resources::kPageTemplate),
      style_css_(embedded_resources::kStyleCSS),
      page_body_(embedded_resources::kPageBody),
      table_(embedded_resources::kTable),
      inline_js_(absl::StrJoin(
          {
              embedded_resources::kLocalStorageManagerJS,
              embedded_resources::kThemeJS,
              embedded_resources::kSplitterJS,
              embedded_resources::kRewriteStepsJS,
              embedded_resources::kInitJS,
          },
          "\n")) {}

const std::string& QueryWebTemplates::GetWebPageContents() const {
  return page_template_;
}

const std::string& QueryWebTemplates::GetWebPageCSS() const {
  return style_css_;
}

const std::string& QueryWebTemplates::GetWebPageBody() const {
  return page_body_;
}

const std::string& QueryWebTemplates::GetTable() const { return table_; }

const std::string& QueryWebTemplates::GetInlineJS() const { return inline_js_; }

}  // namespace googlesql
