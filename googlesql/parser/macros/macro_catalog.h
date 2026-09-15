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

#ifndef GOOGLESQL_PARSER_MACROS_MACRO_CATALOG_H_
#define GOOGLESQL_PARSER_MACROS_MACRO_CATALOG_H_

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "googlesql/public/catalog.h"
#include "googlesql/public/parse_location.h"
#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/node_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace googlesql {
namespace parser {
namespace macros {

// Keep this cheap to copy.
class MacroInfo : public Macro {
 public:
  MacroInfo() = default;
  MacroInfo(absl::string_view source_text, ParseLocationRange location,
            ParseLocationRange name_location, ParseLocationRange body_location,
            int definition_start_offset = 0, int definition_start_line = 1,
            int definition_start_column = 1)
      : source_text_(source_text),
        location_(location),
        name_location_(name_location),
        body_location_(body_location),
        definition_start_offset_(definition_start_offset),
        definition_start_line_(definition_start_line),
        definition_start_column_(definition_start_column) {}

  MacroInfo(const MacroInfo&) = default;
  MacroInfo& operator=(const MacroInfo&) = default;
  MacroInfo(MacroInfo&&) = default;
  MacroInfo& operator=(MacroInfo&&) = default;
  ~MacroInfo() override = default;

  // Returns the name of this macro.
  absl::string_view name() const {
    return name_location_.GetTextFrom(source_text_);
  }

  // Returns the body of this macro.
  absl::string_view body() const override {
    return body_location_.GetTextFrom(source_text_);
  }

  absl::string_view Name() const override { return name(); }
  absl::string_view FullName() const override { return name(); }

  absl::string_view source_text() const override { return source_text_; }
  ParseLocationRange location() const override { return location_; }
  ParseLocationRange name_location() const override { return name_location_; }
  ParseLocationRange body_location() const override { return body_location_; }
  int definition_start_offset() const override {
    return definition_start_offset_;
  }
  int definition_start_line() const override { return definition_start_line_; }
  int definition_start_column() const override {
    return definition_start_column_;
  }

  friend bool operator==(const MacroInfo& lhs, const MacroInfo& rhs) {
    return lhs.source_text_ == rhs.source_text_ &&
           lhs.location_ == rhs.location_ &&
           lhs.name_location_ == rhs.name_location_ &&
           lhs.body_location_ == rhs.body_location_ &&
           lhs.definition_start_offset_ == rhs.definition_start_offset_ &&
           lhs.definition_start_line_ == rhs.definition_start_line_ &&
           lhs.definition_start_column_ == rhs.definition_start_column_;
  }

 private:
  // The contents of the source where this macro was defined. This is needed
  // when printing error messages to show the definition in its context.
  absl::string_view source_text_;

  // Location of the macro definition, starting from the DEFINE keyword.
  ParseLocationRange location_;

  // Location of the macro name.
  ParseLocationRange name_location_;

  // Location of the start of the macro body.
  ParseLocationRange body_location_;

  // The offset of the macro definition in its file. This is important to
  // to decide whether tokens were originally adjacent or not. See b/389149112.
  int definition_start_offset_ = 0;

  // Optional line and column where the macro definition starts. Useful when
  // the full original input source is unavailable, and `source_text_` contains
  // only the definition source, to report accurate locations.
  // Note that these are not themselves the offsets, but rather the actual
  // line and column in the original input source, 1-based.
  // The offsets are simply computed by subtracting 1.
  int definition_start_line_ = 1;
  int definition_start_column_ = 1;
};

// Keep this cheap to copy.
struct MacroCatalogOptions {
  // If enabled, allows overwriting existing macro definitions in the catalog
  // when registering a macro with an existing name.
  bool allow_overwrite = false;
};

// Represents the catalog of existing macros and their definitions.
// This will likely develop into an interface for more sophisticated catalog in
// the future, like catalog.h, with multi-part paths.
class MacroCatalog : public googlesql::Catalog {
 public:
  std::string FullName() const override { return "MacroCatalog"; }

  // Returns a statically allocated empty macro catalog.
  static const MacroCatalog& EmptyMacroCatalog() {
    static const absl::NoDestructor<MacroCatalog> kEmptyMacroCatalog;
    return *kEmptyMacroCatalog;
  }

  explicit MacroCatalog(MacroCatalogOptions options = {})
      : options_(options),
        macros_(std::make_shared<
                absl::node_hash_map<std::string, std::map<int, MacroInfo>>>()) {
  }

  // Registers the given macro. Returns an error if it fails, i.e. because a
  // macro with this name already exists and `allow_overwrite_` is not enabled.
  absl::Status RegisterMacro(MacroInfo macro_info);

  // Returns pointer to MacroInfo for the given name, or nullptr if not found.
  const MacroInfo* /*absl_nullable*/ FindPtr(absl::string_view macro_name) const;

  // Returns the MacroInfo for the given name, or nullopt if not found.
  std::optional<MacroInfo> Find(absl::string_view macro_name) const;

  // In case of a macro redefinition, returns a new version of the catalog
  // starting from the given version_id.
  std::unique_ptr<MacroCatalog> NewVersion();

  using Catalog::GetMacro;
  absl::Status GetMacro(const std::string& name, const Macro** macro,
                        const Catalog::FindOptions& options) override;

 private:
  // Make the copy constructor private so that the class is not externally
  // copyable. This is to avoid any inconsistent shared state between the
  // different versions due to changes made to copies.
  MacroCatalog(const MacroCatalog& other) = default;

  // The version at and after which this catalog is valid.
  int version_id_ = 0;

  MacroCatalogOptions options_;

  // Uses node_hash_map<> for pointer stability. We use a shared pointer for
  // shared ownership across different versions of the catalog.
  std::shared_ptr<absl::node_hash_map<std::string, std::map<int, MacroInfo>>>
      macros_ = nullptr;
};

}  // namespace macros
}  // namespace parser
}  // namespace googlesql

#endif  // GOOGLESQL_PARSER_MACROS_MACRO_CATALOG_H_
