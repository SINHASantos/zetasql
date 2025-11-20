# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash
set -e
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

REAL_BINARY="${SCRIPT_DIR}/execute_query_bin"

# The runfiles directory is expected to be alongside this script.
export RUNFILES_DIR="${SCRIPT_DIR}/execute_query_bin.runfiles"

# Execute the actual binary, passing along all command-line arguments.
exec "${REAL_BINARY}" "$@"
