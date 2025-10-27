#!/usr/bin/env bash

# Runs static analysis tools locally and saves reports to
# build/static-analysis/*.txt. Tools are optional; missing tools
# will be reported but won't abort the whole script.

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
BUILD_DIR="${REPO_ROOT}/build"
REPORT_DIR="${BUILD_DIR}/static-analysis"

mkdir -p "${REPORT_DIR}"

echof() { printf "%s\n" "$*"; }

tool_exists() {
  command -v "$1" >/dev/null 2>&1
}

detect_macos_sdkroot() {
  if [[ "$(uname -s)" == "Darwin" ]] && tool_exists xcrun; then
    xcrun --show-sdk-path 2>/dev/null || true
  fi
}

maybe_add_llvm_to_path() {
  if [[ "$(uname -s)" == "Darwin" ]] && tool_exists brew; then
    local llvm_prefix
    llvm_prefix=$(brew --prefix llvm 2>/dev/null || true)
    if [[ -n "${llvm_prefix}" && -d "${llvm_prefix}/bin" ]]; then
      PATH="${llvm_prefix}/bin:${PATH}"
      export PATH
    fi
  fi
}

write_header() {
  local f="$1"; shift
  printf "# %s\n# Run at: %s\n# CWD: %s\n\n" "$*" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "${REPO_ROOT}" >"${f}"
}

echo "Configuring CMake (generates compile_commands.json)..."
write_header "${REPORT_DIR}/cmake-config.txt" "cmake configure"
cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON >>"${REPORT_DIR}/cmake-config.txt" 2>&1 || true

echo "Building project (best-effort to populate compile_commands.json)..."
write_header "${REPORT_DIR}/build.txt" "cmake build"
# Build but don't fail the whole script if build fails; clang-tidy still can use compile_commands.json
cmake --build "${BUILD_DIR}" -j 2 >>"${REPORT_DIR}/build.txt" 2>&1 || echo "Build failed; continuing to run static checks (compile_commands.json may be missing)" >>"${REPORT_DIR}/build.txt"

# Run tests and store output
TEST_OUT="${REPORT_DIR}/tests-report.txt"
write_header "${TEST_OUT}" "ctest"
if [[ -d "${BUILD_DIR}" ]]; then
  if tool_exists ctest; then
    echo "Running ctest..." >>"${TEST_OUT}"
    cmake -E chdir "${BUILD_DIR}" ctest --output-on-failure >>"${TEST_OUT}" 2>&1 || echo "Tests failed; see above for details" >>"${TEST_OUT}"
  else
    echo "ctest not found; install CMake tools (ctest ships with CMake)" >>"${TEST_OUT}"
  fi
else
  echo "Build directory ${BUILD_DIR} missing; skipping tests" >>"${TEST_OUT}"
fi

# Find source files tracked by git (excluding tests/)
SRC_FILES=$(git -C "${REPO_ROOT}" ls-files '*.c' '*.cc' '*.cxx' '*.cpp' '*.c++' '*.h' '*.hh' '*.hpp' '*.hxx' | grep -v '^tests/' || true)
TUS=$(git -C "${REPO_ROOT}" ls-files '*.c' '*.cc' '*.cxx' '*.cpp' '*.c++' | grep -v '^tests/' || true)

########################################
# cpplint
########################################
CPPLINT_OUT="${REPORT_DIR}/cpplint-report.txt"
write_header "${CPPLINT_OUT}" "cpplint"
if tool_exists python3 && python3 -m pip show cpplint >/dev/null 2>&1; then
  if [[ -n "${SRC_FILES}" ]]; then
    echo "Running cpplint..." >>"${CPPLINT_OUT}"
    git -C "${REPO_ROOT}" ls-files '*.c' '*.cc' '*.cxx' '*.cpp' '*.c++' '*.h' '*.hh' '*.hpp' '*.hxx' | \
      grep -v '^tests/' | \
      tr '\n' '\0' | \
      xargs -0 -r python3 -m cpplint >>"${CPPLINT_OUT}" 2>&1 || true
  else
    echo "No source files found for cpplint." >>"${CPPLINT_OUT}"
  fi
else
  echo "cpplint (python module) not found; install with 'pip install cpplint'" >>"${CPPLINT_OUT}"
fi

########################################
# cppcheck
########################################
CPPCHECK_OUT="${REPORT_DIR}/cppcheck-report.txt"
write_header "${CPPCHECK_OUT}" "cppcheck"
if tool_exists cppcheck; then
  if [[ -f "${BUILD_DIR}/compile_commands.json" ]]; then
    echo "Running cppcheck with compile_commands.json..." >>"${CPPCHECK_OUT}"
    cppcheck --project="${BUILD_DIR}/compile_commands.json" \
      --enable=warning,style,performance,portability \
      --inline-suppr --std=c++17 --suppress=missingIncludeSystem >>"${CPPCHECK_OUT}" 2>&1 || true
  else
    echo "compile_commands.json not found; running cppcheck on src/" >>"${CPPCHECK_OUT}"
    cppcheck --enable=warning,style,performance,portability --inline-suppr --std=c++17 --suppress=missingIncludeSystem "${REPO_ROOT}/src" >>"${CPPCHECK_OUT}" 2>&1 || true
  fi
else
  echo "cppcheck not found; install it (brew install cppcheck)" >>"${CPPCHECK_OUT}"
fi

########################################
# clang-tidy
########################################
CLANG_TIDY_OUT="${REPORT_DIR}/clang-tidy-report.txt"
write_header "${CLANG_TIDY_OUT}" "clang-tidy"
maybe_add_llvm_to_path
if tool_exists clang-tidy; then
  if [[ -f "${BUILD_DIR}/compile_commands.json" && -n "${TUS}" ]]; then
    echo "Running clang-tidy over translation units..." >>"${CLANG_TIDY_OUT}"
    CLANG_TIDY_ARGS=(--warnings-as-errors=* -quiet -p "${BUILD_DIR}")
    SDKROOT_PATH=$(detect_macos_sdkroot)
    if [[ -n "${SDKROOT_PATH}" ]]; then
      export SDKROOT="${SDKROOT_PATH}"
      echo "Detected macOS SDK at ${SDKROOT_PATH}; exporting SDKROOT for clang-tidy" >>"${CLANG_TIDY_OUT}"
    fi
    while IFS= read -r -d '' tu; do
      echo "clang-tidy: ${tu}" >>"${CLANG_TIDY_OUT}"
      clang-tidy "${CLANG_TIDY_ARGS[@]}" "${tu}" >>"${CLANG_TIDY_OUT}" 2>&1 || true
    done < <(git -C "${REPO_ROOT}" ls-files '*.c' '*.cc' '*.cxx' '*.cpp' '*.c++' | grep -v '^tests/' | tr '\n' '\0' || true)
  else
    echo "clang-tidy skipped: compile_commands.json or translation units missing" >>"${CLANG_TIDY_OUT}"
  fi
else
  echo "clang-tidy not found; install it (brew install llvm)" >>"${CLANG_TIDY_OUT}"
fi

########################################
# clang-format check
########################################
CLANG_FORMAT_OUT="${REPORT_DIR}/clang-format-report.txt"
write_header "${CLANG_FORMAT_OUT}" "clang-format"
if tool_exists clang-format; then
  echo "Checking formatting..." >>"${CLANG_FORMAT_OUT}"
  # Prefer --dry-run --Werror if available
  if clang-format --help 2>&1 | grep -q -- "--dry-run"; then
    git -C "${REPO_ROOT}" ls-files '*.c' '*.cc' '*.cxx' '*.cpp' '*.c++' '*.h' '*.hh' '*.hpp' '*.hxx' | \
      grep -v '^tests/' | \
      tr '\n' '\0' | \
      xargs -0 -r -n1 sh -c 'clang-format --dry-run --Werror "$0" 2>&1 || echo "FORMAT_ISSUE: $0"' >>"${CLANG_FORMAT_OUT}" 2>&1 || true
  else
    # Fallback: diff formatted output
    git -C "${REPO_ROOT}" ls-files '*.c' '*.cc' '*.cxx' '*.cpp' '*.c++' '*.h' '*.hh' '*.hpp' '*.hxx' | \
      grep -v '^tests/' | \
      tr '\n' '\0' | \
      xargs -0 -r -n1 sh -c 'f="$0"; clang-format "$f" | diff -u "$f" - 2>/dev/null || echo "FORMAT_ISSUE: $f"' >>"${CLANG_FORMAT_OUT}" 2>&1 || true
  fi
else
  echo "clang-format not found; install it (brew install clang-format or llvm)" >>"${CLANG_FORMAT_OUT}"
fi

echo "Static analysis reports written to: ${REPORT_DIR}"
echo "Reports:"
ls -1 "${REPORT_DIR}"

echo "Run 'cat ${REPORT_DIR}/<tool>-report.txt' to inspect individual reports."

# mark todo completed
python3 - <<'PY'
print('')
PY
