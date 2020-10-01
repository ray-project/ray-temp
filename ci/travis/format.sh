#!/usr/bin/env bash
# YAPF + Clang formatter (if installed). This script formats all changed files from the last mergebase.
# You are encouraged to run this locally before pushing changes for review.

# Cause the script to exit if a single command fails
set -euo pipefail

FLAKE8_VERSION_REQUIRED="3.7.7"
YAPF_VERSION_REQUIRED="0.23.0"
SHELLCHECK_VERSION_REQUIRED="0.7.1"
MYPY_VERSION_REQUIRED="0.782"

check_command_exist() {
    VERSION=""
    case "$1" in
        yapf)
            VERSION=$YAPF_VERSION_REQUIRED
            ;;
        flake8)
            VERSION=$FLAKE8_VERSION_REQUIRED
            ;;
        shellcheck)
            VERSION=$SHELLCHECK_VERSION_REQUIRED
            ;;
        mypy)
            VERSION=$MYPY_VERSION_REQUIRED
            ;;
        *)
            echo "$1 is not a required dependency"
            exit 1
    esac
    if ! [ -x "$(command -v "$1")" ]; then
        echo "$1 not installed. pip install $1==$VERSION"
        exit 1
    fi
}

check_command_exist yapf
check_command_exist flake8
check_command_exist mypy

ver=$(yapf --version)
if ! echo "$ver" | grep -q 0.23.0; then
    echo "Wrong YAPF version installed: 0.23.0 is required, not $ver. $YAPF_DOWNLOAD_COMMAND_MSG"
    exit 1
fi

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

FLAKE8_VERSION=$(flake8 --version | head -n 1 | awk '{print $1}')
YAPF_VERSION=$(yapf --version | awk '{print $2}')
SHELLCHECK_VERSION=$(shellcheck --version | awk '/^version:/ {print $2}')
MYPY_VERSION=$(mypy --version | awk '{print $2}')

# params: tool name, tool version, required version
tool_version_check() {
    if [ "$2" != "$3" ]; then
        echo "WARNING: Ray uses $1 $3, You currently are using $2. This might generate different results."
    fi
}

tool_version_check "flake8" "$FLAKE8_VERSION" "$FLAKE8_VERSION_REQUIRED"
tool_version_check "yapf" "$YAPF_VERSION" "$YAPF_VERSION_REQUIRED"
tool_version_check "shellcheck" "$SHELLCHECK_VERSION" "$SHELLCHECK_VERSION_REQUIRED"
tool_version_check "mypy" "$MYPY_VERSION" "$MYPY_VERSION_REQUIRED"

if which clang-format >/dev/null; then
  CLANG_FORMAT_VERSION=$(clang-format --version | awk '{print $3}')
  tool_version_check "clang-format" "$CLANG_FORMAT_VERSION" "7.0.0"
else
    echo "WARNING: clang-format is not installed!"
fi

if [[ $(flake8 --version) != *"flake8_quotes"* ]]; then
    echo "WARNING: Ray uses flake8 with flake8_quotes. Might error without it. Install with: pip install flake8-quotes"
fi

SHELLCHECK_FLAGS=(
  --exclude=1090  # "Can't follow non-constant source. Use a directive to specify location."
  --exclude=1091  # "Not following {file} due to some error"
  --exclude=2207  # "Prefer mapfile or read -a to split command output (or quote to avoid splitting)." -- these aren't compatible with macOS's old Bash
)

YAPF_FLAGS=(
    '--style' "$ROOT/.style.yapf"
    '--recursive'
    '--parallel'
)

# TODO(dmitri): When more of the codebase is typed properly, the mypy flags
# should be set to do a more stringent check. 
MYPY_FLAGS=(
    '--follow-imports=skip'
)

YAPF_EXCLUDES=(
    '--exclude' 'python/ray/cloudpickle/*'
    '--exclude' 'python/build/*'
    '--exclude' 'python/ray/core/src/ray/gcs/*'
    '--exclude' 'python/ray/thirdparty_files/*'
)

GIT_LS_EXCLUDES=(
  ':(exclude)python/ray/cloudpickle/'
)

# TODO(barakmich): This should be cleaned up. I've at least excised the copies
# of these arguments to this location, but the long-term answer is to actually
# make a flake8 config file
FLAKE8_EXCLUDE="--exclude=python/ray/core/generated/,streaming/python/generated,doc/source/conf.py,python/ray/cloudpickle/,python/ray/thirdparty_files/,python/build/,python/.eggs/"
FLAKE8_IGNORES="--ignore=C408,E121,E123,E126,E226,E24,E704,W503,W504,W605"
FLAKE8_PYX_IGNORES="--ignore=C408,E121,E123,E126,E211,E225,E226,E227,E24,E704,E999,W503,W504,W605"

shellcheck_scripts() {
  shellcheck "${SHELLCHECK_FLAGS[@]}" "$@"
}

# Runs mypy on each argument in sequence. This is different than running mypy 
# once on the list of arguments.
mypy_on_each() {
    for file in "$@"; do
       echo "Running mypy on $file"
       mypy ${MYPY_FLAGS[@]+"${MYPY_FLAGS[@]}"} "$file"
    done
}

# Runs mypy in sequence on each changed python file that differs from the 
# master branch. Currently invoked in format_changed AND format_all.
mypy_on_changed() {
    # The hideous line starting "IFS" loads the newline-separated
    # changed_py_file_string into the array changed_py_file_array in a way
    # compatible with macOS's old Bash.
    local changed_py_file_string
    local changed_py_file_array
    MERGEBASE="$(git merge-base upstream/master HEAD)"
    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.py' &>/dev/null; then
        changed_py_file_string="$(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py')"
        IFS=$'\n' read -rd '' -a changed_py_file_array <<< "$changed_py_file_string" || true
        echo "Running mypy on changed python files"
        mypy_on_each "${changed_py_file_array[@]}"
    fi
}

# Format specified files
format_files() {
    local shell_files=() python_files=() bazel_files=()

    local name
    for name in "$@"; do
      local base="${name%.*}"
      local suffix="${name#${base}}"

      local shebang=""
      read -r shebang < "${name}" || true
      case "${shebang}" in
        '#!'*)
          shebang="${shebang#/usr/bin/env }"
          shebang="${shebang%% *}"
          shebang="${shebang##*/}"
          ;;
      esac

      if [ "${base}" = "WORKSPACE" ] || [ "${base}" = "BUILD" ] || [ "${suffix}" = ".BUILD" ] || [ "${suffix}" = ".bazel" ] || [ "${suffix}" = ".bzl" ]; then
        bazel_files+=("${name}")
      elif [ -z "${suffix}" ] && [ "${shebang}" != "${shebang#python}" ] || [ "${suffix}" != "${suffix#.py}" ]; then
        python_files+=("${name}")
      elif [ -z "${suffix}" ] && [ "${shebang}" != "${shebang%sh}" ] || [ "${suffix}" != "${suffix#.sh}" ]; then
        shell_files+=("${name}")
      else
        echo "error: failed to determine file type: ${name}" 1>&2
        return 1
      fi
    done

    if [ 0 -lt "${#python_files[@]}" ]; then
      yapf --in-place "${YAPF_FLAGS[@]}" -- "${python_files[@]}"
      echo "Running mypy on provided python files:"
      mypy_on_each "${python_files[@]}" 
    fi

    if shellcheck --shell=sh --format=diff - < /dev/null; then
      if [ 0 -lt "${#shell_files[@]}" ]; then
        local difference
        difference="$(shellcheck_scripts --format=diff "${shell_files[@]}" || true && printf "-")"
        difference="${difference%-}"
        printf "%s" "${difference}" | patch -p1
      fi
    else
      echo "error: this version of shellcheck does not support diffs"
    fi
}

# Format all files, and print the diff to stdout for travis.
# For now, mypy only runs on changed python files.
format_all() {
    command -v flake8 &> /dev/null;
    HAS_FLAKE8=$?

    echo "$(date)" "YAPF...."
    git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 10 \
      yapf --in-place "${YAPF_EXCLUDES[@]}" "${YAPF_FLAGS[@]}"
    echo "$(date)" "MYPY...."
    mypy_on_changed
    if [ $HAS_FLAKE8 ]; then
      echo "$(date)" "Flake8...."
      git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 \
        flake8 --inline-quotes '"' --no-avoid-escape  "$FLAKE8_EXCLUDE" "$FLAKE8_IGNORES"

      git ls-files -- '*.pyx' '*.pxd' '*.pxi' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 \
        flake8 --inline-quotes '"' --no-avoid-escape "$FLAKE8_EXCLUDE" "$FLAKE8_PYX_IGNORES"
    fi

    echo "$(date)" "clang-format...."
    if command -v clang-format >/dev/null; then
      git ls-files -- '*.cc' '*.h' '*.proto' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 clang-format -i
    fi

    if command -v shellcheck >/dev/null; then
      local shell_files non_shell_files
      non_shell_files=($(git ls-files -- ':(exclude)*.sh'))
      shell_files=($(git ls-files -- '*.sh'))
      if [ 0 -lt "${#non_shell_files[@]}" ]; then
        shell_files+=($(git --no-pager grep -l -- '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' "${non_shell_files[@]}" || true))
      fi
      if [ 0 -lt "${#shell_files[@]}" ]; then
        echo "$(date)" "shellcheck scripts...."
        shellcheck_scripts "${shell_files[@]}"
      fi
    fi
    echo "$(date)" "done!"
}

# Format files that differ from main branch. Ignores dirs that are not slated
# for autoformat yet.
format_changed() {
    # The `if` guard ensures that the list of filenames is not empty, which
    # could cause yapf to receive 0 positional arguments, making it hang
    # waiting for STDIN.
    #
    # `diff-filter=ACRM` and $MERGEBASE is to ensure we only format files that
    # exist on both branches.
    mypy_on_changed

    MERGEBASE="$(git merge-base upstream/master HEAD)"

    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.py' &>/dev/null; then
        git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
             yapf --in-place "${YAPF_EXCLUDES[@]}" "${YAPF_FLAGS[@]}"
        echo "Running mypy on changed python files:"
        git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
             mypy ${MYPY_FLAGS[@]+"${MYPY_FLAGS[@]}"} 
        if which flake8 >/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
                 flake8 --inline-quotes '"' --no-avoid-escape "$FLAKE8_EXCLUDE" "$FLAKE8_IGNORES"
        fi
    fi

    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.pyx' '*.pxd' '*.pxi' &>/dev/null; then
        if which flake8 >/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.pyx' '*.pxd' '*.pxi' | xargs -P 5 \
                 flake8 --inline-quotes '"' --no-avoid-escape "$FLAKE8_EXCLUDE" "$FLAKE8_PYX_IGNORES"
        fi
    fi

    if which clang-format >/dev/null; then
        if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.cc' '*.h' &>/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.cc' '*.h' | xargs -P 5 \
                 clang-format -i
        fi
    fi

    if command -v shellcheck >/dev/null; then
        local shell_files non_shell_files
        non_shell_files=($(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- ':(exclude)*.sh'))
        shell_files=($(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.sh'))
        if [ 0 -lt "${#non_shell_files[@]}" ]; then
            shell_files+=($(git --no-pager grep -l -- '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' "${non_shell_files[@]}" || true))
        fi
        if [ 0 -lt "${#shell_files[@]}" ]; then
            shellcheck_scripts "${shell_files[@]}"
        fi
    fi
}


# This flag formats individual files. --files *must* be the first command line
# arg to use this option.
if [ "${1-}" == '--files' ]; then
    format_files "${@:2}"
    # If `--all` is passed, then any further arguments are ignored and the
    # entire python directory is formatted.
elif [ "${1-}" == '--all' ]; then
    format_all "${@}"
    if [ -n "${FORMAT_SH_PRINT_DIFF-}" ]; then git --no-pager diff; fi
else
    # Add the upstream remote if it doesn't exist
    if ! git remote -v | grep -q upstream; then
        git remote add 'upstream' 'https://github.com/ray-project/ray.git'
    fi

    # Only fetch master since that's the branch we're diffing against.
    git fetch upstream master || true

    # Format only the files that changed in last commit.
    format_changed
fi

# Ensure import ordering
# Make sure that for every import psutil; import setproctitle
# There's a import ray above it.

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python}

$PYTHON_EXECUTABLE ci/travis/check_import_order.py . -s ci -s python/ray/thirdparty_files -s python/build -s lib

if ! git diff --quiet &>/dev/null; then
    echo 'Reformatted changed files. Please review and stage the changes.'
    echo 'Files updated:'
    echo

    git --no-pager diff --name-only

    exit 1
fi
