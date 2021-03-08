#!/usr/bin/env bash

set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function usage()
{
  echo "Usage: copyright-format.sh [<args>]"
  echo
  echo "Options:"
  echo "  -h|--help               print the help info"
  echo "  -c|--check              check whether there are format issues in C++ files"
  echo "  -f|--fix                fix all the format issue directly"
  echo
}

pushd "$ROOT_DIR"/../..

COPYRIGHT_FILE="$ROOT_DIR"/copyright.txt

COPYRIGHT=$(cat "$COPYRIGHT_FILE")

LINES_NUM=$(echo "$COPYRIGHT" | wc -l)

RUN_TYPE="diff"

FILE_LIST_TMP_FILE="/tmp/.cr_file_list_tmp"

TMP_FILE="/tmp/.cr_tmp"

CPP_FILES=(
    src
    cpp
)

EXCLUDES_DIRS=(
    src/ray/object_manager/plasma/
    src/ray/thirdparty
)

ERROR_FILES=()

# Parse options
while [ $# -gt 0 ]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -c|--check)
      RUN_TYPE="diff"
      ;;
    -f|--fix)
      RUN_TYPE="fix"
      ;;
    *)
      echo "ERROR: unknown option \"$key\""
      echo
      usage
      exit 1
      ;;
  esac
  shift
done

for directory in "${CPP_FILES[@]}"; do
    cmd_args="find $directory -type f"
    for excluded in "${EXCLUDES_DIRS[@]}"; do
        cmd_args="${cmd_args} ! -path " 
        cmd_args="${cmd_args} '${excluded}"
        if [[ "${excluded: -1}" != "/" ]];then
            cmd_args="${cmd_args}/"
        fi
        cmd_args="${cmd_args}*'"
    done
    cmd_args="${cmd_args} \( -name '*.cc' -or -name '*.h' \)"
    eval "${cmd_args}" > "$FILE_LIST_TMP_FILE"
    while IFS=$'\n' read -r f
    do
        head_content=$(sed -n "1,${LINES_NUM}p" "$f")
        if [[ "$head_content" != "$COPYRIGHT" ]];then
            ERROR_FILES+=("$f")
            if [[ "$RUN_TYPE" == "fix" ]];then
                sed '1s/^/\n/' "$f" > $TMP_FILE
                mv $TMP_FILE "$f"
                cat "$COPYRIGHT_FILE" "$f" > $TMP_FILE
                mv $TMP_FILE "$f"
            fi
        fi
    done < $"$FILE_LIST_TMP_FILE"
    rm -f "$FILE_LIST_TMP_FILE"
done

if [[ ${#ERROR_FILES[*]} -gt 0 ]];then
    if [[ "$RUN_TYPE" == "fix" ]];then
        echo "Copyright has been added to the files below:"
        printf '%s\n' "${ERROR_FILES[@]}"
        exit 0
    else
        echo "Missing copyright info at the beginning of below files. Please run 'sh ci/travis/copyright-format.sh -f' to fix them:"
        printf '%s\n' "${ERROR_FILES[@]}"
        exit 1
    fi
else
    echo 'Copyright check succeeded.'
    exit 0
fi
