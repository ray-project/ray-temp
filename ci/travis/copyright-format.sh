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

FILE_LIST_TMP_FILE=".cr_file_list_tmp"

TMP_FILE=".cr_tmp"

CPP_FILES=(cpp src)

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
    find "$directory" ! -name "$(printf "*\n*")" -name '*.cc' -or -name '*.h' > "$FILE_LIST_TMP_FILE"
    while IFS=$'\n' read -r f
    do
        head_content=$(sed -n "1,${LINES_NUM}p" "$f")
        if [[ "$head_content" != "$COPYRIGHT" ]];then
            ERROR_FILES+=("$f")
            if [[ "$RUN_TYPE" == "fix" ]];then
                sed -i '1s/^/\n/' "$f"
                cat "$COPYRIGHT_FILE" "$f" > $TMP_FILE
                mv $TMP_FILE "$f"
            fi
        fi
    done < $"$FILE_LIST_TMP_FILE"
    rm -f "$FILE_LIST_TMP_FILE"
done

if [[ ${#ERROR_FILES[*]} -gt 0 ]];then
    if [[ "$RUN_TYPE" == "fix" ]];then
        echo 'Copyright has been added to the files below:'
        printf '%s\n' "${ERROR_FILES[@]}"
        exit 0
    else
        echo 'Please add copyright at the biginning of the files below:'
        printf '%s\n' "${ERROR_FILES[@]}"
        exit 1
    fi
else
    echo 'Copyright check succeeded.'
    exit 0
fi
