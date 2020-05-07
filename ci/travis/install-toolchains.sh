#!/usr/bin/env bash

set -euxo pipefail

LLVM_VERSION="9.0.0"

install_toolchains() {
  local osversion="" url="" urlbase="https://releases.llvm.org" targetdir="/usr/local"
  case "${OSTYPE}" in
    msys)
      osversion=win
      if [ "${HOSTTYPE}" != "${HOSTTYPE%64}" ]; then
        osversion="${osversion}64"
      else
        osversion="${osversion}32"
      fi
      url="${urlbase}/${LLVM_VERSION}/LLVM-${LLVM_VERSION}-${osversion}.exe"
      ;;
    linux-gnu)
      osversion="${OSTYPE}-$(sed -n -e '/^PRETTY_NAME/ { s/^[^=]*="\(.*\)"/\1/g; s/ /-/; s/\([0-9]*\.[0-9]*\)\.[0-9]*/\1/; s/ .*//; p }' /etc/os-release | tr '[:upper:]' '[:lower:]')"
      ;;
    darwin*)
      osversion="darwin-apple"
      ;;
  esac
  if [ -z "${url}" ]; then
    url="${urlbase}/${LLVM_VERSION}/clang+llvm-${LLVM_VERSION}-${HOSTTYPE}-${osversion}.tar.xz"
  fi
  curl -f -s -L -R "${url}" | if [ "${OSTYPE}" = "msys" ]; then
    local target="./${url##*/}"
    install /dev/stdin "${target}"
    mkdir -p -- "${targetdir}"
    7z x -bsp0 -bso0 "${target}" -o"${targetdir}"
    MSYS2_ARG_CONV_EXCL="*" Reg Add "HKLM\SOFTWARE\LLVM\LLVM" /ve /t REG_SZ /f /reg:32 \
      /d "$(cygpath -w -- "${targetdir}")" > /dev/null
    rm -f -- "${target}"
  else
    sudo tar -x -J --strip-components=1 -C "${targetdir}"
  fi
  "${targetdir}"/bin/clang --version 1>&2
}

install_toolchains "$@"
