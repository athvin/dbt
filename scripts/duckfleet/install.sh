#!/bin/sh
# Install the prebuilt DuckFleet dbt binary (athvin/duck-fleet#215).
#
#   curl -LsSf https://github.com/athvin/dbt/releases/latest/download/dbt-installer.sh | sh
#
# Downloads dbt-<os>-<arch>.tar.gz from a duckfleet-v* release, verifies it against
# the release's SHA256SUMS, and installs `dbt` into $DBT_INSTALL_DIR (default
# ~/.local/bin). Nothing is compiled and nothing else is installed.
#
#   DBT_VERSION=duckfleet-v0.1.8   a specific release (default: the one this
#                                  installer was published with, else latest)
#   DBT_INSTALL_DIR=/usr/local/bin where to put dbt
set -eu

repo="athvin/dbt"
# Replaced with the release tag when the release publishes this script.
published_tag="@DBT_RELEASE_TAG@"
case "$published_tag" in @*) published_tag="";; esac
tag="${DBT_VERSION:-$published_tag}"
dir="${DBT_INSTALL_DIR:-$HOME/.local/bin}"

say() { printf 'dbt-installer: %s\n' "$*" >&2; }
die() { say "$*"; exit 1; }

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) die "no prebuilt dbt for $(uname -s); build from source: cargo install --git https://github.com/$repo dbt-sa-cli" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  arm64 | aarch64) arch=aarch64 ;;
  *) die "no prebuilt dbt for $(uname -m)" ;;
esac

if [ -n "$tag" ]; then
  base="https://github.com/$repo/releases/download/$tag"
else
  base="https://github.com/$repo/releases/latest/download"
fi
asset="dbt-$os-$arch.tar.gz"

fetch() {
  if command -v curl >/dev/null 2>&1; then curl -fsSL --retry 3 -o "$2" "$1"
  elif command -v wget >/dev/null 2>&1; then wget -q -O "$2" "$1"
  else die "needs curl or wget"; fi
}
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# The Linux build targets glibc 2.35 (Ubuntu 22.04, Debian 12) and newer.
if [ "$os" = linux ] && command -v ldd >/dev/null 2>&1; then
  glibc=$(ldd --version 2>/dev/null | head -1 | sed -n 's/.* \([0-9][0-9]*\.[0-9][0-9]*\)$/\1/p')
  case "$glibc" in
    2.[0-9] | 2.[0-2][0-9] | 2.3[0-4]) die "dbt needs glibc 2.35 or newer; this system has $glibc" ;;
  esac
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
say "downloading $asset${tag:+ ($tag)}"
fetch "$base/$asset" "$tmp/$asset"
fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS"
want=$(grep " $asset\$" "$tmp/SHA256SUMS" | cut -d' ' -f1)
[ -n "$want" ] || die "SHA256SUMS lists no $asset"
got=$(sha256 "$tmp/$asset")
[ "$want" = "$got" ] || die "checksum mismatch for $asset (want $want, got $got)"
tar -xzf "$tmp/$asset" -C "$tmp"
mkdir -p "$dir"
install -m 0755 "$tmp/dbt" "$dir/dbt"
say "installed $("$dir/dbt" --version 2>/dev/null | head -1) to $dir/dbt"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) say "add $dir to your PATH" ;;
esac
