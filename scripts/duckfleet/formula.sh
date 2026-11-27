#!/bin/sh
# Print Formula/dbt-duckfleet.rb for a release (athvin/duck-fleet#215): the same
# formula #211 hand-wrote and verified with `brew install` / `brew test`, with the
# version, URLs and checksums filled in from that release's SHA256SUMS.
#
#   scripts/duckfleet/formula.sh duckfleet-v0.1.8 dist/SHA256SUMS > dbt-duckfleet.rb
set -eu
tag=$1
sums=$2
version=${tag#duckfleet-v}
url() { printf 'https://github.com/athvin/dbt/releases/download/%s/dbt-%s.tar.gz' "$tag" "$1"; }
sum() {
  s=$(grep " dbt-$1.tar.gz\$" "$sums" | cut -d' ' -f1)
  [ -n "$s" ] || { echo "formula.sh: $sums lists no dbt-$1.tar.gz" >&2; exit 1; }
  printf '%s' "$s"
}
cat <<RUBY
class DbtDuckfleet < Formula
  desc "dbt-core v2 with DuckFleet as a built-in adapter"
  homepage "https://github.com/athvin/dbt"
  version "$version"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "$(url darwin-aarch64)"
      sha256 "$(sum darwin-aarch64)"
    end
    on_intel do
      url "$(url darwin-x86_64)"
      sha256 "$(sum darwin-x86_64)"
    end
  end

  on_linux do
    on_arm do
      url "$(url linux-aarch64)"
      sha256 "$(sum linux-aarch64)"
    end
    on_intel do
      url "$(url linux-x86_64)"
      sha256 "$(sum linux-x86_64)"
    end
  end

  # Installs a \`dbt\` command: unlink any other dbt (pip, pipx) on PATH first.
  def install
    bin.install "dbt"
  end

  test do
    assert_match "dbt", shell_output("#{bin}/dbt --version")
  end
end
RUBY
