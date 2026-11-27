# The DuckFleet fork

`athvin/dbt` is dbt-labs/dbt (the Rust dbt v2) plus a first-class DuckFleet
adapter. dbt v2 has no out-of-tree adapter plugins (the adapter set is a closed
`AdapterType` enum), so DuckFleet ships as this fork. The default branch,
`duckfleet-adapter`, is upstream `main` plus the DuckFleet commits. Releases are
`duckfleet-v*` tags, built by `.github/workflows/duckfleet-release.yml`.

## What the fork adds

Keep this diff small and in as few places as possible; every line here is a line
that can conflict on the next sync.

- `crates/dbt-adapter/src/engine/duckfleet.rs`: the DuckFleet engine (HTTP to the
  DuckFleet gateway through the sidecar client).
- `dbt_macro_assets/dbt-duckfleet/`: the macro package (DuckDB macros are
  inherited; the seed override binds INSERTs).
- Registration of `AdapterType::DuckFleet` and its dispatch to DuckDB's macros:
  small edits in `dbt-adapter`, `dbt-adapter-core`, `dbt-adapter-engine`,
  `dbt-adapter-sql`, `dbt-schemas`, `dbt-profile-schemas`, `dbt-jinja`
  (`dispatch_object.rs`), `dbt-loader`, `dbt-compilation` and `dbt-tasks-sa`.
- `.github/workflows/duckfleet-release.yml` and
  `.github/workflows/duckfleet-upstream-check.yml`.

The exact list, from any checkout:

```sh
git fetch https://github.com/dbt-labs/dbt.git main:refs/remotes/upstream/main
git diff --stat "$(git merge-base duckfleet-adapter upstream/main)" duckfleet-adapter
```

## Keeping up with upstream

**How often.** Sync on every upstream v2 release, and at least weekly. The
`duckfleet-upstream-check` workflow runs every Monday (and on demand). Its job
summary says how far behind `duckfleet-adapter` is and whether the merge is clean.
It fails, so GitHub notifies the owner, when an upstream v2 release is missing or
the merge would conflict. The target: never more than one upstream minor release
behind.

**Merge, never rebase.** Published `duckfleet-v*` tags point at commits on this
branch. A rebase would orphan them and rewrite what every release was built from.

**The procedure:**

```sh
git fetch https://github.com/dbt-labs/dbt.git main:refs/remotes/upstream/main --tags
git switch -c chore/upstream-sync-$(date +%Y%m%d) duckfleet-adapter
git merge upstream/main            # resolve conflicts in favour of upstream,
                                   # then re-apply the DuckFleet lines above
cargo build --release --bin dbt
cargo test --release -p dbt-adapter --lib engine::duckfleet
# The fixture gate: dbt build against a real DuckFleet (the local Compose stack
# from athvin/duck-fleet, `make up`), using a project such as
# athvin/duck-fleet-demo-dbt with its local profile.
./target/release/dbt build --project-dir <project> --target local
```

Then open a pull request into `duckfleet-adapter`. After it merges, tag the next
`duckfleet-v*` release, and bump the pinned release in athvin/duck-fleet
(`docker/dbt.Dockerfile`, `ARG DBT_RELEASE`) and in the Homebrew formula
(`athvin/homebrew-duckfleet`, `Formula/dbt-duckfleet.rb`).

Push branches that touch `.github/workflows/` over SSH
(`git@github.com:athvin/dbt.git`): the HTTPS token used here lacks the `workflow`
scope.

## Ending the fork

The fork exists because dbt v2 cannot load an adapter from outside the binary.
It ends if `AdapterType::DuckFleet` is accepted upstream, or if upstream gains an
adapter plugin mechanism. Neither has been proposed yet. Until then the diff above is
kept minimal, so a proposal stays reviewable.
