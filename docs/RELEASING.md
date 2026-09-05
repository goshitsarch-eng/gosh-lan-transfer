# Releasing gosh-lan-transfer

The `Release` workflow runs when `Cargo.toml` or `CHANGELOG.md` changes on
`master`, or through **Actions → Release → Run workflow** on `master`.
It runs Linux, Windows and macOS CI, verifies the source package, checks for
an existing registry version, creates a GitHub tag/release with the `.crate`
asset and changelog notes, then publishes to crates.io using OIDC.
This is a library; there are no CLI or GUI binaries to distribute.

## One-time crates.io setup

An owner must add a trusted publisher at
<https://crates.io/crates/gosh-lan-transfer/settings/new-trusted-publisher>:

| Field | Value |
| --- | --- |
| GitHub owner | `goshitsarch-eng` |
| Owner ID | `252059301` |
| Repository | `gosh-lan-transfer` |
| Workflow filename | `release.yml` |
| Environment | Leave blank (workflow does not use an environment) |

This authorizes only this crate's publishing workflow. Configuring a publisher
for another crate does not authorize this one. No permanent Cargo token or
GitHub repository secret is needed. See the [official action](https://github.com/rust-lang/crates-io-auth-action)
and [crates.io instructions](https://crates.io/docs/trusted-publishing).
If the crate has never been published, bootstrap its first publication using
crates.io's supported owner setup before using this workflow.

## Each release

1. Update the version in `Cargo.toml` and regenerate `Cargo.lock` with Cargo.
2. Add a matching `## [x.y.z]` entry in `CHANGELOG.md`; update README and API docs.
3. Open a PR and wait for every CI platform to pass before merging to `master`.
4. Follow the `Release` workflow to completion. Verify the GitHub asset,
   crates.io version, and subsequent docs.rs build.

Do not create or move the version tag manually. The workflow creates it at the
validated commit, refuses to reuse a tag on another commit, and refuses to
reuse a registry version whose package checksum differs or which is yanked.

If trusted publishing is not configured, the GitHub release can succeed while
the publishing step fails. After configuring it, rerun the failed job for that
same commit. Existing matching registry versions are skipped, so retries after
a partial failure are safe. Do not rerun a newer commit with an old version;
bump the version if the packaged contents have changed.

## Local validation

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo test --locked --doc --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features
cargo package --locked --all-features
```

Multicast integration tests report when the environment blocks discovery.
A passing test suite with a multicast skip does not certify discovery on the
target LAN; verify discovery and a large transfer between two real devices.
