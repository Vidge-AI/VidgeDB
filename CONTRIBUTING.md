# Contributing to VidgeDB

Thanks for looking. This is a small project with a strict house style; reading this file
first will save you a rejected pull request.

## Before you write code

**Read [`DEVELOPING.md`](DEVELOPING.md).** It lists the invariants that must not break —
the ones that give the engine its point (provenance is never promoted, a rolled-back
transaction leaves no durable trace, the writer lock is single, and so on). A change that
weakens one of them will be rejected regardless of how useful it looks.

The observable behaviour of the engine is documented in [`docs/`](docs/); the design
specification referenced as `spec §NN` in code comments is an internal document, not
versioned here. Where the two disagree, **the tested code wins**.

## Setting up

```bash
cargo build --release
VIDGEDB_BIN=$PWD/target/release/vidgedb cargo test --release
cargo fmt
```

**`VIDGEDB_BIN` is not optional.** Several tests drive the real binary as a subprocess —
the only way to observe session poisoning, signal handling, or the JSON-RPC contract.
Without it, those tests fail to spawn and the aggregate count **silently drops**. If your
total is suspiciously round, check the variable before believing it.

Useful variants:

```bash
cargo build --release --no-default-features   # the small core binary (~1.31 MiB)
cargo test --release -- --include-ignored     # re-run the slow #[ignore]-tagged probes
```

## What a good change looks like

- **A test that fails first.** Almost every fix in this repository ships with a test that
  was observed failing before the change and passing after. If your change cannot be shown
  to fail, say so explicitly in the PR description rather than implying coverage.
- **Honest limits.** If something is unverified — a platform you could not run, a path you
  could not exercise — write it down as unverified. This repository's documentation
  distinguishes "measured here" from "expected"; do not blur the two.
- **`cargo fmt` and zero warnings.** Both are checked.

## The clients are thin on purpose

`sdk-python/`, `sdk-js/` and `sdk-nodered/` are wrappers over the engine's JSON-RPC
surface. They do not carry business logic: a new capability belongs in the engine, exposed
through a method, and then surfaced by the clients — never the other way round. The
generic dispatcher is the contract; typed helpers are conveniences layered on top.

Each client is autonomous: its tests build the test twin over JSON-RPC from a stdlib
fixture, so **no Rust toolchain is needed** to run them. Keep it that way.

## Commit messages

Explain *what changed and why it matters*, not just the mechanics. State measured numbers
rather than adjectives, and name anything you did not verify.

## Reporting a bug

Include the exact call, the observed response, and the expected one. If you have a
`.vdg` file that reproduces it, describe how it was produced — the schema, the sequence of
writes, and the version (`vidgedb --version`).

## Licence

By contributing you agree that your work is released under the **Apache-2.0** licence,
the same as the project (see [`LICENSE`](LICENSE)).
