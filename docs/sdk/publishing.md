# Publishing — how these packages get built and released

*State (2026-10-09): the JS SDK and the Node-RED nodes are published on npm under the
`@vidge-ai` scope. Python SDK: not on PyPI yet — install from this repository.*

---

## 1. What goes where

| Package | Registry | Name | Artifact |
|---|---|---|---|
| `sdk-python/` | PyPI | `vidgedb` | wheel + sdist (pure Python, `py3-none-any`) |
| `sdk-js/` | npm | `@vidge-ai/vidgedb` | tarball built from `dist/` (ESM + CJS + `.d.ts`) |
| `sdk-nodered/` | npm (Node-RED library) | `@vidge-ai/node-red-contrib-vidgedb` | tarball with `nodes/` + `examples/` |

All three are **namespaced under the same product name** on purpose: one product, three
languages. The npm packages live inside the `vidge-ai` **organization** (scoped names):
publishing a scoped name from the org's members is what ownership means on npm. The
Node-RED package keeps the `node-red-contrib-` infix inside its scoped name because that
is what the Node-RED palette ecosystem expects.

**A separate engine artifact repository is not needed.** The engine and the clients all
live in this repository (monorepo); the "two artifacts per platform" decision (core
~1.24 MiB vs full ~14 MiB with the OPC-UA server, both built from the same source via
the `opcua` feature) is the `release.yml` workflow's job.

## 2. Credentials

The npm publishes in CI are **token-free**: the `npm-sdks` job of `release.yml` runs
with `permissions: id-token: write` and the registry verifies the GitHub Actions OIDC
identity against the package's **Trusted publisher** configuration (npmjs.com → package
→ Settings → Trusted publishing: provider GitHub Actions, this repository, workflow
`release.yml`). A maintainership token is only needed for the FIRST publish of a brand
new package name (which registers it under the org) or for local manual publishes —
never commit it, never put it in a file: `export NPM_CONFIG_//registry.npmjs.org/:_authToken=…` or
`npm config set //registry.npmjs.org/:_authToken …` locally.

| Secret | Where to create it | Scope |
|---|---|---|
| `GITHUB_TOKEN` | provided by Actions automatically | release artifacts + OIDC publish |
| npm token (bootstrap/manual only) | npmjs.com → Access Tokens | the `vidge-ai` org's packages |

## 3. The release pipeline

```text
git tag v0.1.0  →  push tag
        │
        ├─ job: binaries  → engine tarballs per platform (core + opcua)
        ├─ job: container → ghcr.io image
        ├─ job: npm-sdks  → OIDC publish @vidge-ai/vidgedb + @vidge-ai/node-red-contrib-vidgedb
        │                  (skips a version that already exists on the registry)
        └─ job: release   → attach the engine tarballs to the GitHub release
```

Rules that make this safe:

1. **Everything is gated on the tests.** The `npm-sdks` job installs the freshly built
   engine binary and runs BOTH SDK suites before any publish; a red suite fails the job
   before it uploads. npm does not allow replacing an uploaded version.
2. **Versions are bumped in one place per package**, in the same commit as the tag:
   `sdk-python/pyproject.toml`, `sdk-js/package.json`, `sdk-nodered/package.json`.
   The three must agree — otherwise `pip install vidgedb==0.2.0` and
   `npm install @vidge-ai/vidgedb@0.2.0` bring different things.
3. **The engine version is a separate axis.** These clients work with any engine
   speaking the same method set, so the clients can be released without a new engine.
4. **New publishes may land in npm's staged-review queue** (`0.0.0-stage` placeholder on
   the registry): a maintainer approves them with 2FA on npmjs.com (Staged Packages) or
   `npm stage approve` (npm ≥ 11.15). Do not re-publish while a version waits in the
   queue — the submit already succeeded.

## 4. Building and checking locally (no registry involved)

### Python

```bash
cd sdk-python
python -m build                    # → dist/vidgedb-0.1.0-py3-none-any.whl + .tar.gz
python -m twine check dist/*
pip install dist/*.whl && python -c "import vidgedb; print(vidgedb.__version__)"
```

### JavaScript

```bash
cd sdk-js
npm install
npm run build                      # tsc → dist/esm + dist/cjs
npm test                           # 32 node:test cases
npm pack --dry-run                 # shows exactly what would be uploaded
```

`npm pack --dry-run` is the check people forget: `files` in `package.json` decides what
actually ships, and a missing `dist/` entry produces a package that installs and does
nothing.

### Node-RED

```bash
cd sdk-nodered
npm test                           # 26 cases
npm pack --dry-run
```

## 5. What to publish with which release

1. **npm packages with every engine tag that changes them.** The `npm-sdks` job already
   runs on every `v*` tag; the version-skip guard makes re-tags and release-only runs
   painless.
2. **PyPI first release** when the Python packaging job is wired into `release.yml` —
   until then `pip install vidgedb` is NOT possible, install from source or via the
   `sdk-python/` directory.
3. **The engine tarballs attach themselves** (the `release` job), so `VIDGEDB_BIN` can
   point at a download instead of a build.

## 6. If a publish goes wrong

| Situation | Consequence | Remedy |
|---|---|---|
| Wrong content uploaded to PyPI | the version is burned | bump the patch version, publish again; you cannot overwrite |
| `npm publish` with a missing `dist/` | a package that installs and fails at runtime | publish a patch version with the fix; `npm deprecate` the bad one |
| A leaked token in a workflow | someone else can publish as you | revoke the token first, then rewrite history if needed |
| A package was published under the wrong owner (personal account instead of the org) | npm ownership is fixed at first publish for unscoped names | use scoped names (`@org/…`) from the start; npm has no package migration path |

The first rule of this section is the reason the tests are gated: **a broken client
published is worse than a client not published.**