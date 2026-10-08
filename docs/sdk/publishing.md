# Publishing — how these packages get built and released

*Honest state first: as of `v0.1.0` **nothing here is published**. The names below were
checked as available, but no release has run. This document is the procedure, not a
claim that it has happened.*

---

## 1. What goes where

| Package | Registry | Name (availability checked) | Artifact |
|---|---|---|---|
| `sdk-python/` | PyPI | `vidgedb` | wheel + sdist (pure Python, `py3-none-any`) |
| `sdk-js/` | npm | `vidgedb` | tarball built from `dist/` (ESM + CJS + `.d.ts`) |
| `sdk-nodered/` | npm (Node-RED library) | `node-red-contrib-vidgedb` | tarball with `nodes/` + `examples/` |

All three are **namespaced under the same product name** on purpose: one product, three
languages. The Node-RED package keeps the `node-red-contrib-` prefix because that is what
the Node-RED palette requires.

**A separate engine artifact repository is not needed.** The engine lives in `vidgeDB`
and is released there; the "two artifacts per platform" decision (core ~1.24 MiB vs
full ~14 MiB with the OPC-UA server, both built from the same source via the `opcua`
feature) belongs to that repository's release workflow. Duplicating it here would mean two
workflows building the same binary, and the slower one becomes the reference by accident.

## 2. Credentials needed

None of these are on this machine today, which is why nothing is published:

| Secret | Where to create it | Scope |
|---|---|---|
| `PYPI_API_TOKEN` | pypi.org → Account → API tokens | project `vidgedb` |
| `NPM_TOKEN` | npmjs.com → Access tokens → Automation | publish `vidgedb`, `node-red-contrib-vidgedb` |
| `GITHUB_TOKEN` | provided by Actions automatically | `contents: write` to attach release artifacts |

Store them as repository secrets (`Settings → Secrets and variables → Actions`). Never
in a file, never in a workflow, never in a commit.

## 3. The release pipeline

```text
git tag v0.1.0  →  push tag
        │
        ├─ job: python   → build sdist+wheel, `twine check`, publish to PyPI
        ├─ job: js       → npm ci, npm run build, npm test, npm publish
        ├─ job: node-red → npm test, npm publish
        └─ job: release  → attach the three tarballs to the GitHub release
```

Rules that make this safe:

1. **Everything is gated on the tests.** A publish job with a red suite must fail before
   it uploads; PyPI does not allow republishing the same version, so a bad upload cannot
   be quietly replaced.
2. **Versions are bumped in one place per package**, in the same commit as the tag:
   `sdk-python/pyproject.toml`, `sdk-js/package.json`, `sdk-nodered/package.json`. The three must
   agree — otherwise `pip install vidgedb==0.2.0` and `npm install vidgedb@0.2.0` bring
   different things.
3. **The engine version is a separate axis.** These clients work with any engine speaking
   the same method set, so the clients can be released without a new engine.

## 4. Building and checking locally (no registry involved)

### Python

```bash
cd python
python -m build                    # → dist/vidgedb-0.1.0-py3-none-any.whl + .tar.gz
python -m twine check dist/*
pip install dist/*.whl && python -c "import vidgedb; print(vidgedb.__version__)"
```

### JavaScript

```bash
cd js
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
cd node-red
npm test                           # 27 cases
npm pack --dry-run
```

## 5. What to publish first, and why

The order that minimises embarrassment:

1. **PyPI + npm in the same release.** They are the two "installable client" promises; a
   user who reads the README will try both.
2. **Node-RED after.** Its users install from the palette, and having the other two live
   first means the README can link to real installs.
3. **The engine artifacts alongside** (the `vidgeDB` repository's release workflow), so
   that `VIDGEDB_BIN` can point at a download instead of a build. Until an artifact
   exists, every client instruction starts with "compile Rust", which is the friction
   these packages were meant to remove.

## 6. If a publish goes wrong

| Situation | Consequence | Remedy |
|---|---|---|
| Wrong content uploaded to PyPI | the version is burned | bump the patch version, publish again; you cannot overwrite |
| `npm publish` with a missing `dist/` | a package that installs and fails at runtime | publish a patch version with the fix; `npm deprecate` the bad one |
| A leaked token in a workflow | someone else can publish as you | revoke the token first, then rewrite history if needed |

The first rule of this section is the reason the tests are gated: **a broken client
published is worse than a client not published.**
