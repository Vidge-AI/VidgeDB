# Installation — getting the engine, then a client

*The clients are clients. Without an engine binary they cannot do anything, so this
document covers the engine first. Every option below was exercised on this project.*

---

## 0. What you need

| Piece | Required? | What it is |
|---|---|---|
| the `vidgedb` binary | **yes, always** | the engine; one static file per platform |
| a client package | yes | `vidgedb` (Python), `vidgedb` (npm), or the Node-RED nodes |
| a `.vdg` file | created on first open | your twin; back it up like a database |

## 1. Getting the engine

### A — build from source (any Linux or macOS with Rust)

```bash
git clone https://github.com/Vidge-AI/VidgeDB && cd vidgeDB
cargo build --release --bin vidgedb
export VIDGEDB_BIN=$PWD/target/release/vidgedb
```

This yields the **full** build (~14 MiB on x86_64: the native OPC-UA server pulls a
vendored OpenSSL). For a **core** build without OPC-UA — ~1.24 MiB — add
`--no-default-features`:

```bash
cargo build --release --no-default-features --bin vidgedb
```

If you only read and write twins, the core is the right choice: same engine, same
protocol, a tenth of the size. `--opcua` on a core binary prints an explicit "built
without OPC-UA support" message and exits 2.

### B — cross-compile for a Raspberry Pi

```bash
rustup target add aarch64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl --bin vidgedb
# static aarch64 ELF, runs on Pi 4/5/Zero 2 as-is — copy it with scp
```

Note: with OPC-UA included expect ~12.3 MiB unstripped / ~10.1 MiB stripped (the
figures from before the OPC-UA server existed — ~600 KiB — no longer apply).

### C — container (no toolchain at all)

```bash
docker run -d --name vidgedb \
  -p 127.0.0.1:8888:8888 \
  -v vidgedb-data:/data \
  -e VIDGEDB_TOKEN="$(openssl rand -hex 24)" \
  ghcr.io/<org>/vidgedb
```

The image refuses to start with an empty token, because the native HTTP endpoint has
**no TLS**. Publish the port on `127.0.0.1` and put a TLS reverse proxy in front for
remote access — never `-p 8888:8888` on a reachable host.

### D — release artifacts

Once a `v*` tag has run the release workflow, each platform gets an archive containing
the binary; the installers fetch it:

```bash
# Linux / macOS
curl -fsSL https://github.com/Vidge-AI/VidgeDB/releases/latest/download/install.sh | sh
# Windows (PowerShell)
irm https://github.com/Vidge-AI/VidgeDB/releases/latest/download/install.ps1 | iex
```

Both installers run `--version` on the installed binary **before** reporting success,
so a wrong-architecture download fails loudly instead of looking fine.

### E — offline / air-gapped sites

Copy the binary and the `.vdg` file. There is no runtime dependency, no service to
register, no package to fetch. On systemd:

```ini
[Unit]
Description=VidgeDB digital-twin service
After=network.target

[Service]
ExecStart=/usr/local/bin/vidgedb --service /var/lib/vidgedb/plant.vdg \
          --agent-id plc-bridge --role ingest --retention-days 30
Restart=always
RestartSec=3
User=vidgedb
[Install]
WantedBy=multi-user.target
```

For a pure stdin/stdout consumer, drop `StandardInput=socket` and let the parent
process own the child — which is what every client here does.

## 2. Installing the clients

### Python

```bash
pip install vidgedb
```

**Not on PyPI yet** — see [publishing.md](publishing.md). Until then, from
this repository:

```bash
cd python && pip install -e .
```

Python ≥ 3.9, zero runtime dependencies (stdlib only: `subprocess`, `json`,
`dataclasses`).

### JavaScript / TypeScript

```bash
npm install @vidge-ai/vidgedb
```

Build from source instead:

```bash
cd js && npm install && npm run build
```

Node ≥ 18, ESM and CJS entry points, zero runtime dependencies.

### Node-RED

```bash
cd ~/.node-red
npm install @vidge-ai/node-red-contrib-vidgedb
```

**Not in the Node-RED library yet.** Until then, install from this repository path and
restart Node-RED. The package declares `node-red.nodes` in its `package.json`, which is
what makes the palette entry appear.

## 3. Making the client find the engine

All three packages resolve the binary in the same order:

1. the explicit argument — `bin=...` (Python), `{ bin: ... }` (JS), the `bin` field on
   the Node-RED config node;
2. the `VIDGEDB_BIN` environment variable;
3. `vidgedb` on `PATH`.

The quickest correct setup on a workstation is therefore:

```bash
export VIDGEDB_BIN=/absolute/path/to/vidgedb
```

Set it in the service unit or the container environment for anything long-lived — an
absolute path, because a daemon has no useful `PATH`.

## 4. Verifying the installation

```bash
"$VIDGEDB_BIN" --version          # vidgedb 0.1.0
"$VIDGEDB_BIN" smoke              # the spec §58 example machine, 2 violations detected
```

Then, per client:

```bash
cd python && python -m pytest -q          # 35 passed
cd js && npm test                         # 32 passed  (builds first)
cd node-red && npm test                   # 27 passed
```

If `--version` works but a client cannot start, the problem is resolution (step 3),
not the engine.
