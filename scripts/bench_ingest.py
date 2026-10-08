#!/usr/bin/env python3
"""Mesure la VRAIE ingestion de points par le chemin public : JSON-RPC sur stdin,
comme le fait un client. Trois lots de tailles différentes + un flux continu,
et on relève le débit et le RSS du processus vidgedb.

Usage: python3 bench_ingest.py <binaire> [n_points_par_lot]
"""
import json, os, resource, subprocess, sys, time

BIN = sys.argv[1] if len(sys.argv) > 1 else "./target/release/vidgedb"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
DB = "/tmp/bench_ingest.vdg"
for suf in ("", "-wal", "-wlock"):
    try: os.remove(DB + suf)
    except OSError: pass

T0 = 1_700_000_000


def rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int("".join(c for c in line if c.isdigit()))
    except OSError:
        pass
    return 0


p = subprocess.Popen([BIN, "--service", DB, "--role", "ingest", "--agent-id", "bench"],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
rss_before = rss_kb(p.pid)

def call(obj_id, method, params):
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": obj_id, "method": method,
                              "params": params}) + "\n")
    p.stdin.flush()
    return json.loads(p.stdout.readline())


# entité + spec, une fois
call(1, "upsert_entity", {"name": "EMOT01", "type": "Motor",
                          "props": {"spec.current.max": "10"}, "source": "plc"})

print(f"{'lot':>10} {'points':>8} {'secondes':>9} {'points/s':>12}")
t_total = 0.0
n_total = 0
for batch in (1, 100, 1000, N):
    pts = [[T0 + i, 2.5 + (i % 13) * 0.1] for i in range(batch)]
    t = time.perf_counter()
    r = call(2, "ingest_points", {"entity": "EMOT01", "signal": "current", "points": pts})
    dt = time.perf_counter() - t
    t_total += dt; n_total += batch
    ok = r.get("result", {}).get("accepted", "?")
    print(f"{batch:>10} {batch:>8} {dt:>9.4f} {batch/dt:>12,.0f}   (accepted={ok})")

# flux continu : 200 lots de 1000, une transaction par lot
BATCH, REPS = 1000, 200
t = time.perf_counter()
for k in range(REPS):
    pts = [[T0 + 1_000_000 + k * BATCH + i, 2.5] for i in range(BATCH)]
    call(3, "ingest_points", {"entity": "EMOT01", "signal": "current", "points": pts})
dt = time.perf_counter() - t
print(f"{'flux':>10} {BATCH*REPS:>8} {dt:>9.4f} {BATCH*REPS/dt:>12,.0f}   ({REPS} lots de {BATCH})")

rss_after = rss_kb(p.pid)
tot_points = n_total + BATCH * REPS

# relecture : combien de points le moteur rend-il vraiment ?
r = call(4, "get_measurements", {"entity": "EMOT01", "signal": "current"})
got = r.get("result", {}).get("count", "?")

p.stdin.close()
p.wait(timeout=20)

print()
print(f"points ingérés (chemin JSON-RPC) : {tot_points:,}")
print(f"points relus par le moteur       : {got}")
print(f"RSS avant/après (vidgedb)        : {rss_before:,} kB -> {rss_after:,} kB "
      f"(= {rss_after/1024:.1f} Mo)")
print(f"fichier .vdg                     : {os.path.getsize(DB):,} octets")
print(f"total débit ingéré               : {tot_points/t_total:,.0f} pts/s (sans le flux)")
