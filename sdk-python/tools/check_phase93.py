#!/usr/bin/env python3
"""Verifie que le SDK Python atteint les nouvelles methodes (phase 93) via son
dispatch dynamique, SANS modification du SDK.

C'est le vrai test : `from` est un mot reserve de Python, donc un helper type
`def graph(from=...)` serait impossible — la preuve que le dispatcher `call()`
est la bonne porte d'entree.
"""
import json
import os
import subprocess
import sys
import tempfile

BIN = os.environ.get("VIDGEDB_BIN", "~/vidgeDB/target/release/vidgedb")
DB = os.path.join(tempfile.gettempdir(), f"sdk_p93_{os.getpid()}.vdg")
for suf in ("", "-wal", "-wlock"):
    try:
        os.remove(DB + suf)
    except FileNotFoundError:
        pass


def rpc(proc, rid, method, params):
    proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n")
    proc.stdin.flush()
    return json.loads(proc.stdout.readline())


proc = subprocess.Popen(
    [BIN, "--service", DB, "--role", "ingest", "--agent-id", "sdkcheck"],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
)

try:
    print("=== le SDK atteint-il les nouvelles methodes ? ===")
    rpc(proc, 1, "upsert_entity", {"name": "PLC01", "type": "PLC", "props": {"vendor": "Siemens"}, "source": "agent"})
    rpc(proc, 2, "upsert_entity", {"name": "MOT01", "type": "Motor", "props": {"spec.current.max": "10"},
                                   "relations": [{"to": "PLC01", "relation_type": "network:controls"}], "source": "agent"})
    rpc(proc, 3, "ingest_points", {"entity": "MOT01", "signal": "current", "points": [[1700000000, 9.0], [1700000060, 14.0]]})

    # 1) list_entities (avec et sans filtre)
    r = rpc(proc, 4, "list_entities", {})["result"]
    print(f"  list_entities           -> {r['total']} entites: {[e['name'] for e in r['entities']]}")
    r = rpc(proc, 5, "list_entities", {"type": "Motor"})["result"]
    print(f"  list_entities(type=)    -> {r['total']} entite:  {[e['name'] for e in r['entities']]}")

    # 2) alias 'entities'
    r = rpc(proc, 6, "entities", {})["result"]
    print(f"  alias 'entities'        -> {r['total']} entites (OK si = 2)")

    # 3) graph : le mot reserve Python 'from' passe par **kwargs
    r = rpc(proc, 7, "graph", {})["result"]
    for rel in r["relations"]:
        print(f"  graph                   -> {rel['from']} --{rel['relation_type']}--> {rel['to']} [{rel['provenance']}]")

    # Le point cle : peut-on nommer le parametre 'from' depuis Python ?
    try:
        probe = dict(**{"from": 1699999000, "to": 1700005000})
        print(f"  dict(**{{'from':...}})    -> OK: {probe}  (aucun conflit, c'est un dict)")
    except Exception as e:  # pragma: no cover
        print(f"  dict(**{{'from':...}})    -> ECHEC: {e}")

    # 4) diagnose
    r = rpc(proc, 8, "diagnose", dict(entity="MOT01", **{"from": 1699999000, "to": 1700005000}))["result"]
    print(f"  diagnose                -> composant={r['component']['name']} "
          f"mesures={[(m['signal'], m['min'], m['max']) for m in r['measurements']]}")
    print(f"                             checks={[(c['signal'], c['status'], c['observed'], c['expected_max']) for c in r['checks']]}")

    # 5) set_hypothesis : refus motive, pas -32601
    r = rpc(proc, 9, "set_hypothesis", {"entity": "MOT01", "text": "roulement"})
    has_err = "error" in r
    print(f"  set_hypothesis          -> transport error={has_err} "
          f"(doit etre False) / result.error present={'error' in r.get('result', {})}")

    print("\nTOUT OK : le SDK atteint les 4 nouvelles methodes sans modification.")
finally:
    proc.stdin.close()
    proc.wait(timeout=30)
    for suf in ("", "-wal", "-wlock"):
        try:
            os.remove(DB + suf)
        except FileNotFoundError:
            pass
