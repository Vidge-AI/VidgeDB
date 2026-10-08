#!/usr/bin/env python3
"""PROOF: a Modbus TCP client (simulated PLC) -> a Python gateway -> the
VidgeDB database, over JSON-RPC.

The role of each part is the point of the test:
  - pymodbus does the PROTOCOL (the Modbus bytes on the wire);
  - the vidgedb SDK does the DATABASE (JSON-RPC to the binary);
  - the loop in this script IS THE GATEWAY (register -> series mapping).

Expected output: the two registers read, then a check result read back from
the database, with both provenance sides shown.
"""
import asyncio
import sys

from pymodbus.client import AsyncModbusTcpClient
from vidgedb import VidgeDB

HOST, PORT = "127.0.0.1", 15020
DB = "/tmp/gw.vdg"
BIN = "~/vidgeDB/target/release/vidgedb"


async def main():
    # 1) the simulated PLC
    cli = AsyncModbusTcpClient(HOST, port=PORT)
    ok = await cli.connect()
    if not ok:
        print("FAIL: cannot connect over Modbus")
        return 1
    unit = 1
    # register 0 = current x10 (28 = 2.8 A), register 1 = max setpoint x10 (30 = 3.0 A)
    rr = await cli.read_holding_registers(0, count=2, device_id=unit)
    vals = rr.registers
    print(f"MODBUS READ: register0={vals[0]} register1={vals[1]}")
    current = vals[0] / 10.0
    spec_max = vals[1] / 10.0
    cli.close()

    # 2) the gateway writes into the database through the official SDK
    with VidgeDB(DB, agent_id="gw-modbus", role="ingest", bin=BIN) as db:
        print("SDK:", db.upsert_entity(name="EMOT01", type="Motor",
                                       props={"spec.current.max": "3"}, source="plc"))
        print("SDK:", db.call("ingest_points", entity="EMOT01", signal="current",
                              points=[[1760000000, current]]))

    # 3) read back: the ENGINE weighs the spec (Specification) against the measurement (Observation)
    with VidgeDB(DB, agent_id="gw-reader", bin=BIN) as db:
        rep = db.check("EMOT01", "current", from_=1760000000, to=1760003600)
        print(f"CHECK: status={rep.status} observed={rep.observed} "
              f"expected_max={rep.expected_max} dev={rep.deviation}")
        print(f"PROVENANCE: observed={rep.observed_provenance} expected={rep.expected_provenance}")
    print("GATEWAY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
