#!/usr/bin/env python3
"""Simulated PLC: a Modbus TCP server exposing 2 holding registers.

This is the "PLC" of the test — it knows nothing about VidgeDB. It simply serves
Modbus bytes the way a real PLC would (or an IO-Link gateway behind one).
"""
import asyncio

from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import StartAsyncTcpServer

PORT = 15020


async def main():
    # Holding registers: [current x10 = 28 (2.8 A), max setpoint x10 = 30 (3.0 A)]
    # pymodbus 3.x: addressing is 1-based internally (0 is refused).
    block = ModbusSequentialDataBlock(1, [28, 30] + [0] * 30)
    device = ModbusDeviceContext(hr=block)
    context = ModbusServerContext(devices=device, single=True)
    print(f"simulated PLC: Modbus TCP on 0.0.0.0:{PORT} (registers 0-1 served)")
    await StartAsyncTcpServer(context=context, address=("127.0.0.1", PORT))


if __name__ == "__main__":
    asyncio.run(main())
