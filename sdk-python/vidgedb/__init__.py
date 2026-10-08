"""VidgeDB Python SDK — pilot the `vidgedb --service` JSON-RPC subprocess.

Zero runtime dependencies: stdlib json/subprocess/dataclasses/typing only.
Works on CPython >= 3.9 (incl. Raspberry Pi deployments).

    from vidgedb import VidgeDB

    with VidgeDB("/data/machine.vdg", agent_id="my-agent") as db:
        report = db.check("Motor42", "current", from_=1760000000, to=1760025600)
        print(report.status, report.deviation)
"""

from .service import VidgeDB, VidgeDBError
from .types import CheckResult, CheckStatus, Measurement, MeasurementSeries

__all__ = [
    "VidgeDB",
    "VidgeDBError",
    "Measurement",
    "MeasurementSeries",
    "CheckResult",
    "CheckStatus",
]

__version__ = "0.1.0"