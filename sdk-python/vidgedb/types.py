"""Typed result models for the VidgeDB AgentApi v0 payloads.

Light dataclasses over the JSON the service returns, so an AI agent gets
attribute access + typed fields instead of raw dicts. Deserialization is
total (missing/None fields map to their default, never raise) — a newer
binary adding fields must never break the SDK. Every dataclass keeps the
raw payload in `.raw` for anything the typed view does not cover.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

__all__ = ["CheckStatus", "Measurement", "MeasurementSeries", "CheckResult", "DiagnosticReport"]


@dataclass(frozen=True)
class CheckStatus:
    """Statuses of the spec §25 deviation engine (check.rs)."""

    OK: str = "OK"                    # observed <= expected_max
    VIOLATION: str = "VIOLATION"      # observed > expected_max (strict)
    NO_DATA: str = "NO_DATA"          # spec present, zero points in window
    NO_SPEC: str = "NO_SPEC"          # no spec.<signal>.max property


@dataclass(frozen=True)
class Measurement:
    """One time-series point: unix-seconds `t`, float `value`."""

    t: int
    value: float

    @staticmethod
    def from_rpc(obj: Dict[str, Any]) -> "Measurement":
        return Measurement(t=int(obj.get("t", 0)), value=float(obj.get("value", 0.0)))


@dataclass(frozen=True)
class MeasurementSeries:
    """get_measurements payload: points + aggregate min/max/count."""

    entity: str
    signal: str
    points: List[Measurement]
    count: int
    min: Optional[float]
    max: Optional[float]
    raw: Dict[str, Any]

    @classmethod
    def from_rpc(cls, obj: Dict[str, Any], entity: str = "", signal: str = "") -> "MeasurementSeries":
        pts = [Measurement.from_rpc(p) for p in obj.get("points", [])]
        return cls(
            entity=entity,
            signal=signal,
            points=pts,
            count=int(obj.get("count", 0)),
            min=obj.get("min"),
            max=obj.get("max"),
            raw=obj,
        )


@dataclass(frozen=True)
class CheckResult:
    """check() payload (spec §25, with the §27 example fields)."""

    entity: str
    signal: str
    status: str                       # OK / VIOLATION / NO_DATA / NO_SPEC
    expected_max: Optional[float]
    observed: Optional[float]
    deviation: Optional[float]
    unit: Optional[str]
    expected_provenance: Optional[str]
    observed_provenance: Optional[str]
    points_checked: int
    window_from: int
    window_to: int
    raw: Dict[str, Any]

    @property
    def is_violation(self) -> bool:
        return self.status == CheckStatus.VIOLATION

    @classmethod
    def from_rpc(cls, obj: Dict[str, Any]) -> "CheckResult":
        return cls(
            entity=str(obj.get("entity", "")),
            signal=str(obj.get("signal", "")),
            status=str(obj.get("status", "")),
            expected_max=_opt_float(obj.get("expected_max")),
            observed=_opt_float(obj.get("observed")),
            deviation=_opt_float(obj.get("deviation")),
            unit=obj.get("unit"),
            expected_provenance=obj.get("expected_provenance"),
            observed_provenance=obj.get("observed_provenance"),
            points_checked=int(obj.get("points_checked", 0)),
            window_from=int(obj.get("window", {}).get("from", 0)),
            window_to=int(obj.get("window", {}).get("to", 0)),
            raw=obj,
        )


@dataclass(frozen=True)
class DiagnosticReport:
    """Light view over a DIAGNOSE-shaped report (spec §26).

    VidgeDB v0 exposes DIAGNOSE through `query_temporal` results and the
    `diagnose` crate type; this dataclass stays tolerant — it accepts any
    dict with optional fields and never raises on unknown/newer shapes.
    """

    source: str = "vidgedb_diagnose_v0"
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    hypotheses: List[Dict[str, Any]] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_rpc(cls, obj: Dict[str, Any]) -> "DiagnosticReport":
        return cls(
            source=str(obj.get("source", "vidgedb_diagnose_v0")),
            anomalies=list(obj.get("anomalies", [])),
            hypotheses=list(obj.get("hypotheses", [])),
            raw=obj,
        )

    def to_json(self, indent: int = 2) -> str:
        """Pretty JSON for agent pipelines / CLI printing."""
        return json.dumps(self.raw, indent=indent, default=str)


def _opt_float(v: Any) -> Optional[float]:
    return float(v) if isinstance(v, (int, float)) else None