"""
Normalized Telemetry Models for QUAD Gateway.
Defines structured, immutable data points and acquisition event envelopes.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid


@dataclass(frozen=True)
class TelemetryDataPoint:
    """
    Normalized individual data point acquired from PLC.
    """
    tag: str
    address: str
    raw_value: Any
    value: Any
    unit: Optional[str] = None
    data_type: str = "int16"
    quality: str = "GOOD"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AcquisitionEvent:
    """
    Normalized event envelope produced by PLC acquisition cycle.
    Contains an immutable event_id (UUIDv4) generated at acquisition time.
    """
    gateway_id: str
    machine_id: str
    points: List[TelemetryDataPoint] = field(default_factory=list)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quality: str = "GOOD"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "gateway_id": self.gateway_id,
            "machine_id": self.machine_id,
            "timestamp": self.timestamp,
            "quality": self.quality,
            "data": [p.to_dict() for p in self.points]
        }
