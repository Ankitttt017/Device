"""
Shot Calculation Engine for QUAD Gateway.
Applies the production-report calculation rules to every acquired shot,
driven entirely by the "calculations" section of machine.json.

Rules (same as the Rico production report):
1. Shot Result      : D1301 (SHOT_STATUS) mapped to text -> 1 = OK, 3 = WARM_UP, 5 = NG.
2. Derived values   : value = source x factor (e.g. Clamp Tonnage MN = % x 0.085,
                      Clamp Tonnage T = Clamp Force % x 8.5), optionally for the
                      source's _UPPER / _LOWER limits as well.
3. Limit status     : <TAG>_STATUS = OK when LOWER <= actual <= UPPER, else NG
                      (informational only; the shot result always comes from D1301).

Guarantees:
- Raw PLC values are never modified; calculated values are appended as extra points.
- A missing or BAD source produces a BAD calculated point (no guessing).
"""
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
import logging
from typing import Any, Dict, List, Optional

from quad_gateway.config.config_loader import ConfigurationError, MachineConfig
from quad_gateway.models.telemetry import AcquisitionEvent, TelemetryDataPoint

logger = logging.getLogger("quad_gateway.calculations")

CALC_ADDRESS = "CALC"
ROUNDING_MODES = {"half_up": ROUND_HALF_UP, "down": ROUND_DOWN}


def round_value(value: float, decimals: Optional[int], rounding: str = "half_up") -> Any:
    """
    Rounds like the production report (decimal half-up, or truncate with 'down').
    Returns int when decimals == 0, float otherwise. None keeps full precision.
    """
    if decimals is None:
        return value
    quantum = Decimal(1).scaleb(-decimals)
    result = Decimal(str(value)).quantize(quantum, rounding=ROUNDING_MODES[rounding])
    return int(result) if decimals == 0 else float(result)


class ShotCalculator:
    """
    Appends calculated points (shot result, derived values, limit status) to an AcquisitionEvent.
    """

    def __init__(self, config: MachineConfig):
        self.rules: Dict[str, Any] = config.calculations or {}
        self.enabled = bool(self.rules.get("enabled", False))
        self.register_tags = [r.tag for r in config.registers]
        if self.enabled:
            self._validate(set(self.register_tags))

    def _validate(self, known_tags: set) -> None:
        shot = self.rules.get("shot_result")
        if shot and shot.get("source") not in known_tags:
            raise ConfigurationError(f"shot_result source '{shot.get('source')}' is not a configured register.")
        for d in self.rules.get("derived", []):
            if d.get("source") not in known_tags:
                raise ConfigurationError(f"Derived '{d.get('tag')}' source '{d.get('source')}' is not a configured register.")
            if d.get("rounding", "half_up") not in ROUNDING_MODES:
                raise ConfigurationError(f"Derived '{d.get('tag')}' has invalid rounding '{d.get('rounding')}'.")
            for suffix in ("_UPPER", "_LOWER"):
                if d.get("with_limits") and d["source"] + suffix not in known_tags:
                    raise ConfigurationError(f"Derived '{d['tag']}' needs register '{d['source']}{suffix}'.")

    def apply(self, event: AcquisitionEvent) -> AcquisitionEvent:
        if not self.enabled:
            return event

        points = {p.tag: p for p in event.points}
        calculated: List[TelemetryDataPoint] = []

        # 1. Shot result from D1301
        shot = self.rules.get("shot_result")
        if shot:
            calculated.append(self._shot_result(shot, points.get(shot["source"])))

        # 2. Derived values (x factor), including limits when requested
        for d in self.rules.get("derived", []):
            pairs = [(d["tag"], d["source"])]
            if d.get("with_limits"):
                pairs += [(d["tag"] + s, d["source"] + s) for s in ("_UPPER", "_LOWER")]
            for tag, source in pairs:
                point = self._derived(tag, points.get(source), d)
                calculated.append(point)
                points[tag] = point

        # 3. Limit status for every tag that has _UPPER and _LOWER
        if self.rules.get("limit_status", False):
            out_of_limit = []
            for tag in self.register_tags + [d["tag"] for d in self.rules.get("derived", [])]:
                upper, lower = points.get(tag + "_UPPER"), points.get(tag + "_LOWER")
                if upper is None or lower is None:
                    continue
                status = self._limit_status(tag, points.get(tag), upper, lower)
                calculated.append(status)
                if status.value == "NG":
                    out_of_limit.append(tag)
            calculated.append(
                TelemetryDataPoint(
                    tag="OUT_OF_LIMIT_PARAMS",
                    address=CALC_ADDRESS,
                    raw_value=len(out_of_limit),
                    value=out_of_limit,
                    data_type="calculated",
                )
            )

        event.points.extend(calculated)
        return event

    def _shot_result(self, rule: Dict[str, Any], src: Optional[TelemetryDataPoint]) -> TelemetryDataPoint:
        tag = rule.get("tag", "SHOT_RESULT")
        if src is None or src.quality != "GOOD" or src.value is None:
            return TelemetryDataPoint(tag=tag, address=CALC_ADDRESS, raw_value=None, value=None,
                                      data_type="calculated", quality="BAD")
        text = rule.get("map", {}).get(str(src.value))
        if text is None:
            logger.warning(f"Unknown {rule['source']} value {src.value}; shot result set to UNKNOWN.")
            text = f"UNKNOWN({src.value})"
        return TelemetryDataPoint(tag=tag, address=CALC_ADDRESS, raw_value=src.value, value=text,
                                  data_type="calculated")

    def _derived(self, tag: str, src: Optional[TelemetryDataPoint], rule: Dict[str, Any]) -> TelemetryDataPoint:
        if src is None or src.quality != "GOOD" or not isinstance(src.value, (int, float)):
            return TelemetryDataPoint(tag=tag, address=CALC_ADDRESS, raw_value=None, value=None,
                                      unit=rule.get("unit"), data_type="calculated", quality="BAD")
        value = round_value(src.value * float(rule["factor"]), rule.get("decimals"), rule.get("rounding", "half_up"))
        return TelemetryDataPoint(tag=tag, address=CALC_ADDRESS, raw_value=src.value, value=value,
                                  unit=rule.get("unit"), data_type="calculated")

    def _limit_status(
        self,
        tag: str,
        actual: Optional[TelemetryDataPoint],
        upper: TelemetryDataPoint,
        lower: TelemetryDataPoint,
    ) -> TelemetryDataPoint:
        status_tag = tag + "_STATUS"
        values = [p.value if p is not None and p.quality == "GOOD" else None for p in (actual, lower, upper)]
        if not all(isinstance(v, (int, float)) for v in values):
            return TelemetryDataPoint(tag=status_tag, address=CALC_ADDRESS, raw_value=None, value=None,
                                      data_type="calculated", quality="BAD")
        act, low, up = values
        return TelemetryDataPoint(
            tag=status_tag,
            address=CALC_ADDRESS,
            raw_value=f"{low} <= {act} <= {up}",
            value="OK" if low <= act <= up else "NG",
            data_type="calculated",
        )
