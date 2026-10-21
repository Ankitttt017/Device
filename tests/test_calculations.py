"""
Unit tests for the production-report calculation engine (quad_gateway.processing.calculations).
Expected values are taken from the Rico production report of UBE 850 T - 02 (21/10/2026).
"""
import os
import unittest

from quad_gateway.config.config_loader import (
    ConfigurationError,
    MachineConfig,
    RegisterConfig,
    load_machine_config,
)
from quad_gateway.models.telemetry import AcquisitionEvent, TelemetryDataPoint
from quad_gateway.processing.calculations import ShotCalculator, round_value

RULES = {
    "enabled": True,
    "shot_result": {"tag": "SHOT_RESULT", "source": "SHOT_STATUS", "map": {"1": "OK", "3": "WARM_UP", "5": "NG"}},
    "derived": [
        {"tag": "CLAMP_TONNAGE_HE_LOW_MN_CALC", "source": "CLAMP_TONNAGE_HE_LOW", "factor": 0.085,
         "decimals": 2, "rounding": "half_up", "unit": "MN", "with_limits": True},
        {"tag": "CLAMP_TONNAGE_T_CALC", "source": "CLAMP_FORCE_PCT", "factor": 8.5,
         "decimals": 0, "rounding": "down", "unit": "T"},
    ],
    "limit_status": True,
}
TAGS = [
    "SHOT_STATUS", "CLAMP_FORCE_PCT",
    "CLAMP_TONNAGE_HE_LOW", "CLAMP_TONNAGE_HE_LOW_UPPER", "CLAMP_TONNAGE_HE_LOW_LOWER",
    "BISCUIT_THICKNESS", "BISCUIT_THICKNESS_UPPER", "BISCUIT_THICKNESS_LOWER",
]


def make_config(rules=RULES, tags=TAGS) -> MachineConfig:
    return MachineConfig(
        gateway_id="QUAD-01", gateway_name="Test", machine_id="UBE-850T-02", machine_name="UBE 850 T - 02",
        protocol="SLMP_3E_BINARY", host="127.0.0.1", port=5005, read_only=True, timeout_seconds=1.0,
        monitoring_timer_250ms=4, poll_interval_seconds=1.0,
        registers=[RegisterConfig(tag=t, address=f"D{9000 + i}", device_type="D", device_number=9000 + i,
                                  data_type="int16") for i, t in enumerate(tags)],
        calculations=rules,
    )


def make_event(values: dict) -> AcquisitionEvent:
    points = [
        TelemetryDataPoint(tag=t, address="D0", raw_value=v, value=v, quality="BAD" if v is None else "GOOD")
        for t, v in values.items()
    ]
    return AcquisitionEvent(gateway_id="QUAD-01", machine_id="UBE-850T-02", points=points)


def calc_points(event: AcquisitionEvent) -> dict:
    return {p.tag: p for p in event.points if p.address == "CALC"}


# Report shot #2567: Clamp Force 95 %, HE.Low limits 94-106 %, Biscuit 23 mm (17-30)
GOOD_SHOT = {
    "SHOT_STATUS": 1, "CLAMP_FORCE_PCT": 95,
    "CLAMP_TONNAGE_HE_LOW": 95, "CLAMP_TONNAGE_HE_LOW_UPPER": 106, "CLAMP_TONNAGE_HE_LOW_LOWER": 94,
    "BISCUIT_THICKNESS": 23, "BISCUIT_THICKNESS_UPPER": 30, "BISCUIT_THICKNESS_LOWER": 17,
}


class TestShotCalculator(unittest.TestCase):

    def test_shot_result_mapping_from_d1301(self):
        calc = ShotCalculator(make_config())
        for status, expected in ((1, "OK"), (3, "WARM_UP"), (5, "NG"), (7, "UNKNOWN(7)")):
            event = calc.apply(make_event({**GOOD_SHOT, "SHOT_STATUS": status}))
            point = calc_points(event)["SHOT_RESULT"]
            self.assertEqual(point.value, expected)
            self.assertEqual(point.raw_value, status)

    def test_shot_result_bad_when_d1301_unreadable(self):
        event = ShotCalculator(make_config()).apply(make_event({**GOOD_SHOT, "SHOT_STATUS": None}))
        point = calc_points(event)["SHOT_RESULT"]
        self.assertIsNone(point.value)
        self.assertEqual(point.quality, "BAD")

    def test_clamp_tonnage_matches_report(self):
        event = ShotCalculator(make_config()).apply(make_event(GOOD_SHOT))
        pts = calc_points(event)
        self.assertEqual(pts["CLAMP_TONNAGE_HE_LOW_MN_CALC"].value, 8.08)
        self.assertEqual(pts["CLAMP_TONNAGE_HE_LOW_MN_CALC_UPPER"].value, 9.01)
        self.assertEqual(pts["CLAMP_TONNAGE_HE_LOW_MN_CALC_LOWER"].value, 7.99)
        self.assertEqual(pts["CLAMP_TONNAGE_T_CALC"].value, 807)
        self.assertEqual(pts["CLAMP_TONNAGE_T_CALC"].unit, "T")

    def test_report_rounding(self):
        self.assertEqual(round_value(95 * 0.085, 2), 8.08)      # half-up, not banker's/float 8.07
        self.assertEqual(round_value(999 * 0.085, 2), 84.92)
        self.assertEqual(round_value(101 * 8.5, 0, "down"), 858)
        self.assertEqual(round_value(93 * 8.5, 0, "down"), 790)

    def test_limit_status_inclusive(self):
        calc = ShotCalculator(make_config())
        for biscuit, expected in ((23, "OK"), (17, "OK"), (30, "OK"), (8, "NG"), (31, "NG")):
            event = calc.apply(make_event({**GOOD_SHOT, "BISCUIT_THICKNESS": biscuit}))
            self.assertEqual(calc_points(event)["BISCUIT_THICKNESS_STATUS"].value, expected)

    def test_out_of_limit_summary(self):
        event = ShotCalculator(make_config()).apply(make_event({**GOOD_SHOT, "BISCUIT_THICKNESS": 8}))
        summary = calc_points(event)["OUT_OF_LIMIT_PARAMS"]
        self.assertEqual(summary.value, ["BISCUIT_THICKNESS"])
        self.assertEqual(summary.raw_value, 1)

    def test_bad_source_gives_bad_points_without_guessing(self):
        event = ShotCalculator(make_config()).apply(make_event({**GOOD_SHOT, "CLAMP_TONNAGE_HE_LOW": None}))
        pts = calc_points(event)
        self.assertEqual(pts["CLAMP_TONNAGE_HE_LOW_MN_CALC"].quality, "BAD")
        self.assertEqual(pts["CLAMP_TONNAGE_HE_LOW_STATUS"].quality, "BAD")
        self.assertNotIn("CLAMP_TONNAGE_HE_LOW", pts["OUT_OF_LIMIT_PARAMS"].value)

    def test_raw_register_points_untouched(self):
        event = ShotCalculator(make_config()).apply(make_event(GOOD_SHOT))
        raw = {p.tag: p.value for p in event.points if p.address != "CALC"}
        self.assertEqual(raw, GOOD_SHOT)

    def test_disabled_calculations_do_nothing(self):
        event = ShotCalculator(make_config(rules={})).apply(make_event(GOOD_SHOT))
        self.assertEqual(len(event.points), len(GOOD_SHOT))

    def test_unknown_source_tag_rejected(self):
        with self.assertRaises(ConfigurationError):
            ShotCalculator(make_config(tags=["SHOT_STATUS"]))

    def test_machine_json_calculation_rules_valid(self):
        path = os.path.join(os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json")
        config = load_machine_config(path)
        calc = ShotCalculator(config)
        self.assertTrue(calc.enabled)
        self.assertEqual(config.calculations["shot_result"]["map"], {"1": "OK", "3": "WARM_UP", "5": "NG"})


if __name__ == "__main__":
    unittest.main()
