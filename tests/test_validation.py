"""
Unit and Integration Tests for Real PLC Validation Logic.
Verifies priority register selection, batch planning, safe failure handling,
and scaling reporting using MockSLMPServer.
"""
import os
import unittest

from quad_gateway.validate_plc import TARGET_REGISTER_TAGS, run_plc_validation
from quad_gateway.config.config_loader import load_machine_config


class TestPLCValidation(unittest.TestCase):
    """
    Tests the PLC validation workflow in quad_gateway/validate_plc.py.
    """

    def setUp(self):
        self.config_path = os.path.join(
            os.path.dirname(__file__), "..", "quad_gateway", "config", "machine.json"
        )

    def test_target_registers_exist_in_config(self):
        """
        Verifies all 7 Phase 1 priority validation registers exist in machine.json.
        """
        config = load_machine_config(self.config_path)
        configured_tags = {r.tag for r in config.registers}
        for target_tag in TARGET_REGISTER_TAGS:
            self.assertIn(
                target_tag,
                configured_tags,
                f"Target register {target_tag} missing from machine.json"
            )

    def test_cycle_time_not_shot_time(self):
        """
        Verifies D1127 is strictly configured as CYCLE_TIME and has unresolved scale factor.
        """
        config = load_machine_config(self.config_path)
        cycle_time_reg = next(r for r in config.registers if r.tag == "CYCLE_TIME")
        self.assertEqual(cycle_time_reg.address, "D1127")
        self.assertEqual(cycle_time_reg.data_type, "decimal_scaled")
        self.assertEqual(cycle_time_reg.unit, "sec")
        # Scale factor must be None (unresolved - no guessing)
        self.assertIsNone(cycle_time_reg.scale_factor)

    def test_mock_validation_priority_subset(self):
        """
        Verifies running validation against mock PLC succeeds for the 7 priority registers.
        """
        success, summary = run_plc_validation(
            config_path=self.config_path,
            use_mock=True,
            all_registers=False
        )
        self.assertTrue(success)
        self.assertIsNotNone(summary)
        self.assertEqual(summary["points_count"], 7)
        self.assertEqual(summary["quality"], "GOOD")
        self.assertTrue(summary["all_good"])

    def test_mock_validation_all_registers(self):
        """
        Verifies running validation with all_registers=True covers all 60 registers.
        """
        success, summary = run_plc_validation(
            config_path=self.config_path,
            use_mock=True,
            all_registers=True
        )
        self.assertTrue(success)
        self.assertIsNotNone(summary)
        self.assertEqual(summary["points_count"], 60)
        self.assertEqual(summary["quality"], "GOOD")

    def test_unreachable_plc_fails_safely(self):
        """
        Verifies attempting to validate against an unreachable host fails safely
        without raising unhandled exceptions and returns False.
        """
        success, summary = run_plc_validation(
            config_path=self.config_path,
            use_mock=False,
            override_host="127.0.0.1",
            override_port=59998,  # Unused port
        )
        self.assertFalse(success)
        self.assertIsNone(summary)


if __name__ == "__main__":
    unittest.main()
