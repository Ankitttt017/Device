"""
Real Mitsubishi PLC Validation Module for QUAD Gateway.
Phase 1: Validates network connectivity and initial register set against
the physical Mitsubishi PLC (or Mock PLC for offline verification).

Target Initial Validation Registers:
- D947   HIGH_SHOT_COUNT   (int16)
- D955   NG_COUNTER        (int16)
- D1120  SHOT_NO           (int16)
- D1127  CYCLE_TIME        (decimal_scaled, unit: sec) - ONLY CYCLE_TIME
- D1301  SHOT_STATUS       (int16)
- M840   CYCLE_START       (bit)
- M4598  CYCLE_END         (bit)

Strict Safety:
- Strictly read-only (SLMP 0x0401 Read only, no write commands exist).
- Configuration-driven from machine.json (no hardcoded IP/port/protocol).
- Unapproved memory protection: only reads exact contiguous blocks.
- Unresolved scaling: raw value exposed without inventing scaling factors.
"""
import argparse
import dataclasses
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from quad_gateway.acquisition.collector import AcquisitionCollector
from quad_gateway.config.config_loader import (
    ConfigurationError,
    MachineConfig,
    RegisterConfig,
    load_machine_config,
)
from quad_gateway.drivers.slmp.slmp_driver import (
    SLMPConnectionError,
    SLMPDriver,
    SLMPDriverError,
    SLMPTimeoutError,
)
from quad_gateway.utils.logger import setup_logger

logger = setup_logger("quad_gateway.validate_plc")

# Phase 1 Target Initial Validation Registers
TARGET_REGISTER_TAGS = [
    "HIGH_SHOT_COUNT",
    "NG_COUNTER",
    "SHOT_NO",
    "CYCLE_TIME",
    "SHOT_STATUS",
    "CYCLE_START",
    "CYCLE_END",
]


def format_validation_table(
    points: list, registers_by_tag: Dict[str, RegisterConfig]
) -> str:
    """
    Renders a detailed validation table showing:
    TAG, ADDRESS, TYPE, RAW VAL, DECODED VAL, SCALING STATUS, UNIT, QUALITY.
    """
    lines = []
    header = (
        f"{'TAG':<20} {'ADDRESS':<10} {'TYPE':<16} {'RAW VAL':<12} "
        f"{'DECODED VAL':<15} {'SCALING':<26} {'UNIT':<8} {'QUALITY'}"
    )
    separator = "=" * len(header)
    lines.append(separator)
    lines.append(header)
    lines.append(separator)

    for p in points:
        reg = registers_by_tag.get(p.tag)
        raw_str = str(p.raw_value) if p.raw_value is not None else "ERR"
        val_str = str(p.value) if p.value is not None else "ERR"
        unit_str = str(p.unit) if p.unit is not None else "-"

        # Scaling transparency: do not invent scaling factors
        if reg and reg.data_type == "decimal_scaled":
            if reg.scale_factor is None:
                scaling_str = "UNRESOLVED (raw exposed)"
            else:
                scaling_str = f"Factor: {reg.scale_factor}"
        else:
            scaling_str = "N/A"

        lines.append(
            f"{p.tag:<20} {p.address:<10} {p.data_type:<16} {raw_str:<12} "
            f"{val_str:<15} {scaling_str:<26} {unit_str:<8} {p.quality}"
        )
    lines.append(separator)
    return "\n".join(lines)


def run_plc_validation(
    config_path: str,
    use_mock: bool = False,
    override_host: Optional[str] = None,
    override_port: Optional[int] = None,
    all_registers: bool = False,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Executes real PLC validation against target registers.
    Fails safely if the PLC is unreachable.

    Returns:
        (success: bool, results_summary: Optional[dict])
    """
    logger.info("================================================================================")
    logger.info("       QUAD GATEWAY — PHASE 1: REAL MITSUBISHI PLC VALIDATION")
    logger.info("================================================================================")

    # 1. Load configuration (configuration-driven, never hardcoded)
    logger.info(f"Loading configuration from: {config_path}")
    try:
        config = load_machine_config(config_path)
    except ConfigurationError as e:
        logger.error(f"Configuration validation failed: {e}")
        return False, None

    mock_server = None
    if use_mock:
        from tests.mock_plc import MockSLMPServer

        logger.info("[MODE: MOCK SIMULATION] Starting local Mock SLMP PLC server...")
        mock_server = MockSLMPServer(host="127.0.0.1", port=0)
        mock_server.start()
        host = "127.0.0.1"
        port = mock_server.port
        logger.info(f"Mock SLMP server listening on {host}:{port}")
    else:
        host = override_host or config.host
        port = override_port or config.port
        logger.info("[MODE: REAL PLC] Connecting to physical machine PLC...")

    # Log connection metadata
    logger.info(f"  Target Host:     {host}")
    logger.info(f"  Target Port:     {port}")
    logger.info(f"  Protocol:        {config.protocol}")
    logger.info(f"  Read-Only Mode:  {config.read_only} (SLMP 0x0401 Read only)")
    logger.info(f"  Timeout:         {config.timeout_seconds}s")
    logger.info(f"  Machine ID:      {config.machine_id} ({config.machine_name})")

    # Filter target registers
    if all_registers:
        selected_registers = config.registers
        logger.info(f"Validating ALL {len(selected_registers)} configured registers.")
    else:
        selected_registers = [
            r for r in config.registers if r.tag in TARGET_REGISTER_TAGS
        ]
        logger.info(
            f"Validating Phase 1 Priority Register Set ({len(selected_registers)} registers): "
            f"{', '.join(r.tag for r in selected_registers)}"
        )

    # Sanity checks on target registers
    for r in selected_registers:
        if r.tag == "CYCLE_TIME":
            logger.info(
                f"  Registered D1127 as CYCLE_TIME (Unit: {r.unit}, Scale: "
                f"{r.scale_factor if r.scale_factor is not None else 'UNRESOLVED - raw value exposed'}). "
                "Note: D1127 is strictly CYCLE_TIME, never SHOT_TIME."
            )

    registers_by_tag = {r.tag: r for r in selected_registers}
    sub_config = dataclasses.replace(config, registers=selected_registers)

    # 2. Initialize SLMP Driver (read_only=True strictly enforced)
    driver = SLMPDriver(
        host=host,
        port=port,
        timeout_seconds=config.timeout_seconds,
        monitoring_timer_250ms=config.monitoring_timer_250ms,
        read_only=True,
    )

    collector = AcquisitionCollector(config=sub_config, driver=driver)

    # 3. Connect to PLC (fail safely if unreachable)
    logger.info(f"Attempting TCP connection to PLC at {host}:{port}...")
    conn_start = time.time()
    try:
        driver.connect()
        conn_duration = time.time() - conn_start
        logger.info(
            f"[CONNECTION SUCCESS] Connected to PLC at {host}:{port} in {conn_duration:.3f}s."
        )
    except (SLMPConnectionError, SLMPTimeoutError, OSError) as e:
        conn_duration = time.time() - conn_start
        logger.error(
            f"[CONNECTION FAILED] Could not reach PLC at {host}:{port} after {conn_duration:.3f}s: {e}"
        )
        logger.warning(
            "================================================================================"
        )
        logger.warning("  [VALIDATION FAILED] PLC is unreachable from this workstation.")
        logger.warning(f"  Target: {host}:{port} via {config.protocol}")
        logger.warning("  Diagnosis:")
        logger.warning(
            "    1. Workstation is not connected to the PLC subnet (e.g. 192.168.117.x)."
        )
        logger.warning("    2. Ethernet cable disconnected or PLC powered off.")
        logger.warning("    3. Local or network firewall is blocking TCP port 5002.")
        logger.warning(
            "================================================================================"
        )
        if mock_server:
            mock_server.stop()
        return False, None

    # 4. Acquire validation cycle
    try:
        logger.info("Executing read-only acquisition of validation registers...")
        event = collector.collect_cycle()

        table_str = format_validation_table(event.points, registers_by_tag)
        print("\n" + table_str + "\n")

        # Log each register explicitly
        for p in event.points:
            reg = registers_by_tag.get(p.tag)
            scaling_desc = (
                "unresolved (raw preserved)"
                if reg and reg.data_type == "decimal_scaled" and reg.scale_factor is None
                else (f"factor={reg.scale_factor}" if reg and reg.scale_factor else "direct")
            )
            logger.info(
                f"  Register {p.address:<6} ({p.tag:<16}): raw={p.raw_value}, "
                f"decoded={p.value} {p.unit or ''} [quality={p.quality}, scaling={scaling_desc}]"
            )

        all_good = all(p.quality == "GOOD" for p in event.points)
        if all_good:
            if use_mock:
                logger.info(
                    "[MOCK VALIDATION SUCCESS] All target registers acquired cleanly from Mock PLC."
                )
            else:
                logger.info(
                    f"[REAL PLC VALIDATION SUCCESS] Real Mitsubishi PLC at {host}:{port} "
                    "successfully validated! Real register values returned."
                )
        else:
            logger.warning(
                "[VALIDATION WARNING] One or more registers returned BAD quality."
            )

        summary = {
            "host": host,
            "port": port,
            "protocol": config.protocol,
            "read_only": config.read_only,
            "points_count": len(event.points),
            "quality": event.quality,
            "is_mock": use_mock,
            "all_good": all_good,
        }
        return all_good, summary

    finally:
        driver.disconnect()
        if mock_server:
            mock_server.stop()
        logger.info("PLC connection closed safely.")


def main():
    parser = argparse.ArgumentParser(
        description="QUAD Gateway — Phase 1: Real Mitsubishi PLC Validation"
    )
    default_cfg = os.path.join(os.path.dirname(__file__), "config", "machine.json")
    parser.add_argument(
        "--config", default=default_cfg, help="Path to machine.json configuration"
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Run validation against local Mock SLMP PLC server (for offline testing)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Validate all 60 configured registers instead of priority subset",
    )
    parser.add_argument(
        "--host", default=None, help="Override PLC host IP (e.g. for testing)"
    )
    parser.add_argument(
        "--port", type=int, default=None, help="Override PLC port"
    )

    args = parser.parse_args()

    success, _ = run_plc_validation(
        config_path=args.config,
        use_mock=args.mock,
        override_host=args.host,
        override_port=args.port,
        all_registers=args.all,
    )

    if not success:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
