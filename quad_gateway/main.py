"""
QUAD Gateway — Phase 1 Entry Point
Machine: UBE 850 T - 02
Protocol: Mitsubishi SLMP 3E Binary (Read-Only)

Target Flow:
PLC -> SLMP 3E Binary -> Python Driver -> Collector -> Normalized Console Output
"""
import argparse
from datetime import datetime
import json
import os
import signal
import sys
import time
from typing import Optional

from quad_gateway.acquisition.collector import AcquisitionCollector
from quad_gateway.config.config_loader import ConfigurationError, load_machine_config
from quad_gateway.drivers.slmp.slmp_driver import (
    SLMPConnectionError,
    SLMPDriver,
    SLMPDriverError,
    SLMPTimeoutError,
)
from quad_gateway.mqtt.publisher import FakeMQTTPublisher, MQTTPublisher
from quad_gateway.storage.database import StorageManager
from quad_gateway.sync.sync_manager import SyncManager
from quad_gateway.utils.logger import setup_logger

logger = setup_logger("quad_gateway", log_file="logs/gateway.log")


def format_points_table(
    points: list,
    title_banner: Optional[str] = None,
    cycle_end_time: Optional[str] = None,
) -> str:
    """
    Renders telemetry points as a clean industrial console table.
    """
    lines = []
    header = f"{'TAG':<32} {'ADDRESS':<10} {'TYPE':<15} {'RAW VAL':<15} {'DECODED VAL':<25} {'UNIT':<10} {'STATUS'}"
    separator = "-" * len(header)
    if title_banner:
        lines.append("=" * len(header))
        lines.append(title_banner)
    lines.append(separator)
    lines.append(header)
    lines.append(separator)

    if cycle_end_time:
        lines.append(
            f"{'CYCLE_END_TIME':<32} {'M4598':<10} {'datetime':<15} {'1':<15} {cycle_end_time:<25} {'IST':<10} GOOD"
        )

    for p in points:
        raw_str = str(p.raw_value) if p.raw_value is not None else "ERR"
        val_str = str(p.value) if p.value is not None else "ERR"
        unit_str = str(p.unit) if p.unit is not None else ""
        lines.append(
            f"{p.tag:<32} {p.address:<10} {p.data_type:<15} {raw_str:<15} {val_str:<25} {unit_str:<10} {p.quality}"
        )
    lines.append(separator)
    return "\n".join(lines)


def run_gateway(
    config_path: str,
    single_cycle: bool = False,
    override_host: Optional[str] = None,
    override_port: Optional[int] = None,
    use_mock: bool = False,
    override_db: Optional[str] = None,
    no_mqtt: bool = False,
    fake_mqtt: bool = False,
    force_continuous: bool = False,
    override_trigger_bit: Optional[str] = None,
    override_trigger_poll_ms: Optional[int] = None,
):
    """
    Initializes and executes QUAD Gateway with local SQLite persistence and MQTT synchronization.
    """
    logger.info("==================================================")
    logger.info("  QUAD GATEWAY — RUNTIME STARTUP (SQLITE + MQTT)")
    logger.info("==================================================")

    # 1. Load and validate configuration
    logger.info(f"Loading configuration from: {config_path}")
    try:
        config = load_machine_config(config_path)
        logger.info(
            f"Configuration loaded successfully for Machine: '{config.machine_name}' "
            f"(ID: {config.machine_id}) on Gateway: '{config.gateway_name}'"
        )
    except ConfigurationError as e:
        logger.error(f"Configuration validation error: {e}")
        sys.exit(1)

    mock_server = None
    if use_mock:
        from tests.mock_plc import MockSLMPServer
        logger.info("Starting local Mock SLMP PLC Server...")
        mock_server = MockSLMPServer(host="127.0.0.1", port=0)
        mock_server.start()
        host = "127.0.0.1"
        port = mock_server.port
        logger.info(f"Mock SLMP PLC Server active on {host}:{port}")
    else:
        host = override_host or config.host
        port = override_port or config.port

    logger.info(f"Target PLC: {host}:{port} via {config.protocol} (Read-Only: {config.read_only})")
    logger.info(f"Configured registers count: {len(config.registers)}")

    # 2. Initialize SQLite Storage Layer
    db_path = override_db or config.database_path
    logger.info(f"Initializing SQLite storage at: {db_path}")
    storage = StorageManager(db_path=db_path)
    storage.sync_machine_config(config)

    # 3. Initialize MQTT Publisher and Sync Manager (Phase 3)
    mqtt_publisher = None
    sync_manager = None
    if config.mqtt.enabled and not no_mqtt:
        if fake_mqtt:
            logger.info("Starting in-memory Fake MQTT Publisher for testing...")
            mqtt_publisher = FakeMQTTPublisher(
                host=config.mqtt.host,
                port=config.mqtt.port,
                client_id=config.mqtt.client_id,
            )
        else:
            mqtt_publisher = MQTTPublisher(
                host=config.mqtt.host,
                port=config.mqtt.port,
                client_id=config.mqtt.client_id,
                username=config.mqtt.username,
                password=config.mqtt.password,
                keepalive=config.mqtt.keepalive,
                qos=config.mqtt.qos,
                retain=config.mqtt.retain,
            )
        mqtt_publisher.start()
        sync_manager = SyncManager(
            storage=storage,
            publisher=mqtt_publisher,
            mqtt_config=config.mqtt,
            machine_id=config.machine_id,
        )

    # 4. Instantiate SLMP Driver
    driver = SLMPDriver(
        host=host,
        port=port,
        timeout_seconds=config.timeout_seconds,
        monitoring_timer_250ms=config.monitoring_timer_250ms,
        read_only=config.read_only
    )

    # 3. Instantiate Acquisition Engine
    collector = AcquisitionCollector(config=config, driver=driver)

    # 4. Handle graceful shutdown
    stop_requested = False

    def handle_sigint(signum, frame):
        nonlocal stop_requested
        logger.info("Shutdown signal received. Stopping acquisition...")
        stop_requested = True

    signal.signal(signal.SIGINT, handle_sigint)

    # 5. Connect and run acquisition
    try:
        driver.connect()
    except (SLMPConnectionError, SLMPTimeoutError) as e:
        logger.error(f"Could not establish initial connection to PLC: {e}")
        if single_cycle:
            if mock_server:
                mock_server.stop()
            sys.exit(1)

    cycle_count = 0
    try:
        if single_cycle:
            # Single acquisition cycle (e.g. for testing / one-shot validation)
            logger.info("--- Executing Single Acquisition Cycle (--once) ---")
            cycle_start = time.time()
            event = collector.collect_cycle(trigger_type="MANUAL_SINGLE")
            duration = time.time() - cycle_start
            logger.info(
                f"Single cycle completed in {duration:.3f}s. "
                f"Points: {len(event.points)}, Event UUID: {event.event_id}, Quality: {event.quality}"
            )
            storage.insert_event_and_queue(event)
            logger.info(
                f"Successfully persisted event {event.event_id} to SQLite ({db_path}) with status PENDING."
            )
            if sync_manager:
                try:
                    sync_res = sync_manager.sync_pending_events()
                    if sync_res["published"] > 0:
                        logger.info(f"MQTT Sync: {sync_res['published']} event(s) published.")
                except Exception as e:
                    logger.warning(f"MQTT sync error: {e}")
            print(format_points_table(event.points, cycle_end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        elif force_continuous or config.acquisition_mode == "continuous":
            # Continuous timer-based polling
            logger.info(f"Starting continuous polling mode (interval: {config.poll_interval_seconds}s)...")
            while not stop_requested:
                cycle_count += 1
                logger.info(f"--- Starting Acquisition Cycle #{cycle_count} ---")
                cycle_start = time.time()
                try:
                    event = collector.collect_cycle(trigger_type="PERIODIC")
                    duration = time.time() - cycle_start
                    logger.info(
                        f"Acquisition Cycle #{cycle_count} completed in {duration:.3f}s. "
                        f"Points: {len(event.points)}, Event UUID: {event.event_id}, Quality: {event.quality}"
                    )
                    storage.insert_event_and_queue(event)
                    logger.info(
                        f"Successfully persisted event {event.event_id} to SQLite ({db_path}) with status PENDING."
                    )
                    if sync_manager:
                        try:
                            sync_res = sync_manager.sync_pending_events()
                            if sync_res["published"] > 0:
                                logger.info(
                                    f"MQTT Sync: {sync_res['published']} event(s) published to broker."
                                )
                        except Exception as e:
                            logger.warning(f"Non-fatal error in MQTT sync cycle: {e}")
                    print(format_points_table(event.points, cycle_end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                except Exception as e:
                    logger.error(f"Unexpected error during acquisition cycle: {e}")

                time.sleep(config.poll_interval_seconds)

        else:
            # High-Speed Shot/Cycle-Triggered Mode on M4598 (Approach B: Shot/Cycle-Triggered)
            trigger_bit = override_trigger_bit or config.trigger_bit
            trigger_poll_ms = override_trigger_poll_ms or config.trigger_poll_ms
            poll_sleep_sec = max(trigger_poll_ms / 1000.0, 0.01)

            logger.info("==================================================")
            logger.info("  CYCLE-TRIGGERED ACQUISITION ENGINE (M4598)      ")
            logger.info(f"  Trigger Bit:      {trigger_bit} (CYCLE_END)")
            logger.info(f"  Trigger Poll:     {trigger_poll_ms} ms")
            logger.info(f"  Detection:        Rising Edge (0 -> 1)")
            logger.info(f"  Storage Target:   {db_path}")
            logger.info("==================================================")

            # Determine initial state of trigger bit
            try:
                initial_bit = collector.read_bit_value(trigger_bit)
            except Exception as e:
                logger.warning(f"Could not read initial state of {trigger_bit}: {e}. Defaulting to 0.")
                initial_bit = 0

            last_bit_state = initial_bit
            if initial_bit == 1:
                logger.info(
                    f"Trigger bit {trigger_bit} is currently HIGH (1). "
                    "Waiting for cycle reset (1 -> 0 -> 1)..."
                )
            else:
                logger.info(
                    f"Trigger bit {trigger_bit} is currently LOW (0). "
                    "ARMED: Monitoring for machine Shot Completion (0 -> 1)..."
                )

            consecutive_errors = 0
            while not stop_requested:
                try:
                    current_bit = collector.read_bit_value(trigger_bit)
                    consecutive_errors = 0
                except (SLMPConnectionError, SLMPTimeoutError, OSError) as e:
                    consecutive_errors += 1
                    if consecutive_errors == 1 or consecutive_errors % 10 == 0:
                        logger.warning(f"Connection issue polling trigger bit {trigger_bit}: {e}. Waiting 3s for PLC channel reset...")
                    time.sleep(3.0)
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error polling trigger bit {trigger_bit}: {e}")
                    time.sleep(2.0)
                    continue

                # Rising edge: 0 -> 1 (Shot complete!)
                if last_bit_state == 0 and current_bit == 1:
                    cycle_count += 1
                    shot_start = time.time()
                    logger.info("=" * 70)
                    logger.info(
                        f"⚡ [CYCLE_END TRIGGER DETECTED] Rising edge on {trigger_bit} (0 -> 1)!"
                    )
                    logger.info("⚡ Acquiring complete shot telemetry without delay...")

                    try:
                        event = collector.collect_cycle(trigger_type="CYCLE_END")
                        duration = time.time() - shot_start

                        shot_no = next((p.value for p in event.points if p.tag == "SHOT_NO"), "N/A")
                        cycle_time = next((p.value for p in event.points if p.tag == "CYCLE_TIME"), "N/A")
                        total_shots = next((p.value for p in event.points if p.tag == "HIGH_SHOT_COUNT"), "N/A")
                        furnace_temp = next((p.value for p in event.points if p.tag == "FURNACE_METAL_TEMP"), "N/A")
                        pouring_val = next((p.value for p in event.points if p.tag == "POURING_TIME"), "N/A")

                        # Cycle End DateTime (Local IST time, strictly matching Ricosys cycletime EndDateTime)
                        cycle_end_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # Format scaled process times for display
                        cycle_sec_str = f"{cycle_time / 10.0:.1f}s" if isinstance(cycle_time, (int, float)) and cycle_time > 100 else f"{cycle_time}s"
                        pouring_str = f"{pouring_val / 10.0:.2f}s" if isinstance(pouring_val, (int, float)) and pouring_val > 10 else f"{pouring_val}s"

                        logger.info(
                            f"✅ [SHOT COMPLETE] Counter: {shot_no} | "
                            f"Cycle End DateTime: {cycle_end_str} | "
                            f"Cycle Time: {cycle_sec_str} | "
                            f"Pouring: {pouring_str} | "
                            f"Furnace: {furnace_temp}°C | "
                            f"Acquired in: {duration:.3f}s"
                        )

                        # Save immediately into SQLite local database
                        storage.insert_event_and_queue(event)
                        logger.info(
                            f"💾 [LOCAL DB SAVED] Counter #{shot_no} (UUID: {event.event_id}) "
                            f"persisted at {cycle_end_str} to SQLite ({db_path}) with status PENDING."
                        )

                        # MQTT publishing (Drains pending queue including any offline backlog)
                        if sync_manager:
                            try:
                                total_published = 0
                                while True:
                                    sync_res = sync_manager.sync_pending_events(batch_size=50)
                                    if sync_res["published"] > 0:
                                        total_published += sync_res["published"]
                                        if sync_res["published"] < 50:
                                            break
                                    else:
                                        break
                                if total_published > 0:
                                    logger.info(f"📡 [MQTT SYNC] {total_published} event(s) published to MQTT broker.")
                                elif sync_res["status"] == "OFFLINE":
                                    logger.debug("MQTT broker offline; shot safely queued in SQLite.")
                            except Exception as me:
                                logger.warning(f"MQTT sync warning: {me}")

                        banner = (
                            f"  QUAD GATEWAY SHOT REPORT | Counter: {shot_no} | "
                            f"Cycle End DateTime: {cycle_end_str} | Cycle Time: {cycle_sec_str}"
                        )
                        table_output = format_points_table(
                            event.points,
                            title_banner=banner,
                            cycle_end_time=cycle_end_str,
                        )
                        print(table_output, flush=True)
                        logger.info("=" * 70)

                    except Exception as e:
                        logger.error(f"Error acquiring or persisting shot cycle: {e}")

                    last_bit_state = 1

                # Falling edge: 1 -> 0 (Machine re-armed for next cycle)
                elif last_bit_state == 1 and current_bit == 0:
                    logger.info(
                        f"🔄 [CYCLE_END RESET] {trigger_bit} returned to 0. "
                        "ARMED: Ready for next shot cycle!"
                    )
                    last_bit_state = 0

                time.sleep(poll_sleep_sec)

    finally:
        driver.disconnect()
        if mqtt_publisher:
            mqtt_publisher.stop()
        if mock_server:
            mock_server.stop()
        logger.info("QUAD Gateway stopped. Connection closed safely.")


def main():
    parser = argparse.ArgumentParser(description="QUAD Gateway — Industrial Edge Gateway (Phase 1)")
    default_cfg = os.path.join(os.path.dirname(__file__), "config", "machine.json")
    parser.add_argument("--config", default=default_cfg, help="Path to machine.json configuration")
    parser.add_argument("--once", action="store_true", help="Execute a single acquisition cycle and exit")
    parser.add_argument("--continuous", action="store_true", help="Force continuous timer polling instead of cycle-triggered")
    parser.add_argument("--trigger-bit", default=None, help="Override trigger bit address (default: M4598)")
    parser.add_argument("--trigger-poll-ms", type=int, default=None, help="Trigger polling interval in milliseconds (default: 50)")
    parser.add_argument("--mock", action="store_true", help="Run against a local in-memory Mock SLMP PLC server")
    parser.add_argument("--host", default=None, help="Override PLC host IP (e.g. for mock testing)")
    parser.add_argument("--port", type=int, default=None, help="Override PLC port (e.g. for mock testing)")
    parser.add_argument("--db", default=None, help="Override SQLite database path")
    parser.add_argument("--no-mqtt", action="store_true", help="Disable MQTT sync manager")
    parser.add_argument("--fake-mqtt", action="store_true", help="Use in-memory Fake MQTT publisher for offline testing")

    parser.add_argument("--validate", action="store_true", help="Validate initial register set against PLC and exit")
    parser.add_argument("--all", action="store_true", help="With --validate, validates all 60 registers")

    args = parser.parse_args()
    if args.validate:
        from quad_gateway.validate_plc import run_plc_validation
        success, _ = run_plc_validation(
            config_path=args.config,
            use_mock=args.mock,
            override_host=args.host,
            override_port=args.port,
            all_registers=args.all
        )
        sys.exit(0 if success else 1)

    run_gateway(
        config_path=args.config,
        single_cycle=args.once,
        override_host=args.host,
        override_port=args.port,
        use_mock=args.mock,
        override_db=args.db,
        no_mqtt=args.no_mqtt,
        fake_mqtt=args.fake_mqtt,
        force_continuous=args.continuous,
        override_trigger_bit=args.trigger_bit,
        override_trigger_poll_ms=args.trigger_poll_ms,
    )


if __name__ == "__main__":
    main()
