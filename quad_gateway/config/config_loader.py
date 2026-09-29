"""
Configuration loader and validator for QUAD Gateway.
Validates machine definition, network connection, read-only safety,
and register definitions.
"""
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class ConfigurationError(ValueError):
    """Raised when configuration validation fails."""
    pass


@dataclass
class RegisterConfig:
    tag: str
    address: str
    device_type: str  # "D", "M", etc.
    device_number: int
    data_type: str  # "int16", "uint16", "decimal_scaled", "string", "bit"
    words: int = 1
    scale_factor: Optional[float] = None
    unit: Optional[str] = None
    description: Optional[str] = None
    byte_order: Optional[str] = None  # "little" or "big" for strings


@dataclass
class MQTTConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: str = "QUAD-01"
    keepalive: int = 60
    topic_prefix: str = "quad"
    qos: int = 1
    retain: bool = False
    batch_size: int = 50
    retry_initial_delay: float = 2.0
    retry_max_delay: float = 60.0
    retry_backoff_factor: float = 2.0


@dataclass
class MachineConfig:
    gateway_id: str
    gateway_name: str
    machine_id: str
    machine_name: str
    protocol: str
    host: str
    port: int
    read_only: bool
    timeout_seconds: float
    monitoring_timer_250ms: int
    poll_interval_seconds: float
    registers: List[RegisterConfig] = field(default_factory=list)
    database_path: str = "data/quad_gateway.db"
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    acquisition_mode: str = "cycle_triggered"
    trigger_bit: str = "M4598"
    trigger_poll_ms: int = 50


def parse_device_address(address_str: str) -> Tuple[str, int]:
    """
    Parses a device address string such as 'D1127' or 'M840'.
    Returns (device_type, device_number).
    """
    match = re.match(r"^([A-Za-z]+)(\d+)$", address_str.strip())
    if not match:
        raise ConfigurationError(f"Invalid device address format: '{address_str}'. Expected format like 'D100' or 'M840'.")
    dev_type = match.group(1).upper()
    dev_num = int(match.group(2))
    return dev_type, dev_num


def load_machine_config(config_path: str) -> MachineConfig:
    """
    Loads and strictly validates the machine configuration JSON file.
    """
    if not os.path.exists(config_path):
        raise ConfigurationError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in configuration file {config_path}: {e}")

    # 1. Validate top-level keys
    for key in ("machine", "connection", "registers"):
        if key not in data:
            raise ConfigurationError(f"Missing required configuration section: '{key}'")

    machine_sec = data["machine"]
    conn_sec = data["connection"]
    gateway_sec = data.get("gateway", {})
    acq_sec = data.get("acquisition", {})

    # 2. Validate connection
    protocol = conn_sec.get("protocol", "").upper()
    if protocol != "SLMP_3E_BINARY":
        raise ConfigurationError(
            f"Unsupported protocol: '{protocol}'. Only 'SLMP_3E_BINARY' is supported in this configuration."
        )

    host = conn_sec.get("host")
    if not host or not isinstance(host, str):
        raise ConfigurationError(f"Invalid connection host: {host}")

    port = conn_sec.get("port")
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise ConfigurationError(f"Invalid connection port: {port}. Must be integer between 1 and 65535.")

    # MANDATORY READ-ONLY SAFETY CHECK
    read_only = conn_sec.get("read_only")
    if read_only is not True:
        raise ConfigurationError(
            "SAFETY VIOLATION: 'connection.read_only' must be strictly true. "
            "Write operations are strictly prohibited on the QUAD Gateway."
        )

    timeout = float(conn_sec.get("timeout_seconds", 5.0))
    timer_250ms = int(conn_sec.get("monitoring_timer_250ms", 16))
    poll_interval = float(acq_sec.get("poll_interval_seconds", 1.0))
    acq_mode = str(acq_sec.get("mode", "cycle_triggered")).lower()
    trigger_bit = str(acq_sec.get("trigger_bit", "M4598")).strip().upper()
    trigger_poll_ms = int(acq_sec.get("trigger_poll_ms", 50))

    # Validate trigger bit format
    try:
        parse_device_address(trigger_bit)
    except ConfigurationError as e:
        raise ConfigurationError(f"Invalid acquisition trigger_bit: {e}")

    # 3. Validate registers
    raw_registers = data.get("registers", [])
    if not raw_registers or not isinstance(raw_registers, list):
        raise ConfigurationError("Configuration 'registers' must be a non-empty list.")

    registers: List[RegisterConfig] = []
    seen_tags = set()

    for idx, reg in enumerate(raw_registers):
        tag = reg.get("tag")
        if not tag or not isinstance(tag, str):
            raise ConfigurationError(f"Register at index {idx} has missing or invalid 'tag'.")
        if tag in seen_tags:
            raise ConfigurationError(f"Duplicate register tag '{tag}' at index {idx}.")
        seen_tags.add(tag)

        addr_str = reg.get("address")
        if not addr_str or not isinstance(addr_str, str):
            raise ConfigurationError(f"Register '{tag}' has missing or invalid 'address'.")
        dev_type, dev_num = parse_device_address(addr_str)

        data_type = reg.get("data_type", "int16").lower()
        valid_data_types = {"int16", "uint16", "decimal_scaled", "string", "bit"}
        if data_type not in valid_data_types:
            raise ConfigurationError(
                f"Register '{tag}' has invalid data_type '{data_type}'. "
                f"Must be one of: {sorted(list(valid_data_types))}"
            )

        words = int(reg.get("words", 1))
        if data_type == "string" and words < 1:
            raise ConfigurationError(f"String register '{tag}' must specify words >= 1.")

        if data_type == "bit" and dev_type not in {"M", "X", "Y", "B", "L", "F"}:
            raise ConfigurationError(
                f"Bit register '{tag}' address '{addr_str}' must be a valid bit device type (M, X, Y, etc.)."
            )

        scale_factor = reg.get("scale_factor")
        if scale_factor is not None:
            try:
                scale_factor = float(scale_factor)
            except (ValueError, TypeError):
                raise ConfigurationError(f"Register '{tag}' has invalid scale_factor: {scale_factor}")

        byte_order = reg.get("byte_order")
        if byte_order and byte_order not in {"little", "big"}:
            raise ConfigurationError(f"Register '{tag}' has invalid byte_order: '{byte_order}'. Must be 'little' or 'big'.")

        registers.append(
            RegisterConfig(
                tag=tag,
                address=addr_str,
                device_type=dev_type,
                device_number=dev_num,
                data_type=data_type,
                words=words,
                scale_factor=scale_factor,
                unit=reg.get("unit"),
                description=reg.get("description"),
                byte_order=byte_order
            )
        )

    storage_sec = data.get("storage", {})
    database_path = storage_sec.get("database_path", "data/quad_gateway.db")

    mqtt_sec = data.get("mqtt", {})
    mqtt_config = MQTTConfig(
        enabled=bool(mqtt_sec.get("enabled", True)),
        host=str(os.environ.get("MQTT_BROKER_HOST") or mqtt_sec.get("host", "172.16.4.104")),
        port=int(os.environ.get("MQTT_BROKER_PORT") or mqtt_sec.get("port", 1883)),
        username=mqtt_sec.get("username"),
        password=mqtt_sec.get("password"),
        client_id=str(mqtt_sec.get("client_id", gateway_sec.get("id", "QUAD-01"))),
        keepalive=int(mqtt_sec.get("keepalive", 60)),
        topic_prefix=str(mqtt_sec.get("topic_prefix", "quad")),
        qos=int(mqtt_sec.get("qos", 1)),
        retain=bool(mqtt_sec.get("retain", False)),
        batch_size=int(mqtt_sec.get("batch_size", 50)),
        retry_initial_delay=float(mqtt_sec.get("retry_initial_delay", 2.0)),
        retry_max_delay=float(mqtt_sec.get("retry_max_delay", 60.0)),
        retry_backoff_factor=float(mqtt_sec.get("retry_backoff_factor", 2.0)),
    )

    return MachineConfig(
        gateway_id=gateway_sec.get("id", "QUAD-01"),
        gateway_name=gateway_sec.get("name", "QUAD Edge Gateway"),
        machine_id=machine_sec.get("id", "UBE-850T-02"),
        machine_name=machine_sec.get("name", "UBE 850 T - 02"),
        protocol=protocol,
        host=host,
        port=port,
        read_only=read_only,
        timeout_seconds=timeout,
        monitoring_timer_250ms=timer_250ms,
        poll_interval_seconds=poll_interval,
        registers=registers,
        database_path=database_path,
        mqtt=mqtt_config,
        acquisition_mode=acq_mode,
        trigger_bit=trigger_bit,
        trigger_poll_ms=trigger_poll_ms,
    )
