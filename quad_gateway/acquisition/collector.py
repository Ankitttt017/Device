"""
Data Acquisition Engine for QUAD Gateway.
Orchestrates reading of configured PLC registers, decodes them according to
data type rules, and produces normalized TelemetryDataPoints and AcquisitionEvents.
"""
import logging
from typing import Any, List, Optional

from quad_gateway.config.config_loader import (
    MachineConfig,
    RegisterConfig,
    parse_device_address,
)
from quad_gateway.drivers.slmp.decoder import (
    decode_bit,
    decode_int16,
    decode_scaled_decimal,
    decode_string,
    decode_uint16,
)
from quad_gateway.drivers.slmp.slmp_driver import SLMPDriver
from quad_gateway.models.telemetry import AcquisitionEvent, TelemetryDataPoint

logger = logging.getLogger("quad_gateway.collector")


class WordBatch:
    """
    Represents a strictly contiguous block of word registers.
    Guarantees no unapproved memory between registers is read.
    """
    def __init__(self, device_type: str, start_address: int):
        self.device_type = device_type
        self.start_address = start_address
        self.registers: List[RegisterConfig] = []
        self.total_words = 0

    def can_append(self, reg: RegisterConfig) -> bool:
        if reg.device_type != self.device_type:
            return False
        return (self.start_address + self.total_words) == reg.device_number

    def append(self, reg: RegisterConfig) -> None:
        self.registers.append(reg)
        self.total_words += reg.words


def plan_word_batches(registers: List[RegisterConfig]) -> List[WordBatch]:
    """
    Groups word registers into strictly contiguous batches.
    Any gap between registers results in a separate batch to avoid reading
    unapproved PLC addresses.
    """
    word_regs = [r for r in registers if r.data_type != "bit"]
    # Sort by device type and address
    sorted_regs = sorted(word_regs, key=lambda r: (r.device_type, r.device_number))

    batches: List[WordBatch] = []
    current_batch: Optional[WordBatch] = None

    for reg in sorted_regs:
        if current_batch is None:
            current_batch = WordBatch(reg.device_type, reg.device_number)
            current_batch.append(reg)
        elif current_batch.can_append(reg) and (current_batch.total_words + reg.words <= 960):
            current_batch.append(reg)
        else:
            batches.append(current_batch)
            current_batch = WordBatch(reg.device_type, reg.device_number)
            current_batch.append(reg)

    if current_batch:
        batches.append(current_batch)

    return batches


class AcquisitionCollector:
    """
    Industrial acquisition collector for reading and normalizing PLC data.
    """

    def __init__(self, config: MachineConfig, driver: SLMPDriver):
        self.config = config
        self.driver = driver
        self.word_batches = plan_word_batches(config.registers)
        self.bit_registers = [r for r in config.registers if r.data_type == "bit"]
        logger.info(
            f"Acquisition planned: {len(self.word_batches)} contiguous word batches, "
            f"{len(self.bit_registers)} bit registers."
        )

    def read_bit_value(self, address_str: str) -> int:
        """
        Reads a single bit register (e.g. 'M4598') directly from the PLC.
        Returns 0 or 1.
        """
        dev_type, dev_num = parse_device_address(address_str)
        bit_vals = self.driver.read_bits(
            device_type=dev_type,
            head_device_number=dev_num,
            points=1
        )
        return bit_vals[0] if bit_vals else 0

    def collect_cycle(self, trigger_type: str = "CYCLE_END") -> AcquisitionEvent:
        """
        Executes one complete read cycle across all configured registers.
        Returns a normalized AcquisitionEvent.
        """
        points: List[TelemetryDataPoint] = []
        cycle_quality = "GOOD"

        # 1. Execute word batches
        for batch in self.word_batches:
            try:
                raw_bytes = self.driver.read_raw_bytes(
                    device_type=batch.device_type,
                    head_device_number=batch.start_address,
                    points=batch.total_words
                )

                # Slice and decode each register in the batch
                offset_words = 0
                for reg in batch.registers:
                    byte_offset = offset_words * 2
                    reg_byte_len = reg.words * 2
                    reg_raw_bytes = raw_bytes[byte_offset:byte_offset + reg_byte_len]
                    offset_words += reg.words

                    point = self._decode_register(reg, reg_raw_bytes)
                    points.append(point)

            except Exception as e:
                logger.error(
                    f"Failed reading batch {batch.device_type}{batch.start_address} "
                    f"({batch.total_words} words): {e}"
                )
                cycle_quality = "BAD"
                # Record error points for transparency
                for reg in batch.registers:
                    points.append(
                        TelemetryDataPoint(
                            tag=reg.tag,
                            address=reg.address,
                            raw_value=None,
                            value=None,
                            unit=reg.unit,
                            data_type=reg.data_type,
                            quality="BAD"
                        )
                    )

        # 2. Execute bit registers (M840, M4598)
        for bit_reg in self.bit_registers:
            try:
                # Read single bit point using SLMP bit subcommand
                bit_vals = self.driver.read_bits(
                    device_type=bit_reg.device_type,
                    head_device_number=bit_reg.device_number,
                    points=1
                )
                val = bit_vals[0] if bit_vals else 0
                points.append(
                    TelemetryDataPoint(
                        tag=bit_reg.tag,
                        address=bit_reg.address,
                        raw_value=val,
                        value=decode_bit(val),
                        unit=bit_reg.unit,
                        data_type="bit",
                        quality="GOOD"
                    )
                )
            except Exception as e:
                logger.error(f"Failed reading bit register {bit_reg.tag} ({bit_reg.address}): {e}")
                cycle_quality = "BAD"
                points.append(
                    TelemetryDataPoint(
                        tag=bit_reg.tag,
                        address=bit_reg.address,
                        raw_value=None,
                        value=None,
                        unit=bit_reg.unit,
                        data_type="bit",
                        quality="BAD"
                    )
                )

        return AcquisitionEvent(
            gateway_id=self.config.gateway_id,
            machine_id=self.config.machine_id,
            points=points,
            quality=cycle_quality,
            trigger=trigger_type
        )

    def _decode_register(self, reg: RegisterConfig, raw_bytes: bytes) -> TelemetryDataPoint:
        """
        Decodes raw bytes according to the register configuration.
        """
        raw_val: Any = None
        scaled_val: Any = None

        if reg.data_type == "string":
            raw_val = raw_bytes.hex()
            scaled_val = decode_string(
                raw_bytes=raw_bytes,
                byte_order=reg.byte_order or "little",
                encoding="ascii",
                strip_null=True
            )
        elif reg.data_type == "int16":
            raw_val = decode_int16(raw_bytes)
            scaled_val = raw_val
        elif reg.data_type == "uint16":
            raw_val = decode_uint16(raw_bytes)
            scaled_val = raw_val
        elif reg.data_type == "decimal_scaled":
            raw_int = decode_int16(raw_bytes)
            raw_val, scaled_val = decode_scaled_decimal(raw_int, reg.scale_factor)
        else:
            raw_val = decode_int16(raw_bytes)
            scaled_val = raw_val

        return TelemetryDataPoint(
            tag=reg.tag,
            address=reg.address,
            raw_value=raw_val,
            value=scaled_val,
            unit=reg.unit,
            data_type=reg.data_type,
            quality="GOOD"
        )
