"""
Local SQLite Storage Module for QUAD Gateway.
Phase 2: Provides thread-safe, transactional, persistent storage for machine
metadata, tag configurations, acquired telemetry events, and server sync queue.

Strict Separation of Concerns:
- SLMP Driver -> live PLC read-only communication
- Collector -> batch planning, acquisition, decoding
- Models -> TelemetryDataPoint, AcquisitionEvent
- StorageLayer -> SQLite persistence and sync queue
"""
from datetime import datetime, timezone
import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from quad_gateway.models.telemetry import AcquisitionEvent

logger = logging.getLogger("quad_gateway.storage")


def utc_now_str() -> str:
    """Returns current UTC timestamp in ISO 8601 string format."""
    return datetime.now(timezone.utc).isoformat()


class StorageManager:
    """
    Industrial SQLite storage manager for QUAD Gateway edge persistence.
    """

    def __init__(self, db_path: str = "data/quad_gateway.db", auto_init: bool = True):
        self.db_path = db_path
        if self.db_path != ":memory:":
            db_dir = os.path.dirname(os.path.abspath(self.db_path))
            os.makedirs(db_dir, exist_ok=True)

        if auto_init:
            self.initialize_database()

    def get_connection(self) -> sqlite3.Connection:
        """
        Creates and configures a new SQLite connection with foreign keys enabled,
        busy timeout, and WAL journal mode.
        """
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        if self.db_path != ":memory:":
            try:
                conn.execute("PRAGMA journal_mode = WAL;")
            except sqlite3.OperationalError:
                pass
        return conn

    def initialize_database(self) -> None:
        """
        Executes schema.sql DDL to ensure all core tables and indexes exist.
        """
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        conn = self.get_connection()
        try:
            with conn:
                conn.executescript(schema_sql)
            logger.info(f"SQLite database initialized successfully at: {self.db_path}")
        finally:
            conn.close()

    # =========================================================================
    # MACHINES TABLE API
    # =========================================================================

    def insert_machine(self, machine_data: Dict[str, Any]) -> int:
        """
        Inserts machine metadata. Raises sqlite3.IntegrityError if machine_id exists.
        """
        now = utc_now_str()
        sql = """
        INSERT INTO machines (
            machine_id, machine_name, gateway_id, plc_ip, plc_port,
            protocol, read_only, enabled, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
        """
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    sql,
                    (
                        machine_data["machine_id"],
                        machine_data["machine_name"],
                        machine_data["gateway_id"],
                        machine_data["plc_ip"],
                        int(machine_data["plc_port"]),
                        machine_data["protocol"],
                        1 if machine_data.get("read_only", True) else 0,
                        1 if machine_data.get("enabled", True) else 0,
                        machine_data.get("created_at", now),
                        machine_data.get("updated_at", now),
                    ),
                )
                return cur.lastrowid
        finally:
            conn.close()

    def upsert_machine(self, machine_data: Dict[str, Any]) -> int:
        """
        Inserts or updates local machine metadata based on machine_id.
        """
        now = utc_now_str()
        sql = """
        INSERT INTO machines (
            machine_id, machine_name, gateway_id, plc_ip, plc_port,
            protocol, read_only, enabled, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(machine_id) DO UPDATE SET
            machine_name = excluded.machine_name,
            gateway_id = excluded.gateway_id,
            plc_ip = excluded.plc_ip,
            plc_port = excluded.plc_port,
            protocol = excluded.protocol,
            read_only = excluded.read_only,
            enabled = excluded.enabled,
            updated_at = excluded.updated_at;
        """
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    sql,
                    (
                        machine_data["machine_id"],
                        machine_data["machine_name"],
                        machine_data["gateway_id"],
                        machine_data["plc_ip"],
                        int(machine_data["plc_port"]),
                        machine_data["protocol"],
                        1 if machine_data.get("read_only", True) else 0,
                        1 if machine_data.get("enabled", True) else 0,
                        machine_data.get("created_at", now),
                        machine_data.get("updated_at", now),
                    ),
                )
                return cur.lastrowid
        finally:
            conn.close()

    def get_machine(self, machine_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetches machine details by machine_id.
        """
        conn = self.get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM machines WHERE machine_id = ?;", (machine_id,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    # =========================================================================
    # MACHINE_TAGS TABLE API
    # =========================================================================

    def insert_machine_tag(self, tag_data: Dict[str, Any]) -> int:
        """
        Inserts a single register tag definition.
        """
        now = utc_now_str()
        sql = """
        INSERT INTO machine_tags (
            machine_id, tag_name, address, data_type, unit,
            scale_factor, enabled, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
        """
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    sql,
                    (
                        tag_data["machine_id"],
                        tag_data["tag_name"],
                        tag_data["address"],
                        tag_data["data_type"],
                        tag_data.get("unit"),
                        tag_data.get("scale_factor"),
                        1 if tag_data.get("enabled", True) else 0,
                        tag_data.get("created_at", now),
                        tag_data.get("updated_at", now),
                    ),
                )
                return cur.lastrowid
        finally:
            conn.close()

    def upsert_machine_tag(self, tag_data: Dict[str, Any]) -> int:
        """
        Inserts or updates a register tag definition.
        Preserves NULL for unknown scale_factor (e.g. D1127 CYCLE_TIME).
        """
        now = utc_now_str()
        sql = """
        INSERT INTO machine_tags (
            machine_id, tag_name, address, data_type, unit,
            scale_factor, enabled, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(machine_id, tag_name) DO UPDATE SET
            address = excluded.address,
            data_type = excluded.data_type,
            unit = excluded.unit,
            scale_factor = excluded.scale_factor,
            enabled = excluded.enabled,
            updated_at = excluded.updated_at;
        """
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    sql,
                    (
                        tag_data["machine_id"],
                        tag_data["tag_name"],
                        tag_data["address"],
                        tag_data["data_type"],
                        tag_data.get("unit"),
                        tag_data.get("scale_factor"),
                        1 if tag_data.get("enabled", True) else 0,
                        tag_data.get("created_at", now),
                        tag_data.get("updated_at", now),
                    ),
                )
                return cur.lastrowid
        finally:
            conn.close()

    def get_machine_tags(
        self, machine_id: str, only_enabled: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Returns all register tags configured for a machine.
        """
        conn = self.get_connection()
        try:
            if only_enabled:
                rows = conn.execute(
                    "SELECT * FROM machine_tags WHERE machine_id = ? AND enabled = 1 ORDER BY id ASC;",
                    (machine_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM machine_tags WHERE machine_id = ? ORDER BY id ASC;",
                    (machine_id,),
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def sync_machine_config(self, config: Any) -> None:
        """
        Synchronizes MachineConfig into machines and machine_tags tables.
        Guarantees machine identity and tag catalog are up to date locally.
        """
        now = utc_now_str()
        machine_info = {
            "machine_id": config.machine_id,
            "machine_name": config.machine_name,
            "gateway_id": config.gateway_id,
            "plc_ip": config.host,
            "plc_port": config.port,
            "protocol": config.protocol,
            "read_only": config.read_only,
            "enabled": True,
            "created_at": now,
            "updated_at": now,
        }

        conn = self.get_connection()
        try:
            with conn:
                # Upsert machine
                conn.execute(
                    """
                    INSERT INTO machines (
                        machine_id, machine_name, gateway_id, plc_ip, plc_port,
                        protocol, read_only, enabled, created_at, updated_at
                    ) VALUES (
                        :machine_id, :machine_name, :gateway_id, :plc_ip, :plc_port,
                        :protocol, :read_only, :enabled, :created_at, :updated_at
                    )
                    ON CONFLICT(machine_id) DO UPDATE SET
                        machine_name = excluded.machine_name,
                        gateway_id = excluded.gateway_id,
                        plc_ip = excluded.plc_ip,
                        plc_port = excluded.plc_port,
                        protocol = excluded.protocol,
                        read_only = excluded.read_only,
                        enabled = excluded.enabled,
                        updated_at = excluded.updated_at;
                    """,
                    machine_info,
                )

                # Upsert each tag
                for reg in config.registers:
                    conn.execute(
                        """
                        INSERT INTO machine_tags (
                            machine_id, tag_name, address, data_type, unit,
                            scale_factor, enabled, created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, 1, ?, ?
                        )
                        ON CONFLICT(machine_id, tag_name) DO UPDATE SET
                            address = excluded.address,
                            data_type = excluded.data_type,
                            unit = excluded.unit,
                            scale_factor = excluded.scale_factor,
                            enabled = excluded.enabled,
                            updated_at = excluded.updated_at;
                        """,
                        (
                            config.machine_id,
                            reg.tag,
                            reg.address,
                            reg.data_type,
                            reg.unit,
                            reg.scale_factor,
                            now,
                            now,
                        ),
                    )
            logger.info(
                f"Synchronized configuration for Machine '{config.machine_id}' "
                f"({len(config.registers)} tags) into SQLite."
            )
        finally:
            conn.close()

    # =========================================================================
    # ACQUISITION_EVENTS & SYNC_QUEUE TRANSACTION API
    # =========================================================================

    def insert_acquisition_event(
        self,
        event_uuid: str,
        gateway_id: str,
        machine_id: str,
        acquired_at: str,
        quality: str,
        payload: str,
        created_at: Optional[str] = None,
    ) -> int:
        """
        Directly inserts a row into acquisition_events.
        """
        now = created_at or utc_now_str()
        sql = """
        INSERT INTO acquisition_events (
            event_uuid, gateway_id, machine_id, acquired_at, quality, payload, created_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?
        );
        """
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    sql,
                    (event_uuid, gateway_id, machine_id, acquired_at, quality, payload, now),
                )
                return cur.lastrowid
        finally:
            conn.close()

    def insert_event_and_queue(self, event: AcquisitionEvent) -> bool:
        """
        Atomic multi-table transaction:
        1. Inserts normalized snapshot into acquisition_events.
        2. Inserts corresponding sync_queue row with status PENDING.
        3. Commits both together.
        Rolls back completely if either operation fails (e.g. duplicate event_uuid).
        """
        now = utc_now_str()

        # Build normalized telemetry payload map preserving all register metadata
        payload_dict = {}
        for p in event.points:
            payload_dict[p.tag] = {
                "address": p.address,
                "data_type": p.data_type,
                "value": p.value,
                "raw_value": p.raw_value,
                "unit": p.unit,
                "quality": p.quality,
            }
        payload_json = json.dumps(payload_dict, ensure_ascii=False)

        conn = self.get_connection()
        try:
            with conn:
                # 1. Insert acquisition event
                conn.execute(
                    """
                    INSERT INTO acquisition_events (
                        event_uuid, gateway_id, machine_id, acquired_at, quality, payload, created_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?
                    );
                    """,
                    (
                        event.event_id,
                        event.gateway_id,
                        event.machine_id,
                        event.timestamp,
                        event.quality,
                        payload_json,
                        now,
                    ),
                )

                # 2. Insert sync queue entry with PENDING status
                conn.execute(
                    """
                    INSERT INTO sync_queue (
                        event_uuid, status, attempt_count, last_attempt_at,
                        next_attempt_at, synced_at, last_error, created_at, updated_at
                    ) VALUES (
                        ?, 'PENDING', 0, NULL, NULL, NULL, NULL, ?, ?
                    );
                    """,
                    (event.event_id, now, now),
                )

            logger.debug(f"Event {event.event_id} and PENDING sync queue row committed to SQLite.")
            return True

        except Exception as e:
            logger.error(f"Failed to persist event {event.event_id} to SQLite: {e}")
            raise
        finally:
            conn.close()

    def get_event_by_uuid(self, event_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves acquisition event by event_uuid and decodes JSON payload.
        """
        conn = self.get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM acquisition_events WHERE event_uuid = ?;",
                (event_uuid,),
            ).fetchone()
            if not row:
                return None
            res = dict(row)
            res["payload_decoded"] = json.loads(res["payload"])
            return res
        finally:
            conn.close()

    def get_sync_queue_by_uuid(self, event_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves sync_queue record by event_uuid.
        """
        conn = self.get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM sync_queue WHERE event_uuid = ?;",
                (event_uuid,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_pending_sync_events(
        self, limit: int = 100, now_iso: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Queries queue items ready for future Main Server synchronization.
        Selects PENDING or FAILED items whose next_attempt_at has elapsed or is NULL.
        Preserves PENDING/FAILED status in deterministic FIFO order (oldest first).
        """
        effective_now = now_iso or utc_now_str()
        conn = self.get_connection()
        try:
            sql = """
            SELECT 
                sq.id AS queue_id,
                sq.event_uuid,
                sq.status,
                sq.attempt_count,
                sq.last_attempt_at,
                sq.next_attempt_at,
                sq.synced_at,
                sq.last_error,
                ae.gateway_id,
                ae.machine_id,
                ae.acquired_at,
                ae.quality,
                ae.payload
            FROM sync_queue sq
            JOIN acquisition_events ae ON sq.event_uuid = ae.event_uuid
            WHERE sq.status IN ('PENDING', 'FAILED')
              AND (sq.next_attempt_at IS NULL OR sq.next_attempt_at <= ?)
            ORDER BY sq.id ASC
            LIMIT ?;
            """
            rows = conn.execute(sql, (effective_now, limit)).fetchall()
            results = []
            for r in rows:
                item = dict(r)
                item["payload_decoded"] = json.loads(item["payload"])
                results.append(item)
            return results
        finally:
            conn.close()

    def update_sync_attempt(
        self,
        event_uuid: str,
        status: str,
        attempt_count: int,
        last_error: Optional[str] = None,
        next_attempt_at: Optional[str] = None,
        synced_at: Optional[str] = None,
    ) -> bool:
        """
        Updates sync_queue status, attempt count, backoff schedule, and error log.
        Atomic operation in SQLite.
        """
        allowed = {"PENDING", "SENDING", "SYNCED", "FAILED"}
        if status not in allowed:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {allowed}")

        now = utc_now_str()
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    """
                    UPDATE sync_queue
                    SET status = ?,
                        attempt_count = ?,
                        last_attempt_at = ?,
                        next_attempt_at = ?,
                        synced_at = COALESCE(?, synced_at),
                        last_error = ?,
                        updated_at = ?
                    WHERE event_uuid = ?;
                    """,
                    (status, attempt_count, now, next_attempt_at, synced_at, last_error, now, event_uuid),
                )
                return cur.rowcount > 0
        finally:
            conn.close()

    def mark_sync_status(
        self,
        event_uuid: str,
        status: str,
        error: Optional[str] = None,
        synced_at: Optional[str] = None,
    ) -> bool:
        """
        Updates sync_queue status for future synchronization manager.
        Supported states: PENDING, SENDING, SYNCED, FAILED.
        """
        allowed = {"PENDING", "SENDING", "SYNCED", "FAILED"}
        if status not in allowed:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {allowed}")

        now = utc_now_str()
        conn = self.get_connection()
        try:
            with conn:
                cur = conn.execute(
                    """
                    UPDATE sync_queue
                    SET status = ?,
                        attempt_count = attempt_count + 1,
                        last_attempt_at = ?,
                        synced_at = COALESCE(?, synced_at),
                        last_error = ?,
                        updated_at = ?
                    WHERE event_uuid = ?;
                    """,
                    (status, now, synced_at, error, now, event_uuid),
                )
                return cur.rowcount > 0
        finally:
            conn.close()
