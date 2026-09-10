-- Schema for Local SQLite Persistent Store (QUAD Gateway)
-- Phase 2: Local SQLite persistence of machine metadata, telemetry events, and sync queue.

PRAGMA foreign_keys = ON;

-- 1. MACHINES TABLE
-- Purpose: Store local machine identity and connection metadata.
CREATE TABLE IF NOT EXISTS machines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id TEXT UNIQUE NOT NULL,
    machine_name TEXT NOT NULL,
    gateway_id TEXT NOT NULL,
    plc_ip TEXT NOT NULL,
    plc_port INTEGER NOT NULL,
    protocol TEXT NOT NULL,
    read_only INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 2. MACHINE_TAGS TABLE
-- Purpose: Store the PLC tag/register mapping used by the QUAD.
CREATE TABLE IF NOT EXISTS machine_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    address TEXT NOT NULL,
    data_type TEXT NOT NULL,
    unit TEXT,
    scale_factor REAL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(machine_id) REFERENCES machines(machine_id) ON DELETE CASCADE,
    UNIQUE(machine_id, tag_name),
    UNIQUE(machine_id, address)
);

-- 3. ACQUISITION_EVENTS TABLE
-- Purpose: Store actual acquisition events produced by the collector.
CREATE TABLE IF NOT EXISTS acquisition_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uuid TEXT UNIQUE NOT NULL,
    gateway_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    quality TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(machine_id) REFERENCES machines(machine_id)
);

-- 4. SYNC_QUEUE TABLE
-- Purpose: Prepare events for future Main Server synchronization.
CREATE TABLE IF NOT EXISTS sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_uuid TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'SENDING', 'SYNCED', 'FAILED')) DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    synced_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(event_uuid) REFERENCES acquisition_events(event_uuid) ON DELETE CASCADE
);

-- PRODUCTION INDEXES
CREATE INDEX IF NOT EXISTS idx_machine_tags_machine_id ON machine_tags(machine_id);
CREATE INDEX IF NOT EXISTS idx_machine_tags_machine_enabled ON machine_tags(machine_id, enabled);
CREATE INDEX IF NOT EXISTS idx_acquisition_events_uuid ON acquisition_events(event_uuid);
CREATE INDEX IF NOT EXISTS idx_acquisition_events_machine_time ON acquisition_events(machine_id, acquired_at);
CREATE INDEX IF NOT EXISTS idx_sync_queue_uuid ON sync_queue(event_uuid);
CREATE INDEX IF NOT EXISTS idx_sync_queue_status_next ON sync_queue(status, next_attempt_at);
