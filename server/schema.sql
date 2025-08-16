-- schema.sql
DROP TABLE IF EXISTS fridge_readings;
DROP TABLE IF EXISTS events;

CREATE TABLE fridge_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    t_c REAL NOT NULL,
    ts TEXT NOT NULL
);

CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    type TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    duration_min REAL
);
