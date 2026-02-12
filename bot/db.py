from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id TEXT PRIMARY KEY,
    warehouse_name TEXT NOT NULL,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS regions (
    region_code TEXT PRIMARY KEY,
    region_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS speeds (
    region_code TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    time_hours REAL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (region_code, warehouse_id),
    FOREIGN KEY (region_code) REFERENCES regions(region_code),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

CREATE TABLE IF NOT EXISTS sales (
    region_code TEXT PRIMARY KEY,
    orders_int INTEGER NOT NULL,
    FOREIGN KEY (region_code) REFERENCES regions(region_code)
);

CREATE TABLE IF NOT EXISTS active_warehouses (
    warehouse_id TEXT PRIMARY KEY,
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

CREATE TABLE IF NOT EXISTS uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    filepath TEXT NOT NULL,
    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user TEXT
);
"""


class Database:
    def __init__(self, db_path: str = "data/bot.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def tx(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def add_upload(self, filename: str, filepath: str, user: str | None) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO uploads (filename, filepath, user) VALUES (?, ?, ?)",
                (filename, filepath, user),
            )

    def upsert_speeds(self, records: list[dict]) -> None:
        with self.tx() as conn:
            for rec in records:
                conn.execute(
                    """
                    INSERT INTO regions(region_code, region_name) VALUES (?, ?)
                    ON CONFLICT(region_code) DO UPDATE SET region_name=excluded.region_name
                    """,
                    (rec["region_code"], rec["region_name"]),
                )
                conn.execute(
                    """
                    INSERT INTO warehouses(warehouse_id, warehouse_name, aliases_json) VALUES (?, ?, ?)
                    ON CONFLICT(warehouse_id) DO UPDATE SET warehouse_name=excluded.warehouse_name
                    """,
                    (rec["warehouse_id"], rec["warehouse_name"], json.dumps([rec["warehouse_name"]], ensure_ascii=False)),
                )
                conn.execute(
                    """
                    INSERT INTO speeds(region_code, warehouse_id, time_hours, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(region_code, warehouse_id) DO UPDATE
                    SET time_hours=excluded.time_hours, updated_at=CURRENT_TIMESTAMP
                    """,
                    (rec["region_code"], rec["warehouse_id"], rec["time_hours"]),
                )

    def replace_sales(self, records: list[dict]) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM sales")
            for rec in records:
                conn.execute(
                    "INSERT INTO sales(region_code, orders_int) VALUES (?, ?)",
                    (rec["region_code"], rec["orders"]),
                )

    def list_warehouses(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT w.warehouse_id, w.warehouse_name,
                       CASE WHEN aw.warehouse_id IS NULL THEN 0 ELSE 1 END AS active
                FROM warehouses w
                LEFT JOIN active_warehouses aw ON aw.warehouse_id = w.warehouse_id
                ORDER BY w.warehouse_name
                """
            ).fetchall()

    def set_active(self, ids: list[str]) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM active_warehouses")
            for w_id in ids:
                conn.execute("INSERT OR IGNORE INTO active_warehouses (warehouse_id) VALUES (?)", (w_id,))

    def add_active(self, w_id: str) -> None:
        with self.tx() as conn:
            conn.execute("INSERT OR IGNORE INTO active_warehouses (warehouse_id) VALUES (?)", (w_id,))

    def remove_active(self, w_id: str) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM active_warehouses WHERE warehouse_id=?", (w_id,))

    def active_ids(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT warehouse_id FROM active_warehouses").fetchall()
        return {str(r[0]) for r in rows}

    def speeds_rows(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT s.region_code, r.region_name, s.warehouse_id,
                       w.warehouse_name, s.time_hours
                FROM speeds s
                JOIN regions r ON r.region_code = s.region_code
                JOIN warehouses w ON w.warehouse_id = s.warehouse_id
                """
            ).fetchall()

    def sales_rows(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute("SELECT region_code, orders_int AS orders FROM sales").fetchall()

    def has_data(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM speeds").fetchone()
        return bool(row["c"])
