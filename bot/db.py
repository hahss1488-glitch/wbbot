from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS warehouses (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS regions (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS speeds (
    region_code TEXT NOT NULL,
    warehouse_id INTEGER NOT NULL,
    time_hours REAL,
    PRIMARY KEY (region_code, warehouse_id),
    FOREIGN KEY (region_code) REFERENCES regions(code),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
);

CREATE TABLE IF NOT EXISTS sales (
    region_code TEXT PRIMARY KEY,
    orders REAL NOT NULL,
    FOREIGN KEY (region_code) REFERENCES regions(code)
);

CREATE TABLE IF NOT EXISTS active_warehouses (
    warehouse_id INTEGER PRIMARY KEY,
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
);

CREATE TABLE IF NOT EXISTS uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    filename TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

    def add_upload(self, kind: str, filename: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO uploads (kind, filename) VALUES (?, ?)",
                (kind, filename),
            )

    def replace_speeds(self, records: list[dict]) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM speeds")
            conn.execute("DELETE FROM regions")
            conn.execute("DELETE FROM warehouses")
            for rec in records:
                conn.execute(
                    "INSERT OR IGNORE INTO regions (code, name) VALUES (?, ?)",
                    (rec["region_code"], rec["region_name"]),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO warehouses (id, name) VALUES (?, ?)",
                    (rec["warehouse_id"], rec["warehouse_name"]),
                )
                conn.execute(
                    "INSERT INTO speeds (region_code, warehouse_id, time_hours) VALUES (?, ?, ?)",
                    (rec["region_code"], rec["warehouse_id"], rec["time_hours"]),
                )
            # clear active warehouses no longer valid
            conn.execute(
                "DELETE FROM active_warehouses WHERE warehouse_id NOT IN (SELECT id FROM warehouses)"
            )

    def replace_sales(self, records: list[dict]) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM sales")
            for rec in records:
                conn.execute(
                    "INSERT INTO sales (region_code, orders) VALUES (?, ?)",
                    (rec["region_code"], rec["orders"]),
                )

    def list_warehouses(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT w.id, w.name,
                CASE WHEN aw.warehouse_id IS NULL THEN 0 ELSE 1 END AS active
                FROM warehouses w
                LEFT JOIN active_warehouses aw ON aw.warehouse_id = w.id
                ORDER BY w.id
                """
            ).fetchall()

    def set_active(self, ids: list[int]) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM active_warehouses")
            for w_id in ids:
                conn.execute(
                    "INSERT OR IGNORE INTO active_warehouses (warehouse_id) VALUES (?)", (w_id,)
                )

    def add_active(self, w_id: int) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO active_warehouses (warehouse_id) VALUES (?)", (w_id,)
            )

    def active_ids(self) -> set[int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT warehouse_id FROM active_warehouses").fetchall()
        return {int(r[0]) for r in rows}

    def speeds_rows(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT s.region_code, r.name as region_name, s.warehouse_id,
                       w.name as warehouse_name, s.time_hours
                FROM speeds s
                JOIN regions r ON r.code = s.region_code
                JOIN warehouses w ON w.id = s.warehouse_id
                """
            ).fetchall()

    def sales_rows(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute("SELECT region_code, orders FROM sales").fetchall()

    def has_data(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM speeds").fetchone()
        return bool(row["c"])
