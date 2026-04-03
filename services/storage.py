import hashlib
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Expense:
    amount: float
    category: str
    expense_date: str
    vendor: str
    source: str
    upload_url: Optional[str] = None


class ExpenseRepository:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount REAL,
                category TEXT,
                expense_date TEXT,
                vendor TEXT,
                source TEXT,
                upload_url TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """)

            # 🔥 Prevent duplicates
            conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS unique_expense
            ON expenses(amount, expense_date, vendor)
            """)

    def add_expenses(self, expenses: Iterable[Expense]) -> int:
        inserted = 0
        with self._connect() as conn:
            for e in expenses:
                try:
                    conn.execute("""
                    INSERT INTO expenses (amount, category, expense_date, vendor, source, upload_url)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        e.amount, e.category, e.expense_date,
                        e.vendor, e.source, e.upload_url
                    ))
                    inserted += 1
                except sqlite3.IntegrityError:
                    continue
        return inserted

    def list_expenses(self):
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM expenses ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

    def clear_all(self):
        with self._connect() as conn:
            conn.execute("DELETE FROM expenses")


class LocalStorageAdapter:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, file_bytes: bytes, filename: str) -> str:
        name = f"{uuid.uuid4().hex[:8]}-{filename}"
        path = self.base_dir / name
        path.write_bytes(file_bytes)
        return str(path)

class AzureBlobStorageAdapter:
    def __init__(self, connection_string: str, container_name: str):
        if not connection_string or not container_name:
            raise ValueError("Azure config missing")

        try:
            from azure.storage.blob import BlobServiceClient
        except Exception as e:
            raise RuntimeError("Install azure-storage-blob") from e

        self.client = BlobServiceClient.from_connection_string(connection_string)
        self.container = self.client.get_container_client(container_name)

        try:
            self.container.create_container()
        except:
            pass

    def save(self, file_bytes: bytes, filename: str) -> str:
        import uuid
        blob_name = f"{uuid.uuid4().hex[:8]}-{filename}"
        blob = self.container.get_blob_client(blob_name)
        blob.upload_blob(file_bytes, overwrite=False)
        return blob.url