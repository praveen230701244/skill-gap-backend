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
    expense_date: str  # YYYY-MM-DD
    vendor: str
    source: str
    upload_url: Optional[str] = None


class ExpenseRepository:
    """
    SQLite-backed expense persistence.
    (Designed so storage can be swapped for Azure Blob/SQL later.)
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS expenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL,
                    expense_date TEXT NOT NULL,
                    vendor TEXT NOT NULL,
                    source TEXT NOT NULL,
                    upload_url TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                """
            )
            # Lightweight migration for existing SQLite databases.
            cols = conn.execute("PRAGMA table_info(expenses)").fetchall()
            col_names = {str(c["name"]) for c in cols}
            if "upload_url" not in col_names:
                conn.execute("ALTER TABLE expenses ADD COLUMN upload_url TEXT")

    def add_expenses(self, expenses: Iterable[Expense]) -> int:
        expenses_list = list(expenses)
        if not expenses_list:
            return 0

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO expenses (amount, category, expense_date, vendor, source, upload_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        e.amount,
                        e.category,
                        e.expense_date,
                        e.vendor,
                        e.source,
                        e.upload_url,
                    )
                    for e in expenses_list
                ],
            )
        return len(expenses_list)

    def list_expenses(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        query = "SELECT id, amount, category, expense_date, vendor, source, upload_url, created_at FROM expenses ORDER BY expense_date DESC, id DESC"
        params: Tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "id": r["id"],
                "amount": float(r["amount"]),
                "category": r["category"],
                "date": r["expense_date"],
                "vendor": r["vendor"],
                "source": r["source"],
                "uploadUrl": r["upload_url"],
                "createdAt": r["created_at"],
            }
            for r in rows
        ]

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM expenses").fetchone()
        return int(row["c"])


class FileStorageAdapter:
    """
    Minimal interface for file storage.
    """

    def save(self, file_bytes: bytes, filename: str) -> str:
        raise NotImplementedError


class LocalStorageAdapter(FileStorageAdapter):
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, file_bytes: bytes, filename: str) -> str:
        # Avoid path traversal by hashing the filename.
        stem = Path(filename).name
        digest = hashlib.sha256(stem.encode("utf-8", errors="ignore")).hexdigest()[:12]
        out_name = f"{digest}-{stem}"
        out_path = self.base_dir / out_name
        out_path.write_bytes(file_bytes)
        return str(out_path)


class AzureBlobStorageAdapter(FileStorageAdapter):
    def __init__(self, connection_string: str, container_name: str):
        if not connection_string or not container_name:
            raise ValueError("Azure Blob config is missing.")
        try:
            from azure.storage.blob import BlobServiceClient
        except Exception as e:
            raise RuntimeError("azure-storage-blob package is required for AzureBlobStorageAdapter.") from e

        self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._container_name = container_name.strip().lower()
        self._container_client = self._blob_service_client.get_container_client(self._container_name)
        # Idempotent container creation.
        try:
            self._container_client.create_container()
        except Exception:
            # Container may already exist.
            pass

    def _safe_blob_name(self, filename: str) -> str:
        clean_name = Path(filename).name.replace("\\", "_").replace("/", "_").strip()
        clean_name = clean_name or "upload.bin"
        prefix = uuid.uuid4().hex[:12]
        return f"{prefix}-{clean_name}"

    def save(self, file_bytes: bytes, filename: str) -> str:
        blob_name = self._safe_blob_name(filename)
        blob_client = self._container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file_bytes, overwrite=False)
        return blob_client.url

