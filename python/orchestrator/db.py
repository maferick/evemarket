from __future__ import annotations

import json
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from typing import Any

import pymysql
import pymysql.cursors


class SupplyCoreDb:
    def __init__(self, config: dict[str, Any]):
        self._config = config

    def connect(self, *, stream: bool = False):
        cursorclass = pymysql.cursors.SSDictCursor if stream else pymysql.cursors.DictCursor
        return pymysql.connect(
            host=str(self._config.get("host", "127.0.0.1")),
            port=int(self._config.get("port", 3306)),
            user=str(self._config.get("username", "root")),
            password=str(self._config.get("password", "")),
            database=str(self._config.get("database", "supplycore")),
            charset=str(self._config.get("charset", "utf8mb4")),
            unix_socket=(str(self._config.get("socket", "")).strip() or None),
            autocommit=True,
            cursorclass=cursorclass,
        )

    @contextmanager
    def cursor(self, *, stream: bool = False):
        connection = self.connect(stream=stream)
        try:
            with connection.cursor() as cursor:
                yield connection, cursor
        finally:
            connection.close()

    def fetch_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        with self.cursor() as (_, cursor):
            cursor.execute(sql, params or ())
            row = cursor.fetchone()
            return dict(row) if row else None

    def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self.cursor() as (_, cursor):
            cursor.execute(sql, params or ())
            return [dict(row) for row in cursor.fetchall()]

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.cursor() as (_, cursor):
            return int(cursor.execute(sql, params or ()))

    def insert(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.cursor() as (connection, cursor):
            cursor.execute(sql, params or ())
            connection.commit()
            return int(cursor.lastrowid or 0)

    def iterate_batches(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        batch_size: int = 1000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        with self.cursor(stream=True) as (_, cursor):
            cursor.execute(sql, params or ())
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(row) for row in rows]


def json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
