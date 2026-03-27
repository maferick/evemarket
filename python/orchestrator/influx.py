from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
import csv
import io
import json
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from .http_client import ipv4_opener


@dataclass(slots=True)
class InfluxConfig:
    enabled: bool
    url: str
    org: str
    bucket: str
    token: str
    timeout_seconds: int

    @classmethod
    def from_runtime(cls, raw: dict[str, Any]) -> "InfluxConfig":
        return cls(
            enabled=bool(raw.get("enabled", False)),
            url=str(raw.get("url", "http://127.0.0.1:8086")).rstrip("/"),
            org=str(raw.get("org", "")).strip(),
            bucket=str(raw.get("bucket", "supplycore_rollups")).strip(),
            token=str(raw.get("token", "")).strip(),
            timeout_seconds=max(3, int(raw.get("timeout_seconds", 15))),
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if self.url == "":
            errors.append("InfluxDB URL is empty.")
        if self.org == "":
            errors.append("InfluxDB org is empty.")
        if self.bucket == "":
            errors.append("InfluxDB bucket is empty.")
        if self.token == "":
            errors.append("InfluxDB token is empty.")
        return errors

    @property
    def write_endpoint(self) -> str:
        return f"{self.url}/api/v2/write?org={urllib.parse.quote(self.org)}&bucket={urllib.parse.quote(self.bucket)}&precision=s"


def _escape_measurement(value: str) -> str:
    return value.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")


def _escape_tag(value: str) -> str:
    return value.replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def _escape_string_field(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _format_field_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value}i"
    if isinstance(value, float):
        return format(value, ".15g")
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dict, list)):
        return f'"{_escape_string_field(json.dumps(value, separators=(",", ":"), ensure_ascii=False))}"'
    return f'"{_escape_string_field(str(value))}"'


def _timestamp_seconds(value: date | datetime) -> int:
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(moment.timestamp())
    if isinstance(value, date):
        return int(datetime.combine(value, time.min, tzinfo=timezone.utc).timestamp())
    raise TypeError(f"Unsupported timestamp value: {value!r}")


def encode_point(measurement: str, tags: dict[str, Any], fields: dict[str, Any], timestamp: date | datetime) -> str:
    field_parts: list[str] = []
    for key, value in fields.items():
        encoded = _format_field_value(value)
        if encoded is None:
            continue
        field_parts.append(f"{_escape_tag(str(key))}={encoded}")

    if not field_parts:
        raise ValueError("InfluxDB points require at least one field.")

    tag_parts = [f"{_escape_tag(str(key))}={_escape_tag(str(value))}" for key, value in sorted(tags.items()) if value not in (None, "")]
    tags_segment = ("," + ",".join(tag_parts)) if tag_parts else ""
    return f"{_escape_measurement(measurement)}{tags_segment} {','.join(field_parts)} {_timestamp_seconds(timestamp)}"


class InfluxWriteError(RuntimeError):
    pass


class InfluxQueryError(RuntimeError):
    pass


class InfluxWriter:
    def __init__(self, config: InfluxConfig):
        self._config = config

    def write_lines(self, lines: list[str]) -> None:
        if lines == []:
            return

        payload = "\n".join(lines).encode("utf-8")
        request = urllib.request.Request(
            self._config.write_endpoint,
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Token {self._config.token}",
                "Content-Type": "text/plain; charset=utf-8",
                "Accept": "application/json",
            },
        )

        try:
            with ipv4_opener.open(request, timeout=self._config.timeout_seconds) as response:
                status_code = int(getattr(response, "status", 204) or 204)
                if status_code not in (204, 200):
                    raise InfluxWriteError(f"InfluxDB write returned unexpected HTTP status {status_code}.")
        except urllib.error.HTTPError as error:
            details = error.read().decode("utf-8", errors="replace").strip()
            raise InfluxWriteError(f"InfluxDB write failed with HTTP {error.code}: {details}") from error
        except urllib.error.URLError as error:
            raise InfluxWriteError(f"InfluxDB write failed: {error.reason}") from error


def _parse_csv_value(raw: str, datatype: str) -> Any:
    value = raw.strip()
    if value == "":
        return None

    normalized = datatype.strip()
    if normalized.startswith("dateTime:"):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    if normalized in {"long", "unsignedLong"}:
        try:
            return int(value)
        except ValueError:
            return value
    if normalized in {"double", "decimal"}:
        try:
            return float(value)
        except ValueError:
            return value
    if normalized == "boolean":
        return value.lower() == "true"
    return value


def parse_flux_csv(payload: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    annotations: dict[str, list[str]] = {}
    header: list[str] | None = None

    for row in csv.reader(io.StringIO(payload)):
        if row == []:
            header = None
            annotations = {}
            continue

        first = row[0] if row else ""
        if first.startswith("#"):
            annotations[first] = row
            continue

        if header is None:
            header = row
            continue

        datatypes = annotations.get("#datatype", [])
        defaults = annotations.get("#default", [])
        record: dict[str, Any] = {}
        for index, column in enumerate(header):
            if column == "":
                continue
            raw_value = row[index] if index < len(row) else ""
            if raw_value == "" and index < len(defaults):
                raw_value = defaults[index]
            datatype = datatypes[index] if index < len(datatypes) else "string"
            record[column] = _parse_csv_value(raw_value, datatype)
        records.append(record)

    return records


class InfluxClient:
    def __init__(self, config: InfluxConfig):
        self._config = config

    @property
    def query_endpoint(self) -> str:
        return f"{self._config.url}/api/v2/query?org={urllib.parse.quote(self._config.org)}"

    @property
    def health_endpoint(self) -> str:
        return f"{self._config.url}/health"

    def health(self) -> dict[str, Any]:
        request = urllib.request.Request(
            self.health_endpoint,
            method="GET",
            headers={"Accept": "application/json"},
        )
        try:
            with ipv4_opener.open(request, timeout=self._config.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            details = error.read().decode("utf-8", errors="replace").strip()
            raise InfluxQueryError(f"InfluxDB health check failed with HTTP {error.code}: {details}") from error
        except urllib.error.URLError as error:
            raise InfluxQueryError(f"InfluxDB health check failed: {error.reason}") from error

    def query_flux(self, flux: str) -> list[dict[str, Any]]:
        payload = json.dumps({"query": flux, "type": "flux"}).encode("utf-8")
        request = urllib.request.Request(
            self.query_endpoint,
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Token {self._config.token}",
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/csv",
            },
        )

        try:
            with ipv4_opener.open(request, timeout=self._config.timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return parse_flux_csv(body)
        except urllib.error.HTTPError as error:
            details = error.read().decode("utf-8", errors="replace").strip()
            raise InfluxQueryError(f"InfluxDB query failed with HTTP {error.code}: {details}") from error
        except urllib.error.URLError as error:
            raise InfluxQueryError(f"InfluxDB query failed: {error.reason}") from error
