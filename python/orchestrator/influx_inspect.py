from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config
from .influx import InfluxClient, InfluxConfig, InfluxQueryError


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect SupplyCore InfluxDB rollup exports")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--dataset", action="append", default=[], help="Limit inspection to one or more dataset keys or measurement names.")
    parser.add_argument("--limit", type=int, default=5, help="Sample point limit for the sample command.")
    parser.add_argument("--group-by", action="append", default=[], help="Optional tag key(s) to group the sample summary by.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _format_value(value: Any) -> str:
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if value is None or value == "":
        return "—"
    return str(value)


def _format_kv(data: dict[str, Any], keys: list[str]) -> str:
    pairs = [f"{key}={_format_value(data.get(key))}" for key in keys if data.get(key) not in (None, "")]
    return ", ".join(pairs) if pairs else "—"


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _schema_measurements_query(bucket: str) -> str:
    return f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{_quote(bucket)}")'


def _schema_tag_keys_query(bucket: str, measurement: str) -> str:
    return (
        'import "influxdata/influxdb/schema"\n'
        f'schema.tagKeys(bucket: "{_quote(bucket)}", predicate: (r) => r._measurement == "{_quote(measurement)}")'
    )


def _schema_field_keys_query(bucket: str, measurement: str) -> str:
    return (
        'import "influxdata/influxdb/schema"\n'
        f'schema.fieldKeys(bucket: "{_quote(bucket)}", predicate: (r) => r._measurement == "{_quote(measurement)}")'
    )


def _pivot_query(bucket: str, measurement: str, tag_keys: list[str], *, sort_desc: bool = False, limit: int | None = None, count_only: bool = False, group_by: list[str] | None = None) -> str:
    row_key = ", ".join(f'"{_quote(column)}"' for column in (["_time", *tag_keys]))
    lines = [
        f'from(bucket: "{_quote(bucket)}")',
        "  |> range(start: 0)",
        f'  |> filter(fn: (r) => r._measurement == "{_quote(measurement)}")',
        f"  |> pivot(rowKey: [{row_key}], columnKey: [\"_field\"], valueColumn: \"_value\")",
    ]
    if group_by:
        group_columns = ", ".join(f'"{_quote(column)}"' for column in group_by)
        lines.append(f"  |> group(columns: [{group_columns}])")
    else:
        lines.append("  |> group()")
    if count_only:
        # Flux cannot aggregate time-typed columns, so count a stable string column.
        lines.append('  |> count(column: "_measurement")')
    else:
        lines.append(f'  |> sort(columns: ["_time"], desc: {"true" if sort_desc else "false"})')
        if limit is not None:
            lines.append(f"  |> limit(n: {max(1, limit)})")
    return "\n".join(lines)


def _normalize_dataset_filters(filters: list[str], available_measurements: list[str]) -> list[str]:
    if filters == []:
        return available_measurements

    aliases = {
        "market_item_price_1h": "market_item_price",
        "market_item_price_1d": "market_item_price",
        "market_item_stock_1h": "market_item_stock",
        "market_item_stock_1d": "market_item_stock",
        "killmail_item_loss_1h": "killmail_item_loss",
        "killmail_item_loss_1d": "killmail_item_loss",
        "killmail_hull_loss_1d": "killmail_hull_loss",
        "killmail_doctrine_activity_1d": "killmail_doctrine_activity",
        "doctrine_fit_activity_1d": "doctrine_fit_activity",
        "doctrine_group_activity_1d": "doctrine_group_activity",
        "doctrine_fit_stock_pressure_1d": "doctrine_fit_stock_pressure",
    }
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in filters:
        value = str(raw).strip()
        if value == "":
            continue
        measurement = aliases.get(value, value)
        if measurement not in available_measurements:
            available = ", ".join(available_measurements)
            raise ValueError(f'Unknown dataset or measurement "{value}". Available measurements: {available}')
        if measurement in seen:
            continue
        seen.add(measurement)
        normalized.append(measurement)
    return normalized


def _extract_schema_values(rows: list[dict[str, Any]]) -> list[str]:
    values: list[str] = []
    for row in rows:
        value = str(row.get("_value") or "").strip()
        if value == "" or value.startswith("_"):
            continue
        if value not in values:
            values.append(value)
    return values


def _diagnose_ui(summary: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if summary["point_count"] <= 0:
        return ["No points were returned for this measurement, so the issue may be an actual write/query gap."]

    notes.append("Data is present in InfluxDB, so the Data Explorer issue is not an export-write failure.")
    if summary["tag_keys"] == []:
        notes.append("The measurement is effectively tag-light, so explore it by measurement + field selection instead of tag filters.")
    else:
        notes.append(f"Tag keys are present ({', '.join(summary['tag_keys'])}), so the schema is not tag-less.")

    newest = summary.get("newest")
    oldest = summary.get("oldest")
    if isinstance(newest, datetime) and isinstance(oldest, datetime):
        age_days = max(0.0, (datetime.now(timezone.utc) - newest.astimezone(timezone.utc)).total_seconds() / 86400.0)
        if age_days > 7:
            notes.append(f"Newest data is {_format_value(newest)}, so a narrow Explorer time range could hide it.")
        notes.append(f"Select measurement \"{summary['measurement']}\" and set the time range to include {_format_value(oldest)} through {_format_value(newest)}.")
    else:
        notes.append(f"Select measurement \"{summary['measurement']}\" explicitly in Data Explorer before concluding the bucket is empty.")
    return notes


def _measurement_summary(client: InfluxClient, bucket: str, measurement: str) -> dict[str, Any]:
    tag_keys = [key for key in _extract_schema_values(client.query_flux(_schema_tag_keys_query(bucket, measurement))) if key not in {"_measurement"}]
    field_keys = _extract_schema_values(client.query_flux(_schema_field_keys_query(bucket, measurement)))

    count_rows = client.query_flux(_pivot_query(bucket, measurement, tag_keys, count_only=True))
    point_count = sum(int(row.get("_value") or 0) for row in count_rows)

    newest_rows = client.query_flux(_pivot_query(bucket, measurement, tag_keys, sort_desc=True, limit=1))
    oldest_rows = client.query_flux(_pivot_query(bucket, measurement, tag_keys, sort_desc=False, limit=1))
    sample_rows = client.query_flux(_pivot_query(bucket, measurement, tag_keys, sort_desc=True, limit=1))

    newest = newest_rows[0].get("_time") if newest_rows else None
    oldest = oldest_rows[0].get("_time") if oldest_rows else None
    sample_row = sample_rows[0] if sample_rows else {}

    return {
        "measurement": measurement,
        "point_count": point_count,
        "oldest": oldest,
        "newest": newest,
        "tag_keys": tag_keys,
        "field_keys": field_keys,
        "sample_tags": {key: sample_row.get(key) for key in tag_keys if key in sample_row},
        "sample_fields": {key: sample_row.get(key) for key in field_keys if key in sample_row},
    }


def _print_inspect(client: InfluxClient, config: InfluxConfig, measurements: list[str]) -> int:
    health = client.health()
    print(f"InfluxDB health: {_format_value(health.get('status') or health.get('message') or 'unknown')}")
    print(f"Bucket: {config.bucket}")

    summaries: list[dict[str, Any]] = []
    for measurement in measurements:
        summaries.append(_measurement_summary(client, config.bucket, measurement))

    print("Measurements:")
    for summary in summaries:
        print(f"- {summary['measurement']}")
        print(f"  points: {summary['point_count']}")
        print(f"  oldest: {_format_value(summary['oldest'])}")
        print(f"  newest: {_format_value(summary['newest'])}")
        print(f"  sample tags: {_format_kv(summary['sample_tags'], list(summary['sample_tags'].keys()))}")
        print(f"  sample fields: {_format_kv(summary['sample_fields'], list(summary['sample_fields'].keys()))}")
        for note in _diagnose_ui(summary):
            print(f"  diagnosis: {note}")
    return 0


def _print_samples(client: InfluxClient, config: InfluxConfig, measurements: list[str], *, limit: int, group_by: list[str]) -> int:
    health = client.health()
    print(f"InfluxDB health: {_format_value(health.get('status') or health.get('message') or 'unknown')}")
    print(f"Bucket: {config.bucket}")

    for measurement in measurements:
        tag_keys = [key for key in _extract_schema_values(client.query_flux(_schema_tag_keys_query(config.bucket, measurement))) if key not in {"_measurement"}]
        field_keys = _extract_schema_values(client.query_flux(_schema_field_keys_query(config.bucket, measurement)))

        print(f"Measurement: {measurement}")
        rows = client.query_flux(_pivot_query(config.bucket, measurement, tag_keys, sort_desc=True, limit=limit))
        if rows == []:
            print("  No points returned.")
            continue

        print(f"  Latest {limit} points:")
        for row in rows:
            print(f"  - _time={_format_value(row.get('_time'))}")
            print(f"    tags: {_format_kv(row, tag_keys)}")
            print(f"    fields: {_format_kv(row, field_keys)}")

        if group_by:
            group_rows = client.query_flux(_pivot_query(config.bucket, measurement, tag_keys, count_only=True, group_by=group_by))
            print(f"  Group summary by {', '.join(group_by)}:")
            for row in group_rows:
                summary_bits = [f"{column}={_format_value(row.get(column))}" for column in group_by]
                summary_bits.append(f"points={_format_value(row.get('_value'))}")
                print(f"  - {', '.join(summary_bits)}")

    return 0


def _load_client(app_root: Path) -> tuple[InfluxClient, InfluxConfig]:
    runtime = load_php_runtime_config(app_root)
    influx_config = InfluxConfig.from_runtime(dict(runtime.raw.get("influxdb", {})))
    if not influx_config.enabled:
        raise ValueError("InfluxDB integration is disabled in runtime config.")
    validation_errors = influx_config.validate()
    if validation_errors:
        raise ValueError("; ".join(validation_errors))
    return InfluxClient(influx_config), influx_config


def inspect_main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        client, config = _load_client(Path(args.app_root).resolve())
        available_measurements = _extract_schema_values(client.query_flux(_schema_measurements_query(config.bucket)))
        measurements = _normalize_dataset_filters(list(args.dataset), available_measurements)
        return _print_inspect(client, config, measurements)
    except (InfluxQueryError, ValueError) as error:
        print(f"ERROR: {error}")
        return 1


def sample_main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        client, config = _load_client(Path(args.app_root).resolve())
        available_measurements = _extract_schema_values(client.query_flux(_schema_measurements_query(config.bucket)))
        measurements = _normalize_dataset_filters(list(args.dataset), available_measurements)
        if measurements == []:
            raise ValueError("No measurements matched the requested filters.")
        return _print_samples(client, config, measurements, limit=max(1, args.limit), group_by=[str(value).strip() for value in args.group_by if str(value).strip() != ""])
    except (InfluxQueryError, ValueError) as error:
        print(f"ERROR: {error}")
        return 1
