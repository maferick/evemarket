from __future__ import annotations

from typing import Any


def runtime_section(raw_config: dict[str, Any], section: str) -> dict[str, Any]:
    """Return a normalized runtime config section for Python-native jobs.

    All launcher paths (scheduler-dispatched runner, worker pool, and manual CLI)
    should resolve runtime configuration through this helper so jobs do not become
    coupled to a single bootstrap context.
    """

    return dict(raw_config.get(section) or {})


def battle_runtime(raw_config: dict[str, Any]) -> dict[str, Any]:
    return runtime_section(raw_config, "battle_intelligence")


def neo4j_runtime(raw_config: dict[str, Any]) -> dict[str, Any]:
    return runtime_section(raw_config, "neo4j")


def influx_runtime(raw_config: dict[str, Any]) -> dict[str, Any]:
    influx = runtime_section(raw_config, "influx")
    if influx == {}:
        return runtime_section(raw_config, "influxdb")
    return influx
