"""Cached gate-distance service using Neo4j shortest-path queries."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..neo4j import Neo4jClient


class GateDistanceService:
    """Query Neo4j for shortest gate-jump distance between EVE systems.

    Results are cached per session so the O(n^2) clustering comparisons
    only trigger one Neo4j query per unique system pair.  When ``client``
    is ``None`` (Neo4j disabled), every lookup returns ``None`` and the
    caller should fall back to constellation-only logic.
    """

    def __init__(self, client: Neo4jClient | None, max_distance: int = 5) -> None:
        self._client = client
        self._max_distance = max_distance
        self._cache: dict[tuple[int, int], int | None] = {}

    def distance(self, system_a: int, system_b: int) -> int | None:
        """Return gate jump count between two systems.

        Returns ``None`` if the systems are more than *max_distance* apart
        or unreachable (e.g. wormhole systems).
        """
        if system_a == system_b:
            return 0
        key = (min(system_a, system_b), max(system_a, system_b))
        if key in self._cache:
            return self._cache[key]
        if self._client is None:
            self._cache[key] = None
            return None

        result = self._client.query(
            f"""
            MATCH (a:System {{system_id: $from_sys}}), (b:System {{system_id: $to_sys}})
            MATCH path = shortestPath((a)-[:CONNECTS_TO*..{self._max_distance}]->(b))
            RETURN length(path) AS distance
            """,
            {"from_sys": system_a, "to_sys": system_b},
        )
        dist = int(result[0]["distance"]) if result else None
        self._cache[key] = dist
        return dist
