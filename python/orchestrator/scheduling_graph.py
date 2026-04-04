"""DAG-based job dependency graph for intelligent scheduling.

Replaces coarse lock-group serialisation with fine-grained dependency tracking.
Jobs declare their upstream requirements via ``depends_on``; the graph resolves
execution tiers (waves) so that independent jobs run concurrently while
dependency chains are respected.

Key concepts:
  - **Dependency edge**: job B ``depends_on`` job A means B cannot start until A
    has completed successfully in the current scheduling cycle.
  - **Execution tier**: a set of jobs whose dependencies are all satisfied.
    All jobs in the same tier may run concurrently.
  - **Concurrency group**: jobs that share a physical resource (e.g. Neo4j
    write lock) and must not overlap, even if they have no data dependency.
    Within a tier, only one job per concurrency group runs at a time.
  - **Ready set**: the subset of due jobs whose upstream dependencies have
    already completed, making them eligible for immediate dispatch.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class JobNode:
    """A single job in the dependency graph."""
    key: str
    depends_on: tuple[str, ...]
    concurrency_group: str
    priority: str
    resource_cost: str
    tier: int = 0  # assigned during topological sort


@dataclass(slots=True)
class SchedulingPlan:
    """Result of analysing the dependency graph for a set of due jobs."""
    tiers: list[list[str]]                         # ordered waves of job keys
    ready_jobs: list[str]                           # jobs eligible right now
    blocked_jobs: dict[str, list[str]]              # job -> unsatisfied deps
    concurrency_groups: dict[str, list[str]]        # group -> job keys in group
    job_tiers: dict[str, int]                       # job_key -> tier number
    explanation: list[str] = field(default_factory=list)  # human-readable log


class CyclicDependencyError(ValueError):
    """Raised when the job graph contains a cycle."""


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def build_graph(
    definitions: dict[str, dict[str, Any]],
    known_external_keys: set[str] | None = None,
) -> dict[str, JobNode]:
    """Build the full dependency graph from worker job definitions.

    Each definition may contain:
      - ``depends_on``: list of job keys that must complete first
      - ``concurrency_group``: resource-level mutual exclusion label
      - ``lock_group``: legacy field, used as fallback concurrency_group

    Args:
        definitions: job definitions to include in this graph.
        known_external_keys: job keys that exist in the full registry but are
            not part of *this* graph (e.g. jobs in another loop).  Dependencies
            on these keys are silently stripped (they run in a different loop
            and are assumed fresh).  Dependencies on keys that are neither in
            ``definitions`` nor in ``known_external_keys`` are logged as
            warnings — those indicate a real configuration error.
    """
    nodes: dict[str, JobNode] = {}
    all_keys = set(definitions.keys())
    external = known_external_keys or set()

    for key, defn in definitions.items():
        raw_deps = defn.get("depends_on") or []
        if isinstance(raw_deps, str):
            raw_deps = [raw_deps]
        # Only keep dependencies that actually exist in this graph.
        deps = tuple(d for d in raw_deps if d in all_keys)
        if len(deps) != len(raw_deps):
            # Distinguish cross-loop deps (expected) from truly unknown jobs.
            truly_missing = set(raw_deps) - all_keys - external
            if truly_missing:
                logger.warning(
                    "scheduling_graph: job %s depends on unknown jobs: %s (ignored)",
                    key, ", ".join(sorted(truly_missing)),
                )
        # Concurrency group: prefer explicit field, fall back to lock_group.
        cg = str(defn.get("concurrency_group") or defn.get("lock_group") or "")
        nodes[key] = JobNode(
            key=key,
            depends_on=deps,
            concurrency_group=cg,
            priority=str(defn.get("priority") or "normal"),
            resource_cost=str(defn.get("resource_cost") or "low"),
        )
    return nodes


# ---------------------------------------------------------------------------
# Topological sort & tier assignment
# ---------------------------------------------------------------------------

def _topological_tiers(nodes: dict[str, JobNode]) -> tuple[list[list[str]], dict[str, int]]:
    """Kahn's algorithm producing execution tiers (parallel waves).

    Returns (tiers, job_tier_map) where each tier is a list of job keys
    that can execute concurrently once all prior tiers have finished.
    """
    in_degree: dict[str, int] = {k: 0 for k in nodes}
    dependents: dict[str, list[str]] = defaultdict(list)

    for key, node in nodes.items():
        for dep in node.depends_on:
            if dep in nodes:
                in_degree[key] += 1
                dependents[dep].append(key)

    # Tier 0: jobs with no dependencies
    queue: deque[str] = deque(k for k, deg in in_degree.items() if deg == 0)
    tiers: list[list[str]] = []
    job_tier: dict[str, int] = {}
    processed = 0

    while queue:
        current_tier: list[str] = []
        next_queue: deque[str] = deque()
        while queue:
            job_key = queue.popleft()
            current_tier.append(job_key)
            job_tier[job_key] = len(tiers)
            processed += 1
            for downstream in dependents[job_key]:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    next_queue.append(downstream)
        # Sort within tier by priority for deterministic ordering.
        priority_order = {"highest": 0, "high": 1, "normal": 2, "low": 3}
        current_tier.sort(key=lambda k: (priority_order.get(nodes[k].priority, 2), k))
        tiers.append(current_tier)
        queue = next_queue

    if processed != len(nodes):
        cycle_members = sorted(k for k in nodes if k not in job_tier)
        raise CyclicDependencyError(
            f"Cyclic dependency detected among: {', '.join(cycle_members)}"
        )

    return tiers, job_tier


def validate_graph(nodes: dict[str, JobNode]) -> list[str]:
    """Return a list of issues found in the graph (empty = valid)."""
    issues: list[str] = []
    for key, node in nodes.items():
        for dep in node.depends_on:
            if dep not in nodes:
                issues.append(f"{key} depends on unknown job {dep}")
            if dep == key:
                issues.append(f"{key} depends on itself")
    # Check for cycles
    try:
        _topological_tiers(nodes)
    except CyclicDependencyError as exc:
        issues.append(str(exc))
    return issues


# ---------------------------------------------------------------------------
# Scheduling plan — which jobs are ready to run right now?
# ---------------------------------------------------------------------------

def compute_scheduling_plan(
    definitions: dict[str, dict[str, Any]],
    due_job_keys: set[str],
    completed_job_keys: set[str],
    running_job_keys: set[str],
) -> SchedulingPlan:
    """Determine which due jobs are ready for dispatch.

    Args:
        definitions: full worker job definitions (used to build graph)
        due_job_keys: jobs that are due for execution this cycle
        completed_job_keys: jobs that completed successfully in recent history
            (within their staleness window)
        running_job_keys: jobs currently being executed
    """
    all_nodes = build_graph(definitions)
    tiers, job_tier = _topological_tiers(all_nodes)

    # Compute ready set: a due job is ready if all its dependencies are
    # either completed or not due (i.e. already fresh enough).
    ready: list[str] = []
    blocked: dict[str, list[str]] = {}
    explanations: list[str] = []

    # Collect concurrency groups
    cg_map: dict[str, list[str]] = defaultdict(list)
    for key in due_job_keys:
        node = all_nodes.get(key)
        if node and node.concurrency_group:
            cg_map[node.concurrency_group].append(key)

    for job_key in due_job_keys:
        node = all_nodes.get(job_key)
        if not node:
            ready.append(job_key)
            explanations.append(f"  {job_key}: ready (no graph entry, treated as independent)")
            continue

        # Check each upstream dependency.
        unsatisfied: list[str] = []
        for dep in node.depends_on:
            if dep in running_job_keys:
                unsatisfied.append(dep)
            elif dep in due_job_keys and dep not in completed_job_keys:
                unsatisfied.append(dep)
            # If dep is not due and not running, it's either fresh or disabled
            # — treat as satisfied.

        if unsatisfied:
            blocked[job_key] = unsatisfied
            explanations.append(
                f"  {job_key} (tier {job_tier.get(job_key, '?')}): BLOCKED by "
                f"{', '.join(unsatisfied)}"
            )
        else:
            ready.append(job_key)
            explanations.append(
                f"  {job_key} (tier {job_tier.get(job_key, '?')}): READY"
            )

    # Sort ready jobs: lower tier first, then by priority.
    priority_order = {"highest": 0, "high": 1, "normal": 2, "low": 3}
    ready.sort(key=lambda k: (
        job_tier.get(k, 999),
        priority_order.get(all_nodes[k].priority if k in all_nodes else "normal", 2),
        k,
    ))

    return SchedulingPlan(
        tiers=tiers,
        ready_jobs=ready,
        blocked_jobs=blocked,
        concurrency_groups=dict(cg_map),
        job_tiers=job_tier,
        explanation=explanations,
    )


# ---------------------------------------------------------------------------
# Concurrency-group filtering
# ---------------------------------------------------------------------------

def filter_by_concurrency_groups(
    ready_jobs: list[str],
    running_job_keys: set[str],
    nodes: dict[str, JobNode],
) -> tuple[list[str], dict[str, str]]:
    """From the ready set, remove jobs whose concurrency group is occupied.

    Returns (dispatchable, deferred_reasons) where deferred_reasons maps
    job_key -> reason string for jobs that were held back.
    """
    # Determine which concurrency groups are currently occupied.
    occupied_groups: set[str] = set()
    for running_key in running_job_keys:
        node = nodes.get(running_key)
        if node and node.concurrency_group:
            occupied_groups.add(node.concurrency_group)

    dispatchable: list[str] = []
    deferred: dict[str, str] = {}
    # Track groups we're about to dispatch into — only one new job per group.
    dispatching_groups: set[str] = set()

    for job_key in ready_jobs:
        node = nodes.get(job_key)
        cg = node.concurrency_group if node else ""
        if not cg:
            dispatchable.append(job_key)
            continue
        if cg in occupied_groups or cg in dispatching_groups:
            holder = "a running job" if cg in occupied_groups else "a newly dispatched job"
            deferred[job_key] = (
                f"concurrency group '{cg}' is held by {holder}"
            )
            continue
        dispatchable.append(job_key)
        dispatching_groups.add(cg)

    return dispatchable, deferred


# ---------------------------------------------------------------------------
# Human-readable graph summary (for CLI / logs)
# ---------------------------------------------------------------------------

def format_graph_summary(definitions: dict[str, dict[str, Any]]) -> str:
    """Return a multi-line human-readable summary of the dependency graph."""
    nodes = build_graph(definitions)
    issues = validate_graph(nodes)
    if issues:
        return "GRAPH VALIDATION ERRORS:\n" + "\n".join(f"  - {i}" for i in issues)

    tiers, job_tier = _topological_tiers(nodes)
    lines: list[str] = [
        f"Job Dependency Graph: {len(nodes)} jobs across {len(tiers)} execution tiers",
        "",
    ]

    # Group by concurrency group for readability
    cg_jobs: dict[str, list[str]] = defaultdict(list)
    for key, node in nodes.items():
        cg_jobs[node.concurrency_group or "(independent)"].append(key)

    for tier_idx, tier_jobs in enumerate(tiers):
        lines.append(f"── Tier {tier_idx} ({len(tier_jobs)} jobs, all runnable in parallel) ──")
        for job_key in tier_jobs:
            node = nodes[job_key]
            deps_str = f" ← depends on: {', '.join(node.depends_on)}" if node.depends_on else ""
            cg_str = f" [concurrency: {node.concurrency_group}]" if node.concurrency_group else ""
            lines.append(f"  {node.priority:>7s}  {job_key}{deps_str}{cg_str}")
        lines.append("")

    return "\n".join(lines)
