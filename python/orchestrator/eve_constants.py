"""Shared EVE Online game constants used across orchestrator jobs."""
from __future__ import annotations

# ── Fleet function classification by ship group_id ───────────────────────────
# Maps EVE item group IDs to fleet functions based on typical fleet roles.

FLEET_FUNCTION_BY_GROUP: dict[int, str] = {
    # Mainline DPS
    25: "mainline_dps",       # Frigate
    26: "mainline_dps",       # Cruiser
    27: "mainline_dps",       # Battleship
    380: "mainline_dps",      # Destroyer
    420: "mainline_dps",      # Destroyer (alternate group)
    419: "mainline_dps",      # Battlecruiser
    1201: "mainline_dps",     # Attack Battlecruiser
    358: "mainline_dps",      # Heavy Assault Cruiser
    1305: "mainline_dps",     # Tactical Destroyer
    900: "mainline_dps",      # Marauder
    963: "mainline_dps",      # Strategic Cruiser
    1972: "mainline_dps",     # Flag Cruiser
    # Capital DPS
    485: "capital_dps",       # Dreadnought
    # Logistics
    832: "logistics",         # Logistics Cruiser
    1527: "logistics",        # Logistics Frigate
    # Capital Logistics
    1973: "capital_logistics", # Force Auxiliary
    # Tackle
    831: "tackle",            # Interceptor
    324: "tackle",            # Assault Frigate
    # Heavy Tackle / Bubble Control
    894: "heavy_tackle",      # Heavy Interdictor
    541: "bubble_control",    # Interdictor
    # Command Support / Boosts
    540: "command",           # Command Ship
    1534: "command",          # Command Destroyer
    # Skirmish Control / EWAR
    893: "ewar",              # Electronic Attack Frigate
    906: "ewar",              # Combat Recon
    # Bomber Wing
    834: "bomber",            # Stealth Bomber
    898: "bomber",            # Black Ops
    # Scout / Probe / Hunter & Cyno / Bridge Support
    830: "scout",             # Covert Ops
    833: "scout",             # Force Recon
    # Drone Assist / Utility
    547: "capital_dps",       # Carrier
    # Supercapital Projection
    659: "supercapital",      # Supercarrier
    30: "supercapital",       # Titan
    # Non-combat (excluded from fleet function display)
    29: "non_combat",         # Capsule
    31: "non_combat",         # Shuttle
    28: "non_combat",         # Industrial
    463: "non_combat",        # Mining Barge
    513: "non_combat",        # Freighter
    543: "non_combat",        # Exhumer
    902: "non_combat",        # Jump Freighter
    941: "non_combat",        # Industrial Command Ship
    1022: "non_combat",       # Prototype Exploration Ship
    1202: "non_combat",       # Blockade Runner
    1203: "non_combat",       # Transport Ship
    1283: "non_combat",       # Expedition Frigate
}

# Fleet functions where dying frequently is expected (don't penalize in suspicion)
HIGH_LOSS_ROLES: frozenset[str] = frozenset({
    "tackle", "heavy_tackle", "bubble_control", "bomber", "scout",
})

# Fleet functions where low kill counts are expected (don't penalize in suspicion)
LOW_KILL_ROLES: frozenset[str] = frozenset({
    "logistics", "capital_logistics", "command", "ewar", "scout",
})
