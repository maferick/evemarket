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

# ── Ship size class by group_id ──────────────────────────────────────────────
# Used for peer normalization: only compare pilots who flew similar-sized hulls.
# A battleship naturally out-damages a frigate — comparing them is meaningless.

SHIP_SIZE_BY_GROUP: dict[int, str] = {
    # Small (frigates, destroyers)
    25: "small",        # Frigate
    324: "small",       # Assault Frigate
    380: "small",       # Destroyer
    420: "small",       # Destroyer (alt)
    831: "small",       # Interceptor
    541: "small",       # Interdictor
    830: "small",       # Covert Ops
    834: "small",       # Stealth Bomber
    893: "small",       # Electronic Attack Frigate
    1283: "small",      # Expedition Frigate
    1305: "small",      # Tactical Destroyer
    1527: "small",      # Logistics Frigate
    1534: "small",      # Command Destroyer
    # Medium (cruisers, battlecruisers)
    26: "medium",       # Cruiser
    358: "medium",      # Heavy Assault Cruiser
    419: "medium",      # Battlecruiser
    540: "medium",      # Command Ship
    832: "medium",      # Logistics Cruiser
    833: "medium",      # Force Recon
    894: "medium",      # Heavy Interdictor
    906: "medium",      # Combat Recon
    963: "medium",      # Strategic Cruiser
    1201: "medium",     # Attack Battlecruiser
    1972: "medium",     # Flag Cruiser
    # Large (battleships)
    27: "large",        # Battleship
    898: "large",       # Black Ops
    900: "large",       # Marauder
    # Capital
    485: "capital",     # Dreadnought
    547: "capital",     # Carrier
    659: "capital",     # Supercarrier
    30: "capital",      # Titan
    1973: "capital",    # Force Auxiliary
}

# ── Hull weight scores for composition normalization ────────────────────────
# Reflects combat power multiplier per hull size class.
# A side bringing battleships vs cruisers should be *expected* to outperform.
HULL_WEIGHT: dict[str, float] = {
    "small": 1.0,
    "medium": 2.0,
    "large": 3.5,
    "capital": 6.0,
}

# Fleet function combat power multiplier for composition normalization.
# Reflects how much a role contributes to raw kill/damage output.
ROLE_COMBAT_WEIGHT: dict[str, float] = {
    "mainline_dps": 1.0,
    "capital_dps": 2.5,
    "logistics": 0.1,       # Doesn't contribute kills but multiplies fleet survival
    "capital_logistics": 0.1,
    "tackle": 0.3,
    "heavy_tackle": 0.4,
    "bubble_control": 0.3,
    "command": 0.2,
    "ewar": 0.3,
    "bomber": 0.9,
    "scout": 0.1,
    "supercapital": 4.0,
    "non_combat": 0.0,
}

# Fleet function force-multiplier contribution (logistics, command, ewar
# amplify the whole fleet rather than producing kills directly).
ROLE_MULTIPLIER_WEIGHT: dict[str, float] = {
    "logistics": 1.5,
    "capital_logistics": 2.5,
    "command": 1.3,
    "ewar": 0.8,
}

# ── Economic Warfare: item flag → slot category mapping (ESI standard) ────────

ITEM_FLAG_CATEGORY: dict[int, str] = {
    # EVE ``invFlags`` IDs (verified against the SDE):
    #   LoSlot0..LoSlot7  = 11..18
    #   MedSlot0..MedSlot7 = 19..26
    #   HiSlot0..HiSlot7  = 27..34
    # An earlier version of this map had the 11-18 and 27-34 ranges
    # swapped — clustering still worked because the labels were
    # consistently wrong, but the human-readable slot category and the
    # ``_canonical_name`` heuristic that picks the "dominant high-slot
    # module" were both inverted. Fixing the labels here re-anchors
    # every downstream consumer to reality.
    **{f: "low" for f in range(11, 19)},         # low slots (11-18)
    **{f: "medium" for f in range(19, 27)},      # mid slots (19-26)
    **{f: "high" for f in range(27, 35)},        # high slots (27-34)
    **{f: "rig" for f in range(92, 95)},         # rig slots (92-94)
    87: "drone",                                  # drone bay
    **{f: "subsystem" for f in range(125, 133)},  # T3 subsystems (125-132)
}

# Meta group IDs that indicate fitting-optimized module choices.
# 4=Faction, 6=Deadspace, 14=Tech III — pilots choose these because
# standard T2 variants don't fit or perform worse in tight fits.
FITTING_CONSTRAINED_META_GROUPS: frozenset[int] = frozenset({4, 6, 14})

# Type name keywords that indicate a fitting variant module (CPU/PG optimized).
FITTING_VARIANT_KEYWORDS: tuple[str, ...] = (
    "Compact", "Restrained", "Scoped", "Enduring", "Ample", "Copious",
)
