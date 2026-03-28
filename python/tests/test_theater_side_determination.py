from __future__ import annotations

import unittest

from orchestrator.jobs.theater_analysis import _determine_sides


class TheaterSideDeterminationTests(unittest.TestCase):
    def test_friendly_side_wins_even_when_enemy_side_has_more_pilots(self) -> None:
        participants = [
            {"character_id": 1, "side_key": "LEFT", "alliance_id": 9001, "corporation_id": 0},
            {"character_id": 2, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 3, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 4, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": {9001},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": set(),
            "opponent_corporation_ids": set(),
        }

        side_labels, _char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(side_labels["LEFT"], "side_a")
        self.assertFalse(side_meta["used_fallback"])

    def test_friendly_corporation_match_without_alliance_match_is_still_friendly(self) -> None:
        participants = [
            {"character_id": 10, "side_key": "LEFT", "alliance_id": 0, "corporation_id": 4242},
            {"character_id": 11, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 12, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 13, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": set(),
            "friendly_corporation_ids": {4242},
            "opponent_alliance_ids": set(),
            "opponent_corporation_ids": set(),
        }

        side_labels, _char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(side_labels["LEFT"], "side_a")
        self.assertFalse(side_meta["used_fallback"])

    def test_friendly_and_opponent_entities_map_to_left_and_right(self) -> None:
        participants = [
            {"character_id": 20, "side_key": "LEFT", "alliance_id": 111, "corporation_id": 0},
            {"character_id": 21, "side_key": "LEFT", "alliance_id": 0, "corporation_id": 2222},
            {"character_id": 22, "side_key": "RIGHT", "alliance_id": 333, "corporation_id": 0},
            {"character_id": 23, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 4444},
        ]
        side_configuration = {
            "friendly_alliance_ids": {111},
            "friendly_corporation_ids": {2222},
            "opponent_alliance_ids": {333},
            "opponent_corporation_ids": {4444},
        }

        side_labels, _char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(side_labels["LEFT"], "side_a")
        self.assertEqual(side_labels["RIGHT"], "side_b")
        self.assertFalse(side_meta["used_fallback"])

    def test_fallback_only_when_no_configured_entities_match(self) -> None:
        participants = [
            {"character_id": 30, "side_key": "LEFT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 31, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 32, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
        ]
        side_configuration = {
            "friendly_alliance_ids": {9999},
            "friendly_corporation_ids": {8888},
            "opponent_alliance_ids": {7777},
            "opponent_corporation_ids": {6666},
        }

        side_labels, _char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertTrue(side_meta["used_fallback"])
        self.assertEqual(side_labels["RIGHT"], "side_a")
        self.assertEqual(side_labels["LEFT"], "side_b")


if __name__ == "__main__":
    unittest.main()
