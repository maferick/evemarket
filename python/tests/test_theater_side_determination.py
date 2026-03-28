from __future__ import annotations

import unittest

from orchestrator.jobs.theater_analysis import _determine_sides, _classify_alliance


class TheaterSideDeterminationTests(unittest.TestCase):
    def test_friendly_alliance_classified_as_friendly(self) -> None:
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

        _side_labels, char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(char_sides[1], "friendly")
        self.assertEqual(char_sides[2], "third_party")
        self.assertEqual(side_meta["total_friendly_matches"], 1)

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

        _side_labels, char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(char_sides[10], "friendly")
        self.assertEqual(side_meta["total_friendly_matches"], 1)

    def test_friendly_and_opponent_entities_classified_correctly(self) -> None:
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

        _side_labels, char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(char_sides[20], "friendly")
        self.assertEqual(char_sides[21], "friendly")
        self.assertEqual(char_sides[22], "opponent")
        self.assertEqual(char_sides[23], "opponent")
        self.assertEqual(side_meta["total_friendly_matches"], 2)
        self.assertEqual(side_meta["total_opponent_matches"], 2)

    def test_unmatched_entities_classified_as_third_party(self) -> None:
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

        _side_labels, char_sides, side_meta = _determine_sides(participants, side_configuration)

        self.assertEqual(char_sides[30], "third_party")
        self.assertEqual(char_sides[31], "third_party")
        self.assertEqual(char_sides[32], "third_party")
        self.assertEqual(side_meta["total_third_party"], 3)

    def test_classify_alliance_helper(self) -> None:
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": {200},
            "opponent_alliance_ids": {300},
            "opponent_corporation_ids": {400},
        }
        self.assertEqual(_classify_alliance(100, 0, config), "friendly")
        self.assertEqual(_classify_alliance(0, 200, config), "friendly")
        self.assertEqual(_classify_alliance(300, 0, config), "opponent")
        self.assertEqual(_classify_alliance(0, 400, config), "opponent")
        self.assertEqual(_classify_alliance(999, 999, config), "third_party")
        self.assertEqual(_classify_alliance(0, 0, config), "third_party")


if __name__ == "__main__":
    unittest.main()
