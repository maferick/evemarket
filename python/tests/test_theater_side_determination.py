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
        self.assertEqual(_classify_alliance(100, 0, config), ("friendly", "config"))
        self.assertEqual(_classify_alliance(0, 200, config), ("friendly", "config"))
        self.assertEqual(_classify_alliance(300, 0, config), ("opponent", "config"))
        self.assertEqual(_classify_alliance(0, 400, config), ("opponent", "config"))
        self.assertEqual(_classify_alliance(999, 999, config), ("third_party", "none"))
        self.assertEqual(_classify_alliance(0, 0, config), ("third_party", "none"))

    def test_classify_alliance_graph_inference(self) -> None:
        """Graph-based inference classifies unknown alliances by their relationships."""
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {300},
            "opponent_corporation_ids": set(),
        }
        # Alliance 500 is allied with friendly 100 (high confidence)
        # Alliance 600 is allied with opponent 300 (high confidence)
        graph = {
            500: {
                "allied": [{"target": 100, "confidence": 0.8, "shared_killmails": 50}],
                "hostile": [],
            },
            600: {
                "allied": [{"target": 300, "confidence": 0.7, "shared_killmails": 30}],
                "hostile": [],
            },
            700: {
                "allied": [
                    {"target": 100, "confidence": 0.5, "shared_killmails": 20},
                    {"target": 300, "confidence": 0.5, "shared_killmails": 20},
                ],
                "hostile": [],
            },
        }
        # 500 should be inferred as friendly
        self.assertEqual(_classify_alliance(500, 0, config, graph), ("friendly", "graph"))
        # 600 should be inferred as opponent
        self.assertEqual(_classify_alliance(600, 0, config, graph), ("opponent", "graph"))
        # 700 is ambiguous — allied with both sides equally → third_party
        self.assertEqual(_classify_alliance(700, 0, config, graph), ("third_party", "none"))
        # Config always overrides graph
        self.assertEqual(_classify_alliance(100, 0, config, graph), ("friendly", "config"))


    def test_one_friendly_vs_one_hostile(self) -> None:
        """One friendly alliance vs one hostile alliance — both classified correctly."""
        participants = [
            {"character_id": 40, "side_key": "LEFT", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 41, "side_key": "LEFT", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 42, "side_key": "RIGHT", "alliance_id": 200, "corporation_id": 0},
            {"character_id": 43, "side_key": "RIGHT", "alliance_id": 200, "corporation_id": 0},
        ]
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200},
            "opponent_corporation_ids": set(),
        }
        _, char_sides, meta = _determine_sides(participants, config)
        self.assertEqual(char_sides[40], "friendly")
        self.assertEqual(char_sides[41], "friendly")
        self.assertEqual(char_sides[42], "opponent")
        self.assertEqual(char_sides[43], "opponent")
        self.assertEqual(meta["total_friendly_matches"], 2)
        self.assertEqual(meta["total_opponent_matches"], 2)
        self.assertEqual(meta["total_third_party"], 0)

    def test_one_friendly_vs_multiple_hostile(self) -> None:
        """Friendlies vs multiple hostile alliances."""
        participants = [
            {"character_id": 50, "side_key": "LEFT", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 51, "side_key": "RIGHT", "alliance_id": 200, "corporation_id": 0},
            {"character_id": 52, "side_key": "RIGHT", "alliance_id": 300, "corporation_id": 0},
            {"character_id": 53, "side_key": "RIGHT", "alliance_id": 400, "corporation_id": 0},
        ]
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200, 300},
            "opponent_corporation_ids": set(),
        }
        _, char_sides, meta = _determine_sides(participants, config)
        self.assertEqual(char_sides[50], "friendly")
        self.assertEqual(char_sides[51], "opponent")
        self.assertEqual(char_sides[52], "opponent")
        # 400 is not in opponent list — classified as third_party by backend
        self.assertEqual(char_sides[53], "third_party")
        self.assertEqual(meta["total_friendly_matches"], 1)
        self.assertEqual(meta["total_opponent_matches"], 2)
        self.assertEqual(meta["total_third_party"], 1)

    def test_hostile_not_explicitly_tracked(self) -> None:
        """Alliances not in either friendly or opponent lists become third_party
        at the backend level. The PHP layer then reclassifies them as hostile."""
        participants = [
            {"character_id": 60, "side_key": "LEFT", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 61, "side_key": "RIGHT", "alliance_id": 999, "corporation_id": 0},
        ]
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": set(),
            "opponent_corporation_ids": set(),
        }
        _, char_sides, meta = _determine_sides(participants, config)
        self.assertEqual(char_sides[60], "friendly")
        # Backend classifies as third_party; PHP frontend reclassifies to opponent
        self.assertEqual(char_sides[61], "third_party")
        self.assertEqual(meta["total_third_party"], 1)

    def test_missing_alliance_metadata(self) -> None:
        """Characters with no alliance or corporation identity data."""
        participants = [
            {"character_id": 70, "side_key": "LEFT", "alliance_id": 100, "corporation_id": 0},
            {"character_id": 71, "side_key": "RIGHT", "alliance_id": 0, "corporation_id": 0},
            {"character_id": 72, "side_key": "RIGHT"},
        ]
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": set(),
            "opponent_corporation_ids": set(),
        }
        _, char_sides, meta = _determine_sides(participants, config)
        self.assertEqual(char_sides[70], "friendly")
        self.assertEqual(char_sides[71], "third_party")
        self.assertEqual(char_sides[72], "third_party")

    def test_no_question_mark_in_label_generation(self) -> None:
        """Verify that the classify_alliance function never returns '?'."""
        config = {
            "friendly_alliance_ids": {100},
            "friendly_corporation_ids": set(),
            "opponent_alliance_ids": {200},
            "opponent_corporation_ids": set(),
        }
        # All possible edge cases
        for alliance_id in [0, 100, 200, 500, 999999]:
            for corp_id in [0, 100, 200]:
                result = _classify_alliance(alliance_id, corp_id, config)
                self.assertIn(result, ("friendly", "opponent", "third_party"),
                              f"Unexpected classification '{result}' for alliance={alliance_id}, corp={corp_id}")
                self.assertNotEqual(result, "?",
                                    f"Got '?' for alliance={alliance_id}, corp={corp_id}")


if __name__ == "__main__":
    unittest.main()
