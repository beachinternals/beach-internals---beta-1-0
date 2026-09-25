"""
Tests for the per-set / min-sets sample-size gate on consistency_sd_match
and consistency_sd_set2set (server_code/metric_calc_functions.py).

These functions live inside an Anvil app and import anvil.files, anvil.tables
and logger_utils at module scope. None of those are available (or needed) for
this unit-test run, so this file stubs them out in sys.modules before
importing metric_calc_functions -- see _install_anvil_stubs() below. The
gate's own dictionary lookups are bypassed the same way: tests seed
metric_calc_functions._DICTIONARY_CACHE directly instead of touching
app_tables, so no live Anvil Data Table connection is required.

Run with:
    uv run --with pandas --with numpy python3 -m unittest tests/test_consistency_metrics.py -v
"""

import sys
import types
import unittest
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parent.parent
SERVER_CODE = APP_ROOT / "server_code"


def _install_anvil_stubs():
  anvil_mod = types.ModuleType("anvil")
  anvil_files_mod = types.ModuleType("anvil.files")
  anvil_files_mod.data_files = None
  anvil_tables_mod = types.ModuleType("anvil.tables")

  class _StubTable:
    def search(self):
      # Real code path if a test forgets to seed _DICTIONARY_CACHE --
      # returns no rows, so callers fall back to the module's own
      # DEFAULT_MIN_ATTEMPTS_FOR_CI / DEFAULT_MIN_SETS_FOR_CONSISTENCY.
      return []

  class _StubAppTables:
    metric_dictionary = _StubTable()

  anvil_tables_mod.app_tables = _StubAppTables()

  anvil_mod.files = anvil_files_mod
  anvil_mod.tables = anvil_tables_mod

  sys.modules.setdefault("anvil", anvil_mod)
  sys.modules.setdefault("anvil.files", anvil_files_mod)
  sys.modules.setdefault("anvil.tables", anvil_tables_mod)

  logger_utils_mod = types.ModuleType("logger_utils")
  logger_utils_mod.log_error = lambda *a, **k: None
  sys.modules.setdefault("logger_utils", logger_utils_mod)


_install_anvil_stubs()
sys.path.insert(0, str(SERVER_CODE))

import pandas as pd  # noqa: E402
import metric_calc_functions as mcf  # noqa: E402


PLAYER = "Player X"
PARTNER = "Partner Y"
OPP1 = "Opp A"
OPP2 = "Opp B"


def make_set(video_id, set_num, kills, errors, neutrals):
  """Build point-by-point rows for one (video_id, set) with `kills` FBK,
  `errors` FBE and `neutrals` in-play (non-terminal) attacks by PLAYER, so
  that set's fbhe = (kills - errors) / (kills + errors + neutrals) and its
  own attempt count = kills + errors + neutrals.
  """
  rows = []
  outcomes = ["FBK"] * kills + ["FBE"] * errors + ["FBIP"] * neutrals
  for outcome in outcomes:
    rows.append({
      "video_id": video_id,
      "set": set_num,
      "player_a1": PLAYER,
      "player_a2": PARTNER,
      "player_b1": OPP1,
      "player_b2": OPP2,
      "att_player": PLAYER,
      "point_outcome": outcome,
    })
  return rows


def build_ppr_df(sets):
  """sets: list of (video_id, set_num, kills, errors, neutrals)."""
  rows = []
  for video_id, set_num, kills, errors, neutrals in sets:
    rows.extend(make_set(video_id, set_num, kills, errors, neutrals))
  return pd.DataFrame(rows)


def seed_dictionary(min_attempts_for_ci, min_sets_for_consistency,
                     base_metric_id="fbhe", cons_metric_id="cons_fbhe_sd_s2s"):
  mcf._DICTIONARY_CACHE = {
    base_metric_id: {"min_attempts_for_ci": min_attempts_for_ci, "min_sets_for_consistency": None},
    cons_metric_id: {"min_attempts_for_ci": None, "min_sets_for_consistency": min_sets_for_consistency},
  }


class ConsistencySd2SetGateTests(unittest.TestCase):

  def setUp(self):
    # 8 "plenty" sets, each with 10 attempts -- fbhe values chosen to have
    # real, nonzero spread so the gate's effect on std_dev is visible.
    self.plenty_defs = [
      ("v1", 1, 8, 2, 0),   # fbhe 0.6
      ("v2", 1, 7, 3, 0),   # fbhe 0.4
      ("v3", 1, 7, 2, 1),   # fbhe 0.5
      ("v4", 1, 8, 1, 1),   # fbhe 0.7
      ("v5", 1, 6, 3, 1),   # fbhe 0.3
      ("v6", 1, 8, 2, 0),   # fbhe 0.6
      ("v7", 1, 7, 2, 1),   # fbhe 0.5
      ("v8", 1, 7, 3, 0),   # fbhe 0.4
    ]
    self.plenty_fbhe = [(k - e) / (k + e + n) for _, _, k, e, n in self.plenty_defs]

  def test_1_unchanged_when_every_set_has_plenty_attempts(self):
    """No set is thin, so the gate excludes nothing -- result matches a
    direct, ungated stdev/mean of every set's value (pre-fix behavior)."""
    seed_dictionary(min_attempts_for_ci=5, min_sets_for_consistency=8)
    ppr_df = build_ppr_df(self.plenty_defs)

    result = mcf.consistency_sd_set2set(ppr_df, PLAYER, "fbhe")

    import numpy as np
    expected_std = float(np.std(self.plenty_fbhe, ddof=1))
    expected_mean = float(np.mean(self.plenty_fbhe))

    self.assertEqual(result["num_sets"], 8)
    self.assertAlmostEqual(result["std_dev"], expected_std, places=9)
    self.assertAlmostEqual(result["mean_value"], expected_mean, places=9)

  def test_2_thin_sets_excluded_and_sd_drops_vs_before_fix(self):
    """Add two 1-2 attempt outlier sets. Before the fix they'd be folded
    into the stdev; after the fix they're excluded, and removing extreme
    outliers can only bring the spread down, never up."""
    seed_dictionary(min_attempts_for_ci=5, min_sets_for_consistency=8)

    thin_defs = [
      ("t1", 1, 1, 0, 0),   # 1 attempt, fbhe = 1.0 (extreme)
      ("t2", 1, 0, 2, 0),   # 2 attempts, fbhe = -1.0 (extreme)
    ]
    all_defs = self.plenty_defs + thin_defs
    ppr_df = build_ppr_df(all_defs)

    result = mcf.consistency_sd_set2set(ppr_df, PLAYER, "fbhe")

    import numpy as np
    thin_fbhe = [(k - e) / (k + e + n) for _, _, k, e, n in thin_defs]
    before_fix_std = float(np.std(self.plenty_fbhe + thin_fbhe, ddof=1))

    self.assertEqual(result["num_sets"], 8)  # both thin sets excluded
    self.assertLess(result["std_dev"], before_fix_std)
    # And it should match the plenty-only computation exactly, confirming
    # the thin sets were excluded outright (not zeroed / NaN'd in).
    self.assertAlmostEqual(result["std_dev"], float(np.std(self.plenty_fbhe, ddof=1)), places=9)

  def test_3_fewer_than_min_sets_emits_nothing(self):
    """Only 5 sets qualify against a min-sets floor of 8 -- std_dev and
    mean_value must both be absent (None), not zero or a suppressed number."""
    seed_dictionary(min_attempts_for_ci=5, min_sets_for_consistency=8)
    five_sets = self.plenty_defs[:5]
    ppr_df = build_ppr_df(five_sets)

    result = mcf.consistency_sd_set2set(ppr_df, PLAYER, "fbhe")

    self.assertIsNone(result["std_dev"])
    self.assertIsNone(result["mean_value"])
    self.assertEqual(result["num_sets"], 5)

  def test_4_reported_count_matches_sets_that_survived_the_gate(self):
    """num_sets must reflect qualifying sets, not the player's total set
    count -- verified against both a thin-set-exclusion case and a
    min-sets-suppression case."""
    seed_dictionary(min_attempts_for_ci=5, min_sets_for_consistency=8)

    thin_defs = [("t1", 1, 1, 0, 0), ("t2", 1, 0, 1, 0), ("t3", 1, 1, 0, 0)]
    ppr_df = build_ppr_df(self.plenty_defs + thin_defs)  # 8 plenty + 3 thin = 11 total sets
    result = mcf.consistency_sd_set2set(ppr_df, PLAYER, "fbhe")
    self.assertEqual(result["num_sets"], 8)  # not 11

    seed_dictionary(min_attempts_for_ci=5, min_sets_for_consistency=8)
    ppr_df_small = build_ppr_df(self.plenty_defs[:3])
    result_small = mcf.consistency_sd_set2set(ppr_df_small, PLAYER, "fbhe")
    self.assertEqual(result_small["num_sets"], 3)
    self.assertIsNone(result_small["std_dev"])

  def test_5_avg_shift_direction_depends_on_excluded_set_not_systematic(self):
    """Excluding a thin set moves mean_value in whichever direction that
    set's value pulled the pool -- down when the excluded value was above
    the surviving mean, up when it was below. Neither direction is
    privileged by the gate itself."""
    seed_dictionary(min_attempts_for_ci=5, min_sets_for_consistency=8)

    import numpy as np
    plenty_mean = float(np.mean(self.plenty_fbhe))

    # Thin set well ABOVE the plenty mean.
    high_thin = [("th", 1, 2, 0, 0)]  # 2 attempts, fbhe = 1.0
    ppr_high = build_ppr_df(self.plenty_defs + high_thin)
    result_high = mcf.consistency_sd_set2set(ppr_high, PLAYER, "fbhe")
    before_fix_mean_high = float(np.mean(self.plenty_fbhe + [1.0]))
    delta_high = result_high["mean_value"] - before_fix_mean_high

    # Thin set well BELOW the plenty mean.
    low_thin = [("tl", 1, 0, 2, 0)]  # 2 attempts, fbhe = -1.0
    ppr_low = build_ppr_df(self.plenty_defs + low_thin)
    result_low = mcf.consistency_sd_set2set(ppr_low, PLAYER, "fbhe")
    before_fix_mean_low = float(np.mean(self.plenty_fbhe + [-1.0]))
    delta_low = result_low["mean_value"] - before_fix_mean_low

    self.assertAlmostEqual(result_high["mean_value"], plenty_mean, places=9)
    self.assertAlmostEqual(result_low["mean_value"], plenty_mean, places=9)
    self.assertLess(delta_high, 0)     # excluding an above-average set pulls the gated mean down vs. before-fix
    self.assertGreater(delta_low, 0)   # excluding a below-average set pushes the gated mean up vs. before-fix

  def test_gate_thresholds_are_actually_read_from_the_dictionary(self):
    """Sanity check that the thresholds aren't hardcoded: a non-default
    min_attempts_for_ci changes which sets survive."""
    # With min_attempts_for_ci=3, a 2-attempt set is excluded...
    seed_dictionary(min_attempts_for_ci=3, min_sets_for_consistency=2)
    ppr_df = build_ppr_df([("a", 1, 5, 0, 0), ("b", 1, 1, 1, 0)])  # 5 attempts, 2 attempts
    result = mcf.consistency_sd_set2set(ppr_df, PLAYER, "fbhe")
    self.assertEqual(result["num_sets"], 1)

    # ...but with min_attempts_for_ci=1, that same 2-attempt set now clears it.
    seed_dictionary(min_attempts_for_ci=1, min_sets_for_consistency=2)
    result2 = mcf.consistency_sd_set2set(ppr_df, PLAYER, "fbhe")
    self.assertEqual(result2["num_sets"], 2)


if __name__ == "__main__":
  unittest.main()
