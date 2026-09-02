"""
player_data_column_alias_map.py
================================
Single source of truth mapping legacy calc_player_data.py column names to
metric_dictionary metric_ids, shared by calc_player_data_dictionary.py and
metric_dictionary_diff.py so the two can never drift apart.

Reconciled against a real generate_slim_metric_dictionary_md() export
(metric_id/coach_alias/metric_name/coach_view/coach_speak_* only -- no
function_name/result_path/data_filter, so entries here are 'guess' rather
than 'confirmed' until metric_dictionary_diff.py's numeric comparison backs
them up). Any column still (None, 'unconfirmed') genuinely has no dictionary
equivalent found in that export.
"""

# ============================================================================
# Columns confirmed (by full-repo grep) to have zero downstream consumers --
# dropped from the new player_data entirely, never mapped, never reported as
# "missing" by the diff tool.
# ============================================================================

DROPPED_LEGACY_COLUMNS = {
  'fbhe_poke', 'fbhe_poke_n', 'fbhe_poke_per',
  'fbhe_shoot', 'fbhe_shoot_n', 'fbhe_shoot_per',
  'fbhe_range',
  'opp_fbhe', 'opp_fbhe1', 'opp_fbhe2', 'opp_fbhe3', 'opp_fbhe4', 'opp_fbhe5',
}

for _fr in (1, 3, 5):
  for _net in (1, 2, 3, 4, 5):
    for _depth in ('c', 'd', 'e'):
      _opp_cell = f"opp_fbhe_{_fr}_{_net}{_depth}"
      DROPPED_LEGACY_COLUMNS.add(_opp_cell)
      DROPPED_LEGACY_COLUMNS.add(f"{_opp_cell}_n")
      DROPPED_LEGACY_COLUMNS.add(f"{_opp_cell}_ea")


# ============================================================================
# legacy_column -> (metric_id, confidence)
#   confidence: 'confirmed' | 'guess' | 'unconfirmed'
# ============================================================================

COLUMN_ALIAS_MAP: dict = {
  'fbhe':        ('fbhe', 'guess'),
  'fbhe1':       ('fbhe1', 'guess'),
  'fbhe2':       ('fbhe2', 'guess'),
  'fbhe3':       ('fbhe3', 'guess'),
  'fbhe4':       ('fbhe4', 'guess'),
  'fbhe5':       ('fbhe5', 'guess'),
  'fbhe_behind': ('fbhe_behind', 'guess'),
  'fbhe_option': ('fbhe_option', 'guess'),
  'fbhe_tempo':  ('fbhe_tempo', 'guess'),
  # legacy poke/shoot/bang 3-way split became harddriven/offspeed in the real
  # dictionary. poke/shoot have no home (see DROPPED_LEGACY_COLUMNS); bang
  # maps to harddriven. Denominator semantics ('both' = self-set attacks)
  # unverified against fbhe_harddriven's actual data_filter -- watch this one
  # in the diff tool.
  'fbhe_bang':   ('fbhe_harddriven', 'guess'),
  'fbhe_oos':    ('fbhe_oos', 'guess'),
  'fbhe_insys':  ('fbhe_insys', 'guess'),
  'fbhe_srv1':   ('fbhe_srv1', 'guess'),
  'fbhe_srv3':   ('fbhe_srv3', 'guess'),
  'fbhe_srv5':   ('fbhe_srv5', 'guess'),
  'tcr':         ('tcr', 'guess'),
  'tcr_r':       ('tcr_r', 'guess'),
  'tcr_s':       ('tcr_s', 'guess'),
  'err_den':     ('err_den', 'guess'),
  'expected':    ('expected', 'guess'),
  'srv_fbhe':    ('srv_fbhe', 'guess'),
  'srv1_fbhe':   ('srv1_fbhe', 'guess'),
  'srv3_fbhe':   ('srv3_fbhe', 'guess'),
  'srv5_fbhe':   ('srv5_fbhe', 'guess'),
  'knockout':    ('knockout', 'guess'),
  'goodpass':    ('goodpass', 'guess'),
  'eso':         ('eso', 'guess'),
  't_eff':       ('t_eff', 'guess'),
  't_eff_r':     ('t_eff_r', 'guess'),
  't_eff_s':     ('t_eff_s', 'guess'),
  't_create':    ('t_create', 'guess'),
  't_create_r':  ('t_create_r', 'guess'),
  't_create_s':  ('t_create_s', 'guess'),
  'cons_fbhe_sd_match': ('cons_fbhe_sd_match', 'guess'),
  'cons_tcr_sd_match':  ('cons_tcr_sd_match', 'guess'),
  'cons_ed_sd_match':   ('cons_ed_sd_match', 'guess'),  # reports_dashboard.py hard dependency
  'cons_ko_sd_match':   ('cons_ko_sd_match', 'guess'),
  'cons_pass_sd_match': ('cons_pass_sd_match', 'guess'),
  'cons_pts_sd_match':  ('cons_pts_sd_match', 'guess'),
  'cons_fbhe_sd_s2s':   ('cons_fbhe_sd_s2s', 'guess'),
  'cons_tcr_sd_s2s':    ('cons_tcr_sd_s2s', 'guess'),
  'cons_ed_sd_s2s':     ('cons_ed_sd_s2s', 'guess'),
  'cons_ko_sd_s2s':     ('cons_ko_sd_s2s', 'guess'),
  'cons_pass_sd_s2s':   ('cons_pass_sd_s2s', 'guess'),
  'cons_pts_sd_s2s':    ('cons_pts_sd_s2s', 'guess'),
  # srv*_ace_per / srv*_err_per exist as real metric_ids -- no bespoke ratio
  # math needed (see PER_RATIO_SPECS below for what's actually still bespoke).
  'srv_ace_per':  ('srv_ace_per', 'guess'),
  'srv_err_per':  ('srv_err_per', 'guess'),
  'srv1_ace_per': ('srv1_ace_per', 'guess'),
  'srv1_err_per': ('srv1_err_per', 'guess'),
  'srv3_ace_per': ('srv3_ace_per', 'guess'),
  'srv3_err_per': ('srv3_err_per', 'guess'),
  'srv5_ace_per': ('srv5_ace_per', 'guess'),
  'srv5_err_per': ('srv5_err_per', 'guess'),
  # fbhe_range, point_per are NOT metric_dictionary lookups -- see
  # calc_player_data_dictionary.py's _compute_fbhe_range/_compute_point_per.
}

# Own-side 45-cell FBHE grid -- confirmed present under identical names in
# the real export, generated rather than hand-listed.
for _fr in (1, 3, 5):
  for _net in (1, 2, 3, 4, 5):
    for _depth in ('c', 'd', 'e'):
      _cell = f"fbhe_{_fr}_{_net}{_depth}"
      COLUMN_ALIAS_MAP[_cell] = (_cell, 'guess')
      COLUMN_ALIAS_MAP[f"{_cell}_ea"] = (f"{_cell}_ea", 'guess')


# ============================================================================
# Confirmed scale mismatches (single-player diff, 2026-09-01): legacy
# consistently formatted these as a percent STRING then parsed back to a
# 0-100 float (e.g. calc_ev's `float(ev_vector[0][:-1])` on '{:.2%}'-formatted
# text; calc_error_den does the same). The metric_dictionary versions
# (calc_ev_obj/calc_error_density_obj in server_functions.py) expose the same
# ratio as a plain 0-1 float instead, consistent with every other 0-1-scale
# metric in the dictionary (fbhe, tcr, goodpass, ...). Legacy's 0-100 choice
# is the outlier here, not the dictionary -- so the compatibility scaling
# belongs in this shim, not in metric_dictionary itself.
#
# The same 0-100-vs-0-1 root cause touches cons_tcr_sd_s2s/cons_ed_sd_s2s too
# -- legacy's calc_consistency_s2s_table (server_functions.py) builds its
# per-set 'Tran Conv'/'Error Den' via calc_trans/calc_error_den's
# percent-string parsing (0-100), while the dictionary's consistency_sd_set2set
# (metric_calc_functions.py) computes the same sub-metrics as plain 0-1
# fractions. BOTH group by (video_id, set) -- same population -- so the ×100
# transform applies here.
#
# cons_tcr_sd_match/cons_ed_sd_match ALSO get x100 here, but for a different
# reason than the others above -- confirmed (2026-09-02) this is NOT a scale
# bug for these two. Legacy's calc_consistency_match_table computes exactly 7
# pooled values (All, 1a, 1b, 2a, 2b, 3a, 3b), each aggregating EVERY match
# this player ever played into one ratio per phase label, then takes stdev
# across those 7 numbers. The dictionary's consistency_sd_match groups by
# (video_id, period) instead -- a separate value per individual match's
# phase, stdev taken across all of them pooled. Different population, not
# just different units -- a multiplier does NOT make this match legacy's
# number, and isn't meant to. We're deliberately trusting the new
# (per-match-period) calculation over legacy's here. The x100 is applied
# purely so this stays on the same 0-100 basis as every other percentage-
# based metric in this table (err_den, cons_ed_sd_*, cons_tcr_sd_s2s) --
# consistent basis matters for any downstream code building a percentile/
# z-score from value+mean+stdev together, and for display next to siblings.
#
# cons_tcr_sd_s2s/cons_ed_sd_s2s (and their fbhe/ko/pass/pts siblings) won't
# fully converge to legacy's number either, for a smaller reason: legacy's
# calc_consistency_s2s_table requires >=20 points in a set to count it;
# consistency_sd_set2set has no such floor, so thin/noisy sets inflate its
# stdev somewhat. Accepted as-is -- trusting the new calculation here too.
# ============================================================================

SCALE_TRANSFORMS = {
  'expected': 100,
  'err_den': 100,
  'cons_tcr_sd_match': 100,
  'cons_tcr_sd_s2s': 100,
  'cons_ed_sd_match': 100,
  'cons_ed_sd_s2s': 100,
}


# ============================================================================
# "_per" ratio columns with NO dictionary equivalent -- NOT simple aliases.
# See calc_player_data_dictionary._compute_per_ratios() for how these are
# used. (srv*_ace_per / srv*_err_per turned out to already exist as real
# metric_ids -- see COLUMN_ALIAS_MAP above -- so they're not here.)
#
# fbhe_*_per: subset pass attempts (captured as <base>_n once the base metric
#   is mapped) / TOTAL pass (or 'both', for fbhe_bang_per) attempts for the
#   player, computed directly from ppr_df, filtered the same way fbhe_obj()
#   does it (server_functions.py ~line 400):
#   `<col>.str.strip() == player_name.strip()`.
#
# Known, acceptable divergence from legacy: calc_player_data.py guards its
# pass-attempts denominator with `if attempts != min_att else 1` (line ~364)
# -- almost certainly meant `!= 0`. The new code guards with `if total_pass:`
# (the correct check), so expect the diff tool to flag a mismatch for any
# player whose total pass attempts happens to exactly equal min_att -- that's
# the new code being right, not a regression to chase.
# ============================================================================

PER_RATIO_SPECS = {
  'fbhe_option_per': {'legacy_numerator_col': 'fbhe_option_n', 'denominator': 'total_pass'},
  'fbhe_behind_per': {'legacy_numerator_col': 'fbhe_behind_n', 'denominator': 'total_pass'},
  'fbhe_tempo_per':  {'legacy_numerator_col': 'fbhe_tempo_n',  'denominator': 'total_pass'},
  'fbhe_oos_per':    {'legacy_numerator_col': 'fbhe_oos_n',    'denominator': 'total_pass'},
  'fbhe_insys_per':  {'legacy_numerator_col': 'fbhe_insys_n',  'denominator': 'total_pass'},
  'fbhe_bang_per':   {'legacy_numerator_col': 'fbhe_bang_n',   'denominator': 'total_both'},
}


# ============================================================================
# Explicit allowlist for player_stats_df -- only these base columns get
# <col>_mean / <col>_stdev companions, so new/bonus metric columns never
# silently grow the stats file that s_w_report.py, reports_league.py,
# calc_traingle_scoring.py, and reports_dashboard.py read from.
# ============================================================================

LEGACY_STATS_BASE_COLUMNS = [
  'fbhe', 'fbhe1', 'fbhe2', 'fbhe3', 'fbhe4', 'fbhe5',
  'fbhe_behind', 'fbhe_behind_per', 'fbhe_option', 'fbhe_option_per',
  'fbhe_tempo', 'fbhe_tempo_per', 'fbhe_bang', 'fbhe_bang_per',
  'fbhe_oos', 'fbhe_oos_per', 'fbhe_insys', 'fbhe_insys_per',
  'fbhe_srv1', 'fbhe_srv3', 'fbhe_srv5',
  'tcr', 'tcr_r', 'tcr_s', 'expected', 'err_den',
  'srv_fbhe', 'srv1_fbhe', 'srv3_fbhe', 'srv5_fbhe',
  'cons_fbhe_sd_match', 'cons_tcr_sd_match', 'cons_ed_sd_match',
  'cons_ko_sd_match', 'cons_pass_sd_match', 'cons_pts_sd_match',
  'cons_fbhe_sd_s2s', 'cons_tcr_sd_s2s', 'cons_ed_sd_s2s',
  'cons_ko_sd_s2s', 'cons_pass_sd_s2s', 'cons_pts_sd_s2s',
  'knockout', 'goodpass', 'eso', 't_eff', 't_eff_r', 't_eff_s',
  'point_per', 't_create', 't_create_r', 't_create_s',
]
for _fr in (1, 3, 5):
  for _net in (1, 2, 3, 4, 5):
    for _depth in ('c', 'd', 'e'):
      LEGACY_STATS_BASE_COLUMNS.append(f"fbhe_{_fr}_{_net}{_depth}")
      LEGACY_STATS_BASE_COLUMNS.append(f"fbhe_{_fr}_{_net}{_depth}_ea")


def unresolved_columns():
  """Every alias entry that is still unmapped or not yet 'confirmed'."""
  return {col: meta for col, meta in COLUMN_ALIAS_MAP.items()
          if meta[0] is None or meta[1] != 'confirmed'}
