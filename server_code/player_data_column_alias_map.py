"""
player_data_column_alias_map.py
================================
Since 2026-09-03, calc_player_data_dictionary.py's player_data is
COMPREHENSIVE by default: every metric_dictionary row with
aggregate_level='Yes' is emitted automatically under its own metric_id,
including distribution-shaped metrics (flattened cell-by-cell -- see
calc_player_data_dictionary._flatten_distribution). This file now only
covers the exceptions on top of that:

- COLUMN_ALIAS_MAP: legacy calc_player_data.py column names that differ from
  their metric_dictionary metric_id (e.g. fbhe_bang -> fbhe_harddriven), or
  that need SCALE_TRANSFORMS applied under the legacy name specifically.
  Kept for the handful of downstream consumers that still hardcode old
  player_data column names (reports_league.py, dashboard.py, s_w_report.py,
  reports_dashboard.py, calc_traingle_scoring.py). Most entries here now
  duplicate what the comprehensive pass already emits under the same name
  (metric_id == legacy_col) -- harmless, not worth pruning.
- DROPPED_LEGACY_COLUMNS: legacy columns with zero downstream consumers
  (confirmed by full-repo grep), explicitly excluded so the diff tool never
  flags them as a regression.
- SCALE_TRANSFORMS / PER_RATIO_SPECS: see their own docstrings below.

Confidence tags ('guess' vs 'confirmed') are historical, from when this file
was reconciled against a generate_slim_metric_dictionary_md() export that
only had metric_id/coach_alias/metric_name/coach_view/coach_speak_* (no
function_name/result_path/data_filter) -- kept for now, not actively used.
"""

# ============================================================================
# Columns confirmed (by full-repo grep) to have zero downstream consumers --
# dropped from the new player_data entirely, never mapped, never reported as
# "missing" by the diff tool.
# ============================================================================

DROPPED_LEGACY_COLUMNS = {
  'fbhe_poke', 'fbhe_poke_n', 'fbhe_poke_per',
  'fbhe_shoot', 'fbhe_shoot_n', 'fbhe_shoot_per',
  'opp_fbhe', 'opp_fbhe1', 'opp_fbhe2', 'opp_fbhe3', 'opp_fbhe4', 'opp_fbhe5',
}

for _fr in (1, 3, 5):
  for _net in (1, 2, 3, 4, 5):
    for _depth in ('c', 'd', 'e'):
      _opp_cell = f"opp_fbhe_{_fr}_{_net}{_depth}"
      DROPPED_LEGACY_COLUMNS.add(_opp_cell)
      DROPPED_LEGACY_COLUMNS.add(f"{_opp_cell}_n")
      DROPPED_LEGACY_COLUMNS.add(f"{_opp_cell}_ea")
      # Own-side ellipse-area cells dropped too (decided 2026-09-03): these
      # were kept only because legacy had them, not because they're trusted --
      # a much earlier TODO in generate_player_metrics_json_server.py already
      # flagged *_ea metrics for producing extreme outliers (e.g. pass_ea
      # ~19933, fbhe_1_3e_ea ~10263). Dropped from player_data entirely now
      # that we're no longer constrained to match legacy's column set 1:1.
      DROPPED_LEGACY_COLUMNS.add(f"fbhe_{_fr}_{_net}{_depth}_ea")


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
# the real export, generated rather than hand-listed. _ea (ellipse area)
# siblings deliberately excluded -- see DROPPED_LEGACY_COLUMNS.
for _fr in (1, 3, 5):
  for _net in (1, 2, 3, 4, 5):
    for _depth in ('c', 'd', 'e'):
      _cell = f"fbhe_{_fr}_{_net}{_depth}"
      COLUMN_ALIAS_MAP[_cell] = (_cell, 'guess')

# ============================================================================
# Pressure-situation family -- NEW to player_data (2026-09-03), no legacy
# equivalent at all. These were previously invisible to player_data purely
# because the original alias map only carried columns legacy already had;
# now that the migration itself is trusted, adding metric_dictionary metrics
# with real value even without a legacy precedent. metric_id spelling is
# copied verbatim from the live dictionary, including its "presssure" (3 s's)
# typo on the tcr_s pair -- the player_data COLUMN name is spelled correctly
# regardless, since there's no legacy name to preserve here.
# ============================================================================
COLUMN_ALIAS_MAP['fbhe_pressure_0']     = ('fbhe_pressure_0', 'guess')
COLUMN_ALIAS_MAP['fbhe_pressure_1']     = ('fbhe_pressure_1', 'guess')
COLUMN_ALIAS_MAP['tcr_pressure_0']      = ('tcr_pressure_0', 'guess')
COLUMN_ALIAS_MAP['tcr_pressure_1']      = ('tcr_pressure_1', 'guess')
COLUMN_ALIAS_MAP['err_den_pressure_0']  = ('err_den_pressure_0', 'guess')
COLUMN_ALIAS_MAP['err_den_pressure_1']  = ('err_den_pressure_1', 'guess')
COLUMN_ALIAS_MAP['goodpass_pressure_0'] = ('goodpass_pressure_0', 'guess')
COLUMN_ALIAS_MAP['goodpass_pressure_1'] = ('goodpass_pressure_1', 'guess')
COLUMN_ALIAS_MAP['knockout_pressure_0'] = ('knockout_pressure_0', 'guess')
COLUMN_ALIAS_MAP['knockout_pressure_1'] = ('knockout_pressure_1', 'guess')
COLUMN_ALIAS_MAP['tcr_r_pressure_0']    = ('tcr_r_pressure_0', 'guess')
COLUMN_ALIAS_MAP['tcr_r_pressure_1']    = ('tcr_r_pressure_1', 'guess')
COLUMN_ALIAS_MAP['tcr_s_pressure_0']    = ('tcr_s_presssure_0', 'guess')  # dictionary typo, see note above
COLUMN_ALIAS_MAP['tcr_s_pressure_1']    = ('tcr_s_presssure_1', 'guess')  # dictionary typo, see note above


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
  # err_den_pressure_0/1 are the same calc_error_density_obj-style ratio as
  # err_den, just situationally filtered -- same 0-1-native dictionary value,
  # same reason to put it on err_den's 0-100 basis for consistency.
  'err_den_pressure_0': 100,
  'err_den_pressure_1': 100,
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
#
# fbhe_oos_per/fbhe_insys_per use 'sibling_sum' rather than 'total_pass'
# (decided 2026-09-02): the dictionary's fbhe_oos/fbhe_insys are BOTH scoped
# to "player passed AND attacked" (self-created offense only -- deliberately
# excludes attacks off a partner's pass), unlike legacy's oos/insys which
# scoped to "any pass by this player" regardless of who attacked it. Dividing
# by an independently-computed total_pass would compare a both-filtered
# numerator against a broader denominator -- the same population mismatch
# that caused the fbhe1-5 zone-count bug. Since fbhe_oos_n and fbhe_insys_n
# now partition the exact same "both" population between them, dividing each
# by their sum keeps numerator and denominator on the same scope AND
# preserves oos_per + insys_per = 100%, same complementary-pair property
# legacy's version had (over a different, broader population).
# ============================================================================

PER_RATIO_SPECS = {
  'fbhe_option_per': {'legacy_numerator_col': 'fbhe_option_n', 'denominator': 'total_pass'},
  'fbhe_behind_per': {'legacy_numerator_col': 'fbhe_behind_n', 'denominator': 'total_pass'},
  'fbhe_tempo_per':  {'legacy_numerator_col': 'fbhe_tempo_n',  'denominator': 'total_pass'},
  'fbhe_oos_per':    {'legacy_numerator_col': 'fbhe_oos_n',    'denominator': 'sibling_sum', 'sibling_col': 'fbhe_insys_n'},
  'fbhe_insys_per':  {'legacy_numerator_col': 'fbhe_insys_n',  'denominator': 'sibling_sum', 'sibling_col': 'fbhe_oos_n'},
  'fbhe_bang_per':   {'legacy_numerator_col': 'fbhe_bang_n',   'denominator': 'total_both'},
}


def unresolved_columns():
  """Every alias entry that is still unmapped or not yet 'confirmed'."""
  return {col: meta for col, meta in COLUMN_ALIAS_MAP.items()
          if meta[0] is None or meta[1] != 'confirmed'}
