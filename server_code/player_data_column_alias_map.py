"""
player_data_column_alias_map.py
================================
Single source of truth mapping legacy calc_player_data.py column names to
metric_dictionary metric_ids, shared by calc_player_data_dictionary.py and
metric_dictionary_diff.py so the two can never drift apart.

Every non-grid "simple" column starts as (None, 'unconfirmed') -- these need
to be filled in against a real metric_dictionary export (see
generate_slim_metric_dictionary_md() in import_csv_file.py) before the new
calculation can produce real values for them. Until then,
calc_player_data_dictionary.py simply skips them (see its skipped_columns
return value) rather than raising.

The 45-cell own-side FBHE grid is pre-filled as (same_name, 'guess') on the
strength of a TODO comment in generate_player_metrics_json_server.py that
references 'fbhe_1_3e_ea' and 'pass_ea' by name -- still needs confirming
against a real export before being trusted.
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
  'fbhe':        (None, 'unconfirmed'),
  'fbhe1':       (None, 'unconfirmed'),
  'fbhe2':       (None, 'unconfirmed'),
  'fbhe3':       (None, 'unconfirmed'),
  'fbhe4':       (None, 'unconfirmed'),
  'fbhe5':       (None, 'unconfirmed'),
  'fbhe_behind': (None, 'unconfirmed'),
  'fbhe_option': (None, 'unconfirmed'),
  'fbhe_tempo':  (None, 'unconfirmed'),
  'fbhe_bang':   (None, 'unconfirmed'),
  'fbhe_oos':    (None, 'unconfirmed'),
  'fbhe_insys':  (None, 'unconfirmed'),
  'fbhe_srv1':   (None, 'unconfirmed'),
  'fbhe_srv3':   (None, 'unconfirmed'),
  'fbhe_srv5':   (None, 'unconfirmed'),
  'tcr':         (None, 'unconfirmed'),
  'tcr_r':       (None, 'unconfirmed'),
  'tcr_s':       (None, 'unconfirmed'),
  'err_den':     (None, 'unconfirmed'),
  'expected':    (None, 'unconfirmed'),
  'srv_fbhe':    (None, 'unconfirmed'),
  'srv1_fbhe':   (None, 'unconfirmed'),
  'srv3_fbhe':   (None, 'unconfirmed'),
  'srv5_fbhe':   (None, 'unconfirmed'),
  'knockout':    (None, 'unconfirmed'),
  'goodpass':    (None, 'unconfirmed'),
  'eso':         (None, 'unconfirmed'),
  't_eff':       (None, 'unconfirmed'),
  't_eff_r':     (None, 'unconfirmed'),
  't_eff_s':     (None, 'unconfirmed'),
  't_create':    (None, 'unconfirmed'),
  't_create_r':  (None, 'unconfirmed'),
  't_create_s':  (None, 'unconfirmed'),
  'cons_fbhe_sd_match': (None, 'unconfirmed'),
  'cons_tcr_sd_match':  (None, 'unconfirmed'),
  'cons_ed_sd_match':   (None, 'unconfirmed'),  # reports_dashboard.py hard dependency
  'cons_ko_sd_match':   (None, 'unconfirmed'),
  'cons_pass_sd_match': (None, 'unconfirmed'),
  'cons_pts_sd_match':  (None, 'unconfirmed'),
  'cons_fbhe_sd_s2s':   (None, 'unconfirmed'),
  'cons_tcr_sd_s2s':    (None, 'unconfirmed'),
  'cons_ed_sd_s2s':     (None, 'unconfirmed'),
  'cons_ko_sd_s2s':     (None, 'unconfirmed'),
  'cons_pass_sd_s2s':   (None, 'unconfirmed'),
  'cons_pts_sd_s2s':    (None, 'unconfirmed'),
  # fbhe_range, point_per are NOT metric_dictionary lookups -- see
  # calc_player_data_dictionary.py's _compute_fbhe_range/_compute_point_per.
}

# Own-side 45-cell FBHE grid, generated rather than hand-listed.
for _fr in (1, 3, 5):
  for _net in (1, 2, 3, 4, 5):
    for _depth in ('c', 'd', 'e'):
      _cell = f"fbhe_{_fr}_{_net}{_depth}"
      COLUMN_ALIAS_MAP[_cell] = (_cell, 'guess')
      COLUMN_ALIAS_MAP[f"{_cell}_ea"] = (f"{_cell}_ea", 'guess')


# ============================================================================
# "_per" ratio columns -- NOT simple aliases. See
# calc_player_data_dictionary._compute_per_ratios() for how these are used.
#
# fbhe_*_per: subset pass attempts (captured as <base>_n once the base metric
#   is mapped) / TOTAL pass attempts for the player.
# srv*_ace_per / srv*_err_per: count of point_outcome TSA/TSE / TOTAL serve
#   attempts for the player, optionally zone-filtered.
#
# Both denominators are computed directly from ppr_df (not via a dictionary
# metric_id), filtered the same way fbhe_obj() does it
# (server_functions.py ~line 400): `<col>.str.strip() == player_name.strip()`.
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
  'srv_ace_per':  {'outcome': 'TSA', 'zone': None, 'denominator': 'total_serve'},
  'srv_err_per':  {'outcome': 'TSE', 'zone': None, 'denominator': 'total_serve'},
  'srv1_ace_per': {'outcome': 'TSA', 'zone': 1,    'denominator': 'total_serve'},
  'srv1_err_per': {'outcome': 'TSE', 'zone': 1,    'denominator': 'total_serve'},
  'srv3_ace_per': {'outcome': 'TSA', 'zone': 3,    'denominator': 'total_serve'},
  'srv3_err_per': {'outcome': 'TSE', 'zone': 3,    'denominator': 'total_serve'},
  'srv5_ace_per': {'outcome': 'TSA', 'zone': 5,    'denominator': 'total_serve'},
  'srv5_err_per': {'outcome': 'TSE', 'zone': 5,    'denominator': 'total_serve'},
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
