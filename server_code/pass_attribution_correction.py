import math

from server_functions import build_point_video_link

# This is a server module. It runs on the Anvil server.
#
# BTD/Balltime's video tracker sometimes gets the *action type* right
# (a pass, set, or attack really happened) but attributes the touch to the
# wrong one of the two teammates -- most visibly when it collapses two or
# three touches in the same rally onto a single player name, which the
# double-contact rule says is impossible.
#
# We never know for certain who actually touched the ball. What we can do is
# lean on the ball's tracked position: each player tends to occupy a
# characteristic court position when passing, and that position is usually
# distinguishable from their teammate's. This module builds per-player,
# per-match position profiles from the match's *clean* (non-conflicting)
# points, uses them to classify the disputed pass touch, and -- only when
# confident -- derives the rest of the touches for that point from the
# double-contact rule (a player can't touch the ball on two consecutive team
# contacts).
#
# This module only ever modifies the derived ppr_df. It never touches the
# BTD source data.

# Minimum number of clean (unflagged) pass touches required for a player's
# position profile to be trusted for classification. Below this, a
# mean/std built from too few points is noise, not signal.
MIN_CLEAN_SAMPLES = 5

# A candidate must be at least this many times closer (z-distance) than the
# other candidate before a correction is auto-applied. Calibrated against a
# handful of real matches reviewed by hand: confidently-correct points
# consistently separated by 2x-3.7x, while a genuinely ambiguous point came
# out at ~1.0x (a dead heat). 1.5x is a conservative line between those --
# expect to retune this once it's run across more real files.
MIN_CONFIDENCE_RATIO = 1.5

# Reject a "clean" point's coordinates as a profile-building input if they
# look like a tracking glitch rather than a real court position (e.g. a
# pass source of y=32.6 or y=-24 have both been seen in real files).
MAX_PLAUSIBLE_COORD = 20.0

_ACTION_ID_FIELDS = ['serve_action_id', 'pass_action_id', 'set_action_id', 'att_action_id', 'dig_action_id']


def _has_name(value):
  return bool(value) and bool(str(value).strip())


def _is_plausible_coord(x, y):
  try:
    return abs(x) <= MAX_PLAUSIBLE_COORD and abs(y) <= MAX_PLAUSIBLE_COORD
  except TypeError:
    return False


def _touch_players(row):
  """Return the touch players in play order (pass, set, att -- whichever
  occurred), and whether any two *adjacent* touches share a name -- the
  double-contact violation this whole module exists to fix.

  Pass and attack being the same player is NOT a violation when there's a
  set in between -- that's the normal, common P-S-P alternation (passer
  digs it, teammate sets, passer comes back and hits). Only touches on
  either side of a given contact can't belong to the same player, so this
  checks adjacency in the touch sequence, not "any name repeated"."""
  touches = []
  if row['pass_yn'] == 'Y' and _has_name(row['pass_player']):
    touches.append(row['pass_player'])
  if row['set_yn'] == 'Y' and _has_name(row['set_player']):
    touches.append(row['set_player'])
  if row['att_yn'] == 'Y' and _has_name(row['att_player']):
    touches.append(row['att_player'])
  has_conflict = any(touches[i] == touches[i + 1] for i in range(len(touches) - 1))
  return touches, has_conflict


def _teammate(row, player):
  if player == row['player_a1']:
    return row['player_a2']
  if player == row['player_a2']:
    return row['player_a1']
  if player == row['player_b1']:
    return row['player_b2']
  if player == row['player_b2']:
    return row['player_b1']
  return None


def _receiving_team_players(row):
  """The pass touch must belong to the team that did NOT serve -- return
  that team's two players as the only valid candidates."""
  serve_player = row['serve_player']
  if serve_player == row['player_a1'] or serve_player == row['player_a2']:
    return row['player_b1'], row['player_b2']
  if serve_player == row['player_b1'] or serve_player == row['player_b2']:
    return row['player_a1'], row['player_a2']
  return None, None


def build_pass_profiles(ppr_df):
  """
  Build per-player pass-position profiles from this match's clean points.

  Profiles are scoped to this match only -- a player's position tendencies
  shift with partner and side, so aggregating across matches (different
  partners, different sides) would corrupt the signal.

  Returns: {player_name: {'mean_x', 'mean_y', 'std_x', 'std_y', 'n'}}
  """
  samples = {}
  for _, row in ppr_df.iterrows():
    if row['pass_yn'] != 'Y' or not _has_name(row['pass_player']):
      continue
    _, has_conflict = _touch_players(row)
    if has_conflict:
      continue
    x, y = row['pass_src_x'], row['pass_src_y']
    if not _is_plausible_coord(x, y):
      continue
    samples.setdefault(row['pass_player'], []).append((x, y))

  profiles = {}
  for player, pts in samples.items():
    n = len(pts)
    mean_x = sum(p[0] for p in pts) / n
    mean_y = sum(p[1] for p in pts) / n
    if n > 1:
      var_x = sum((p[0] - mean_x) ** 2 for p in pts) / (n - 1)
      var_y = sum((p[1] - mean_y) ** 2 for p in pts) / (n - 1)
    else:
      var_x = var_y = 0.0
    # Floor the std so a tight or tiny-N cluster can't produce a
    # near-zero denominator and a falsely "infinite confidence" distance.
    std_x = max(var_x ** 0.5, 0.5)
    std_y = max(var_y ** 0.5, 0.5)
    profiles[player] = {'mean_x': mean_x, 'mean_y': mean_y, 'std_x': std_x, 'std_y': std_y, 'n': n}
  return profiles


def _z_distance(x, y, profile):
  dx = (x - profile['mean_x']) / profile['std_x']
  dy = (y - profile['mean_y']) / profile['std_y']
  return math.sqrt(dx * dx + dy * dy)


def classify_pass_player(row, profiles):
  """
  Classify who actually passed this point. Candidates are constrained to the
  receiving team's two players (the passer can't be on the serving team).

  Returns (player_or_None, info) -- info always carries enough detail to log
  even when no confident call is made, since an unresolved point still needs
  to be reported.
  """
  cand_a, cand_b = _receiving_team_players(row)
  x, y = row['pass_src_x'], row['pass_src_y']
  info = {'candidates': [cand_a, cand_b], 'pass_src': [x, y]}

  if not cand_a or not cand_b:
    info['reason'] = 'could_not_determine_receiving_team'
    return None, info

  if not _is_plausible_coord(x, y):
    info['reason'] = 'missing_or_implausible_pass_coords'
    return None, info

  prof_a, prof_b = profiles.get(cand_a), profiles.get(cand_b)
  if not prof_a or not prof_b or prof_a['n'] < MIN_CLEAN_SAMPLES or prof_b['n'] < MIN_CLEAN_SAMPLES:
    info['reason'] = 'insufficient_clean_samples'
    info['sample_sizes'] = {cand_a: prof_a['n'] if prof_a else 0, cand_b: prof_b['n'] if prof_b else 0}
    return None, info

  dist_a, dist_b = _z_distance(x, y, prof_a), _z_distance(x, y, prof_b)
  info['z_distance'] = {cand_a: dist_a, cand_b: dist_b}

  if dist_a <= dist_b:
    winner, winner_dist, loser_dist = cand_a, dist_a, dist_b
  else:
    winner, winner_dist, loser_dist = cand_b, dist_b, dist_a
  ratio = (loser_dist / winner_dist) if winner_dist > 0 else float('inf')
  info['confidence_ratio'] = ratio

  if ratio < MIN_CONFIDENCE_RATIO:
    info['reason'] = 'ambiguous'
    return None, info

  info['reason'] = 'confident'
  return winner, info


def _apply_alternation(ppr_df, idx, row, passer, teammate):
  """
  Given a confidently-identified passer, derive set_player/att_player via
  the double-contact rule (a player can't touch the ball on two consecutive
  team contacts):
    - 3 touches recorded (pass, set, attack): forced alternation is
      passer -> teammate -> passer.
    - 2 touches, pass + set (ball sent back on the second contact): forced
      alternation is passer -> teammate.
    - 2 touches, pass + attack (no discrete set): forced alternation is
      passer -> teammate.
    - 1 touch (pass only): nothing to derive.

  Writes go through ppr_df.at[...], not the `row` Series (which is a copy
  from iterrows() and would silently discard any assignment).

  If the recorded attacker is on the *other* team, the double-contact rule
  doesn't automatically apply to it:
    - If there's a dig recorded, the rally genuinely went into a longer
      transition -- leave att_player (and the point outcome) alone.
    - If there's no dig, a cross-team attacker with no further touches is
      exactly the shape check_last_point() would have produced a "TE"
      (transition error) outcome from: btd_to_ppr_df's in_trans flag flips
      True the moment a player on the *serving* team touches the ball on a
      non-serve action -- which, if this "attack" was really our passer's
      team finishing a clean first-ball kill, is precisely what a mislabeled
      cross-team attacker would trigger. So: cross-team attacker + no dig +
      the old outcome already being "TE" credited to the other team is
      strong enough evidence to reassign the attack to this team AND fix
      point_outcome/point_outcome_team to match (FBK for this team) --
      otherwise those fields would contradict what pass/set/att now show.
      If the old outcome doesn't match that exact expected shape, it's an
      unexplained case, not a first-ball kill in disguise -- leave it alone
      rather than guess.

  Returns the list of (field, old_value, new_value) changes actually made.
  """
  changes = []
  has_set = row['set_yn'] == 'Y'
  has_att = row['att_yn'] == 'Y'
  own_team = (passer, teammate)

  if row['pass_player'] != passer:
    changes.append(('pass_player', row['pass_player'], passer))
    ppr_df.at[idx, 'pass_player'] = passer

  if has_set and has_att:
    if row['set_player'] != teammate:
      changes.append(('set_player', row['set_player'], teammate))
      ppr_df.at[idx, 'set_player'] = teammate
    if row['att_player'] in own_team and row['att_player'] != passer:
      changes.append(('att_player', row['att_player'], passer))
      ppr_df.at[idx, 'att_player'] = passer
    elif row['att_player'] not in own_team:
      no_dig = row['dig_yn'] != 'Y'
      own_team_str = row['teama'] if passer in (row['player_a1'], row['player_a2']) else row['teamb']
      other_team_str = row['teamb'] if own_team_str == row['teama'] else row['teama']
      old_outcome_fits = row['point_outcome'] == 'TE' and row['point_outcome_team'] == other_team_str
      if no_dig and old_outcome_fits:
        changes.append(('att_player', row['att_player'], passer))
        ppr_df.at[idx, 'att_player'] = passer
        changes.append(('point_outcome', row['point_outcome'], 'FBK'))
        ppr_df.at[idx, 'point_outcome'] = 'FBK'
        changes.append(('point_outcome_team', row['point_outcome_team'], own_team_str))
        ppr_df.at[idx, 'point_outcome_team'] = own_team_str
      # else: dig present (real transition) or outcome doesn't match the
      # expected TE-from-mislabeled-attacker shape -- leave att_player and
      # the point outcome alone.
  elif has_set:
    if row['set_player'] != teammate:
      changes.append(('set_player', row['set_player'], teammate))
      ppr_df.at[idx, 'set_player'] = teammate
  elif has_att:
    if row['att_player'] in own_team and row['att_player'] != teammate:
      changes.append(('att_player', row['att_player'], teammate))
      ppr_df.at[idx, 'att_player'] = teammate

  return changes


def _point_action_ids(row):
  ids = []
  for field in _ACTION_ID_FIELDS:
    val = row.get(field)
    str_val = str(val)
    if val not in (None, 0, '0', 0.0) and str_val not in ids:
      ids.append(str_val)
  return ids


def correct_pass_attribution(ppr_df):
  """
  Detect and, where confident, correct pass/set/attack player mis-attribution
  within a single already-converted ppr_df.

  Only ever modifies ppr_df in place (returned for convenience) -- the BTD
  source data is never touched. Returns (ppr_df, corrections), where
  corrections is a list of dicts (one per conflicting point, resolved or
  not) meant to be serialized as btd_files.corrections_json for audit and
  for the nightly report.
  """
  profiles = build_pass_profiles(ppr_df)
  corrections = []

  for idx, row in ppr_df.iterrows():
    _, has_conflict = _touch_players(row)
    if not has_conflict:
      continue

    passer, info = classify_pass_player(row, profiles)
    entry = {
      'point_no': row['point_no'],
      'video_id': row['video_id'],
      'video_link': build_point_video_link(row['video_id'], _point_action_ids(row)),
      'before': {
        'pass_player': row['pass_player'],
        'set_player': row['set_player'] if row['set_yn'] == 'Y' else None,
        'att_player': row['att_player'] if row['att_yn'] == 'Y' else None,
      },
      'classification': info,
    }

    if passer is None:
      entry['status'] = 'flagged'
      corrections.append(entry)
      continue

    teammate = _teammate(row, passer)
    if teammate is None:
      entry['status'] = 'flagged'
      entry['classification']['reason'] = 'no_teammate_found'
      corrections.append(entry)
      continue

    changes = _apply_alternation(ppr_df, idx, row, passer, teammate)
    entry['status'] = 'corrected' if changes else 'no_change_needed'
    entry['changes'] = changes
    corrections.append(entry)

  return ppr_df, corrections


def team_of(player, row):
  """Return 'A', 'B', or None depending on which roster the player belongs to."""
  if player in (row['player_a1'], row['player_a2']):
    return 'A'
  if player in (row['player_b1'], row['player_b2']):
    return 'B'
  return None


def teammates(team_letter, row):
  if team_letter == 'A':
    return row['player_a1'], row['player_a2']
  else:
    return row['player_b1'], row['player_b2']


def correct_serve_pass_same_team(ppr_df, video_id=None):
  """
  Detects and (where confident) corrects points where pass_player is on
  the same team as serve_player.

  Rule:
    - serve_team == pass_team  ->  bug confirmed
    - receiving_team = the other team
    - if set_player is one of the two receiving_team players ->
          reassign pass_player to the OTHER receiving_team player
          (the one who did NOT set)
    - otherwise (no set_player, or set_player isn't on the receiving
      team) -> do not guess; flag for manual review

  Returns (ppr_df, corrections) in the same shape as
  correct_pass_attribution(), so entries can be appended to the same
  corrections_json list.
  """
  corrections = []

  for index, row in ppr_df.iterrows():
    serve_player = row['serve_player']
    pass_player = row['pass_player']

    if pass_player == 'empty' or serve_player == 'empty':
      continue  # nothing to check - no pass or no serve recorded

    serve_team = team_of(serve_player, row)
    pass_team = team_of(pass_player, row)

    if serve_team is None or pass_team is None:
      # one of the names doesn't match either roster - see Fix 3,
      # not something this function should guess at
      continue

    if serve_team != pass_team:
      continue  # already correct - pass is on the receiving team

    # --- BUG CONFIRMED: pass_player is on the same team as the server ---
    receiving_team = 'B' if serve_team == 'A' else 'A'
    p1, p2 = teammates(receiving_team, row)
    set_player = row['set_player']

    entry = {
      'point_no': int(row['point_no']),
      'video_id': video_id if video_id is not None else row.get('video_id', 'empty'),
      'video_link': (f"https://app.balltime.com/video/{video_id}"
                      if video_id else None),
      'before': {
        'serve_player': serve_player,
        'pass_player': pass_player,
        'set_player': set_player,
      },
      'classification': {
        'error_type': 'serve_and_pass_same_team',
        'receiving_team_candidates': [p1, p2],
      },
    }

    if set_player == p1:
      new_pass = p2
    elif set_player == p2:
      new_pass = p1
    else:
      new_pass = None  # empty, or set_player doesn't resolve cleanly

    if new_pass is not None:
      ppr_df.at[index, 'pass_player'] = new_pass
      entry['classification']['reason'] = 'set_player identifies the setter; pass assigned to the other receiving-team player'
      entry['status'] = 'corrected'
      entry['changes'] = [['pass_player', pass_player, new_pass]]
    else:
      reason = ('no set_player to break the tie' if set_player == 'empty'
                 else f'set_player ({set_player}) is not on the receiving team - deeper issue')
      entry['classification']['reason'] = reason
      entry['status'] = 'flagged'
      entry['changes'] = []

    corrections.append(entry)

  return ppr_df, corrections
