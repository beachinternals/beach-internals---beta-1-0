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


# ============================================================================
# HAWAIIAN ROTATION HANDLING (Aug 2026)
# ============================================================================
# Some teams anchor each player to a fixed physical side of the court
# ("Hawaiian" rotation) rather than a fixed role relative to the net. A
# normal player's role-normalized x position (what's stored in the ppr
# file) stays about the same whether her team is defending near court or
# far court. A Hawaiian player's flips to roughly (8 - x), because she's
# standing in the same physical spot while facing the opposite direction.
#
# We only decide this once per team, using the whole match -- a team could
# in principle switch styles between sets or mid-match, but that's beyond
# what error-correction can reasonably chase; we treat one file as one
# style. And we test each player against her OWN near vs. far mean, not by
# pooling both teammates' raw positions together -- pooling two players who
# sit on genuinely opposite, fixed sides of a NORMAL court averages out to
# somewhere near the center line regardless of near/far, which looks like a
# false Hawaiian signal. Testing individually and then combining avoids that.
MIN_HAWAIIAN_SIDE_SAMPLES = 2  # min clean samples a player needs on EACH side of the court to weigh in on her own flip-or-not signal

def _player_side_signal(samples, player):
  near_pts = samples.get(player, {}).get(True, [])
  far_pts = samples.get(player, {}).get(False, [])
  if len(near_pts) < MIN_HAWAIIAN_SIDE_SAMPLES or len(far_pts) < MIN_HAWAIIAN_SIDE_SAMPLES:
    return None
  near_mean = sum(p[0] for p in near_pts) / len(near_pts)
  far_mean = sum(p[0] for p in far_pts) / len(far_pts)
  return (near_mean - 4) * (far_mean - 4) < 0  # opposite sides of center -> Hawaiian signal

def _team_is_hawaiian(samples, p1, p2):
  signals = [s for s in (_player_side_signal(samples, p1), _player_side_signal(samples, p2)) if s is not None]
  if not signals:
    return None  # not enough data from either player to call it -- treat as normal/blended, don't guess
  return any(signals)

def _profile_from_samples(pts):
  n = len(pts)
  mean_x = sum(p[0] for p in pts) / n
  mean_y = sum(p[1] for p in pts) / n
  if n > 1:
    var_x = sum((p[0] - mean_x) ** 2 for p in pts) / (n - 1)
    var_y = sum((p[1] - mean_y) ** 2 for p in pts) / (n - 1)
  else:
    var_x = var_y = 0.0
  std_x = max(var_x ** 0.5, 0.5)
  std_y = max(var_y ** 0.5, 0.5)
  return {'mean_x': mean_x, 'mean_y': mean_y, 'std_x': std_x, 'std_y': std_y, 'n': n}

def _mirror_profile(profile):
  # Reflect a profile around center court (x=4) -- stands in for a
  # Hawaiian player's thin side when her other side has solid data.
  # y is untouched; only left/right flips. 'derived' marks it as a
  # stand-in rather than a profile built from real samples on that side.
  return {
    'mean_x': 8 - profile['mean_x'],
    'mean_y': profile['mean_y'],
    'std_x': profile['std_x'],
    'std_y': profile['std_y'],
    'n': profile['n'],
    'derived': True,
  }

def build_pass_profiles(ppr_df):
  """
  Builds per-player pass-position data from this match's clean points.

  Unlike before, this does NOT collapse each player straight down to a
  fixed mean/std profile. It keeps the raw (x, y, point_no) samples,
  because classify_pass_player needs to rebuild each candidate's profile
  fresh for every point it judges -- excluding that specific point's own
  touch from the sample pool first (see _build_profile_leave_one_out).

  Why: a point's OWN touch data can be wrong. "Clean" here only means
  "no back-to-back same-player conflict" -- it does NOT mean "verified
  correct." If a genuinely mislabeled point slips past that conflict
  check (very possible -- see Point 3), the old code would use that
  point's own coordinates to help build the profile it was about to be
  judged against. That lets one wrong touch quietly poison the very
  measuring stick used to catch it, and can produce a confidently WRONG
  answer instead of an honestly ambiguous one. Leave-one-out closes that
  hole: a point is never allowed to vouch for itself.
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
    nc = bool(row['near_court'])
    samples.setdefault(row['pass_player'], {True: [], False: []})[nc].append((x, y, row['point_no']))

  if ppr_df.empty:
    return {'samples': {}, 'hawaiian_status': {}, 'teams': []}

  ref = ppr_df.iloc[0]
  teams = [(ref['player_a1'], ref['player_a2']), (ref['player_b1'], ref['player_b2'])]
  hawaiian_status = {(p1, p2): _team_is_hawaiian(samples, p1, p2) for (p1, p2) in teams}

  return {'samples': samples, 'hawaiian_status': hawaiian_status, 'teams': teams}


def _build_profile_leave_one_out(samples, hawaiian, player, near_court, exclude_point_no):
  """
  Builds one player's position profile for one side of the court (near or
  far), excluding the sample from exclude_point_no -- the point currently
  being judged. This is what stops a point from being trained on itself.
  """
  near_pts = [(x, y) for (x, y, pn) in samples.get(player, {}).get(True, []) if pn != exclude_point_no]
  far_pts = [(x, y) for (x, y, pn) in samples.get(player, {}).get(False, []) if pn != exclude_point_no]

  if not hawaiian:
    pooled = near_pts + far_pts
    return _profile_from_samples(pooled) if pooled else None

  near_prof = _profile_from_samples(near_pts) if near_pts else None
  far_prof = _profile_from_samples(far_pts) if far_pts else None
  if near_prof and not far_prof:
    far_prof = _mirror_profile(near_prof)
  if far_prof and not near_prof:
    near_prof = _mirror_profile(far_prof)
  return near_prof if near_court else far_prof


def classify_pass_player(row, profile_data):
  samples = profile_data['samples']
  hawaiian_status = profile_data['hawaiian_status']
  teams = profile_data['teams']

  cand_a, cand_b = _receiving_team_players(row)
  x, y = row['pass_src_x'], row['pass_src_y']
  info = {'candidates': [cand_a, cand_b], 'pass_src': [x, y]}
  if not cand_a or not cand_b:
    info['reason'] = 'could_not_determine_receiving_team'
    return None, info
  if not _is_plausible_coord(x, y):
    info['reason'] = 'missing_or_implausible_pass_coords'
    return None, info

  nc = bool(row['near_court'])
  info['near_court'] = nc

  team_key = next((t for t in teams if cand_a in t and cand_b in t), None)
  hawaiian = hawaiian_status.get(team_key)
  exclude_pn = row['point_no']

  prof_a = _build_profile_leave_one_out(samples, hawaiian, cand_a, nc, exclude_pn)
  prof_b = _build_profile_leave_one_out(samples, hawaiian, cand_b, nc, exclude_pn)

  if not prof_a or not prof_b or prof_a['n'] < MIN_CLEAN_SAMPLES or prof_b['n'] < MIN_CLEAN_SAMPLES:
    info['reason'] = 'insufficient_clean_samples'
    info['sample_sizes'] = {cand_a: prof_a['n'] if prof_a else 0, cand_b: prof_b['n'] if prof_b else 0}
    return None, info

  if prof_a.get('derived') or prof_b.get('derived'):
    info['used_derived_profile'] = True

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
  

def _z_distance(x, y, profile):
  dx = (x - profile['mean_x']) / profile['std_x']
  dy = (y - profile['mean_y']) / profile['std_y']
  return math.sqrt(dx * dx + dy * dy)




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
      'video_link': build_point_video_link(
        video_id if video_id is not None else row.get('video_id'), _point_action_ids(row)),
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


def partner_of(player, team_letter, row):
  p1, p2 = teammates(team_letter, row)
  return p2 if player == p1 else p1


def is_known_good(player, receiving_team, row):
  if player in ('empty', 'UNMATCHED_PLAYER', None):
    return False
  return team_of(player, row) == receiving_team


def correct_missing_touches(ppr_df, video_id=None):
  """
  Fills in pass_player, set_player, and att_player where the _yn flag says
  the touch happened but the player identity is missing/unmatched/still
  wrong, using whichever of the other two touches are already trustworthy
  as an anchor. Flags (does not guess) when there's no usable anchor, or
  when pass_player and att_player disagree in a way that makes set_player
  ambiguous.

  Resolves pass, then att, then set within a point, so a later derivation
  can use an anchor derived earlier in the same point.

  Returns (ppr_df, corrections) in the same shape as the existing
  correction functions, for appending to the same corrections_json list.
  """
  corrections = []

  for index, row in ppr_df.iterrows():
    serve_player = row['serve_player']
    serve_team = team_of(serve_player, row)
    if serve_team is None:
      continue  # can't establish receiving team without a valid serve_player
    receiving_team = 'B' if serve_team == 'A' else 'A'

    pass_yn = row['pass_yn']
    set_yn = row['set_yn']
    att_yn = row['att_yn']

    pass_player = row['pass_player']
    set_player = row['set_player']
    att_player = row['att_player']

    point_changes = []
    point_reasons = []

    # ---- 1. pass_player ----
    if pass_yn == "Y" and not is_known_good(pass_player, receiving_team, row):
      new_pass = None
      reason = None
      if set_yn == "Y" and is_known_good(set_player, receiving_team, row):
        new_pass = partner_of(set_player, receiving_team, row)
        reason = 'derived from set_player (partner)'
      elif set_yn == "N" and att_yn == "Y" and is_known_good(att_player, receiving_team, row):
        new_pass = partner_of(att_player, receiving_team, row)
        reason = 'derived from att_player (partner, no set occurred)'
      elif set_yn == "Y" and att_yn == "Y" and is_known_good(att_player, receiving_team, row):
        new_pass = att_player
        reason = 'derived from att_player (same person, set occurred)'

      if new_pass is not None:
        ppr_df.at[index, 'pass_player'] = new_pass
        point_changes.append(['pass_player', pass_player, new_pass])
        point_reasons.append(f'pass_player: {reason}')
        pass_player = new_pass  # so later derivations in this row see the update
      elif set_yn == "N" and att_yn == "N" and team_of(pass_player, row) != serve_team:
        # Terminal point -- no set/attack ever happened, so there's no other
        # touch to anchor a derivation on. But pass_player isn't sitting on
        # the serving team either (it's just unresolved/empty), so there's
        # no contradiction -- which of the 2 receiving players touched it is
        # structurally unknowable, not a data error. Nothing to flag.
        pass
      else:
        point_reasons.append('pass_player: no usable anchor - flagged')

    # ---- 2. att_player (before set_player, so set_player can use it) ----
    if att_yn == "Y" and not is_known_good(att_player, receiving_team, row):
      new_att = None
      reason = None
      if set_yn == "Y" and is_known_good(pass_player, receiving_team, row):
        new_att = pass_player
        reason = 'derived from pass_player (same person, set occurred)'
      elif set_yn == "N" and is_known_good(pass_player, receiving_team, row):
        new_att = partner_of(pass_player, receiving_team, row)
        reason = 'derived from pass_player (partner, no set occurred)'
      elif set_yn == "Y" and is_known_good(set_player, receiving_team, row):
        new_att = partner_of(set_player, receiving_team, row)
        reason = 'derived from set_player (partner)'

      if new_att is not None:
        ppr_df.at[index, 'att_player'] = new_att
        point_changes.append(['att_player', att_player, new_att])
        point_reasons.append(f'att_player: {reason}')
        att_player = new_att
      else:
        point_reasons.append('att_player: no usable anchor - flagged')

    # ---- 3. set_player (last, so it can use freshly-derived pass/att) ----
    if set_yn == "Y" and not is_known_good(set_player, receiving_team, row):
      pass_good = is_known_good(pass_player, receiving_team, row)
      att_good = is_known_good(att_player, receiving_team, row)
      new_set = None
      reason = None

      if pass_good and att_good and pass_player != att_player:
        reason = (f'CONTRADICTION - pass_player ({pass_player}) and '
                   f'att_player ({att_player}) disagree; cannot determine '
                   f'set_player - flagged for manual review')
      elif pass_good:
        new_set = partner_of(pass_player, receiving_team, row)
        reason = 'derived from pass_player (partner)'
      elif att_good:
        new_set = partner_of(att_player, receiving_team, row)
        reason = 'derived from att_player (partner)'
      else:
        reason = 'set_player: no usable anchor - flagged'

      if new_set is not None:
        ppr_df.at[index, 'set_player'] = new_set
        point_changes.append(['set_player', set_player, new_set])
      point_reasons.append(reason if new_set is not None else f'set_player: {reason}')

    if point_changes or point_reasons:
      corrections.append({
        'point_no': int(row['point_no']),
        'video_id': video_id if video_id is not None else row.get('video_id', 'empty'),
        'video_link': build_point_video_link(
          video_id if video_id is not None else row.get('video_id'), _point_action_ids(row)),
        'classification': {
          'error_type': 'missing_or_unmatched_touch',
          'notes': point_reasons,
        },
        'status': 'corrected' if point_changes else 'flagged',
        'changes': point_changes,
      })

  return ppr_df, corrections
