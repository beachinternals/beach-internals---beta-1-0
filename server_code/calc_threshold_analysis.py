import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables
import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import minimize_scalar
from datetime import datetime


# ─────────────────────────────────────────────────────────────────
#  PPR COLUMN REFERENCE  (confirmed from ppr_csv export)
#
#  Event identification:
#    pass_yn / att_yn / dig_yn  — 'Y' or 'N'
#    serve_speed_mph             — > 0 means a serve was tracked
#    serve_dur                   — serve flight time (negative = corrupt record)
#    point_outcome               — 'FBK','FBE','TK','TE','TSE','TSA'
#    point_outcome_team          — team string of the winning team
#    att_player / serve_player   — PPR name strings e.g. "STETSON 22 Zoe"
#
#  Physical metrics and their confirmed data-quality filters:
#
#    serve_speed_mph
#      Base filter  : serve_speed_mph > 0 AND serve_dur > 0
#      Upper cap    : serve_speed_mph < 75
#      Rationale    : 5 records have serve_dur < 0 (timestamp corruption —
#                     speed stored as |dist/dur| from a negative duration).
#                     Speeds ≥ 75 mph are physically implausible for beach
#                     volleyball (women's OR men's game). 12 rows removed
#                     from 1,367 total (<1%).
#
#    att_speed_mph  (first_ball event type)
#      Base filter  : att_yn == 'Y' AND att_speed_mph > 5
#      Upper cap    : att_speed_mph < 65
#      Rationale    : att_speed_mph == 0 means not tracked (450 of 1,554
#                     attack rows). Values 0.09–5 mph are drift/noise in the
#                     tracking system, not real attacks. Values ≥ 65 mph are
#                     implausible for beach volleyball attacks (only 3 values
#                     above 65, vs 95th pct of 43 mph). Clean range: 5–65 mph.
#
#    att_touch_height  (first_ball event type)
#      Base filter  : att_yn == 'Y'
#      Upper cap    : none needed — max observed is 3.63m, all plausible.
#
#    pass_height  (first_ball event type)
#      Base filter  : att_yn == 'Y' AND pass_height > 0
#      Upper cap    : pass_height < 10
#      Rationale    : 308 rows have pass_height ≥ 100m (tracking errors where
#                     height diverges due to physics calculation on bad coords).
#                     Cap at 10m is generous — 99th pct of valid data is ~7m.
#
#    dig_dur  (transition event type)
#      Base filter  : point_outcome IN ('TK','TE') AND dig_dur > 0
#      Upper cap    : dig_dur < 5
#      Rationale    : Outcome-based filter ('TK','TE') isolates true transition
#                     events for our team. TSE/TSA are opponent transition events
#                     and have no dig data for our player (confirmed from data).
#                     dig_dur > 0 removes 134 rows with no dig tracking.
#                     Cap of 5s is a safety net only — max observed is 4.87s.
#                     dig_dur is used instead of dig_height because:
#                     (1) dig_height is derived from dig_dur via kinematics,
#                         so dur is the primary measurement,
#                     (2) dig_dur has no outlier problem (max 4.87s vs
#                         dig_height max 29m with 19 values ≥ 10m).
#
#  EVENT TYPE FILTERING (summary):
#    "first_ball" : att_yn == 'Y'
#                   (includes all attacks — kills, errors, dug balls;
#                    outcome variable handles the result)
#    "transition" : point_outcome IN ('TK', 'TE')
#                   AND dig_dur > 0 AND dig_dur < 5
#    "serve"      : serve_speed_mph > 0
#                   AND serve_dur > 0
#                   AND serve_speed_mph < 75
#
#  COMPUTED OUTCOME COLUMNS (added once per PPR load by _add_outcome_cols):
#    att_kill        binary 1/0 — att_player scored FBK this point
#    att_point_won   binary 1/0 — att_player's team won the point (FBK/TK/TSA)
#    serve_knockout  binary 1/0 — serve created OOS pass (pass_oos>0) or TSA
#    trans_point_won binary 1/0 — att_player's team scored TK this point
#
#  PLAYER MATCHING:
#    PPR uses name strings matching master_player['player_name']
#    e.g. "STETSON 22 Zoe" — must match exactly.
#
#  threshold_analysis_def columns:
#    active, analysis_name, physical_metric, outcome_metric,
#    event_type, outcome_type ("binary"/"continuous"), min_events,
#    description
#
#  threshold_analysis_results columns:
#    analysis_def (link), skill_level, n_events, n_players, n_excluded,
#    physical_min/max/mean/p25/p75,
#    linear_r/p/sig,
#    breakpoint, breakpoint_ci_lo/hi, slope_below/above,
#    slope_change_p, breakpoint_sig, outcome_below/above/change,
#    auc, roc_threshold/sensitivity/specificity,
#    quad_beta1/beta2/optimal_x/r2/shape,
#    calculated_at, notes
#
#  STARTER ROWS FOR threshold_analysis_def:
#    analysis_name              | physical_metric  | outcome_metric  | event_type  | outcome_type | min_events
#    att_speed → kill           | att_speed_mph    | att_kill        | first_ball  | binary       | 30
#    att_speed → point won      | att_speed_mph    | att_point_won   | first_ball  | binary       | 30
#    serve_speed → knockout     | serve_speed_mph  | serve_knockout  | serve       | binary       | 30
#    dig_dur → trans win        | dig_dur          | trans_point_won | transition  | binary       | 30
#    pass_height → kill         | pass_height      | att_kill        | first_ball  | binary       | 30
#    att_touch_height → kill    | att_touch_height | att_kill        | first_ball  | binary       | 30
# ─────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────
#  FILTER CONSTANTS  (all confirmed from data analysis)
# ─────────────────────────────────────────────────────────────────
SERVE_SPEED_MIN   =  0.0    # > 0 (inherent from tracking)
SERVE_SPEED_MAX   = 75.0    # physically implausible above this for beach volleyball
SERVE_DUR_MIN     =  0.0    # > 0 eliminates 5 corrupt records with negative duration

ATT_SPEED_MIN     =  5.0    # below 5 mph is tracking noise, not a real attack
ATT_SPEED_MAX     = 65.0    # implausible for beach volleyball; 95th pct is 43 mph

ATT_TOUCH_HT_MAX  =  5.0    # safety cap; max observed is 3.63m so never triggers

PASS_HEIGHT_MIN   =  0.0    # > 0
PASS_HEIGHT_MAX   = 10.0    # 308 rows ≥ 100m are tracking errors (physics divergence)

DIG_DUR_MIN       =  0.0    # > 0 removes rows with no dig tracking
DIG_DUR_MAX       =  5.0    # safety cap; max observed is 4.87s

# Transition outcomes: only TK (our kill) and TE (our error)
# TSE/TSA are opponent transition events — no dig data for our player
TRANSITION_OUTCOMES = ('TK', 'TE')

# Speed column resolution:
#   preferred  = the _mph column added when Balltime started tracking speed directly
#   fallback   = the m/s distance/time column present in all files
#   multiplier = factor to convert fallback to mph (None = no reliable fallback)
#
# NOTE: serve_speed (m/s) is NOT a reliable fallback — values are ~0.7 m/s
#       (walking pace) and do not convert cleanly to mph. Older serve data
#       is skipped when serve_speed_mph is absent.
#       att_speed (m/s) converts cleanly: att_speed * 2.237 = att_speed_mph exactly.

# Player column to use when filtering PPR rows by event type
_PLAYER_COL = {
  'first_ball': 'att_player',
  'transition': 'att_player',
  'serve':      'serve_player',
}

_SPEED_COL_MAP = {
  'att_speed_mph':   ('att_speed_mph',  'att_speed', 2.237),
  'serve_speed_mph': ('serve_speed_mph', None,        None),
}


def _resolve_physical_col(physical_col, df):
  """
  Return (actual_col, multiplier) for a given logical metric name.

  For speed metrics, prefers the direct _mph column from Balltime.
  Falls back to the m/s column (with unit conversion) only when the
  _mph column is absent AND a reliable fallback exists (att_speed only).

  For non-speed metrics (heights, durations) returns the column as-is.

  Returns (None, None) if no usable column is found in this dataframe.
  """
  if physical_col in _SPEED_COL_MAP:
    preferred, fallback, mult = _SPEED_COL_MAP[physical_col]
    if preferred in df.columns:
      return preferred, 1.0          # direct mph — no conversion needed
    elif fallback and fallback in df.columns:
      return fallback, mult          # m/s fallback — multiply to get mph
    else:
      return None, None              # no usable column in this file
  else:
    # Non-speed metric (pass_height, dig_dur, att_touch_height etc.)
    if physical_col in df.columns:
      return physical_col, 1.0
    return None, None


# ─────────────────────────────────────────────────────────────────
#  HELPER: load PPR dataframe
# ─────────────────────────────────────────────────────────────────
def _load_ppr_df(league, gender, year):
  try:
    from server_functions import get_ppr_data
    df = get_ppr_data(league, gender, year, 'League', scout=False)
    if isinstance(df, list):
      print(f"  No PPR data: {league} {gender} {year}")
      return None
    print(f"  Loaded PPR {league} {gender} {year}: {df.shape[0]} rows")
    return df
  except Exception as e:
    print(f"  PPR load error: {e}")
    return None


# ─────────────────────────────────────────────────────────────────
#  HELPER: add computed binary outcome columns (called once per PPR load)
# ─────────────────────────────────────────────────────────────────
def _add_outcome_cols(df):
  """
    Adds four binary outcome columns to the PPR dataframe.
    All use point_outcome and point_outcome_team from the raw PPR.

    Called once per PPR load — results are cached in the dataframe
    so we don't recompute for every player.
    """
  def in_team(player, team_str):
    return (isinstance(player, str) and
            isinstance(team_str, str) and
            player in team_str)

    # att_kill: attacker scored a first-ball kill (FBK)
  df['att_kill'] = df.apply(
    lambda r: 1 if (r['point_outcome'] == 'FBK' and
                    in_team(r['att_player'], r['point_outcome_team']))
    else 0, axis=1
  )

  # att_point_won: attacker's team won the point by any means
  df['att_point_won'] = df.apply(
    lambda r: 1 if (r['point_outcome'] in ('FBK', 'TK', 'TSA') and
                    in_team(r['att_player'], r['point_outcome_team']))
    else 0, axis=1
  )

  # serve_knockout: serve forced OOS pass or direct opponent error
  pass_oos = df.get('pass_oos', pd.Series(0, index=df.index)).fillna(0)
  df['serve_knockout'] = (
    (pass_oos > 0) | (df['point_outcome'] == 'TSA')
  ).astype(int)

  # trans_point_won: attacker's team scored a transition kill
  df['trans_point_won'] = df.apply(
    lambda r: 1 if (r['point_outcome'] == 'TK' and
                    in_team(r['att_player'], r['point_outcome_team']))
    else 0, axis=1
  )

  return df


# ─────────────────────────────────────────────────────────────────
#  HELPER: extract player name from master_player row
# ─────────────────────────────────────────────────────────────────
def _get_player_name(player_row):
  """Return PPR-format name string from a master_player linked row.
    
    master_player columns: team, number, shortname, fullname
    PPR format is: "TEAM NUMBER SHORTNAME"  e.g. "STETSON 22 Zoe"
    """
  try:
    team      = player_row['team']      or ''
    number    = player_row['number']    or ''
    shortname = player_row['shortname'] or ''
    if team and shortname:
      return f"{team} {number} {shortname}".strip()
  except Exception as e:
    print(f"  _get_player_name error: {e}")
  return None


# ─────────────────────────────────────────────────────────────────
#  HELPER: filter PPR to event type and apply all quality filters,
#          then return (x_array, y_array) for one player
# ─────────────────────────────────────────────────────────────────
def _get_player_events(ppr_df, player_name, physical_col, outcome_col, event_type):
  """
  Return (x_array, y_array) for one player, or (None, None).

  x values are always in consistent units (mph for speed metrics).
  If the preferred _mph column is missing, falls back to m/s with
  conversion where reliable (att_speed only).
  Silently returns (None, None) for missing/unusable columns.
  """
  try:
    pcol = _PLAYER_COL.get(event_type, 'att_player')

    # Resolve which column to actually read, and any unit conversion needed
    actual_col, multiplier = _resolve_physical_col(physical_col, ppr_df)
    if actual_col is None:
      return None, None   # column not available in this file — skip silently

    # Build the event filter mask and apply metric-specific quality filters
    if event_type == 'first_ball':
      mask = ((ppr_df[pcol] == player_name) &
              (ppr_df['att_yn'] == 'Y'))
      df = ppr_df[mask][[actual_col, outcome_col]].copy().dropna()
      if physical_col == 'att_speed_mph':
        df = df[(df[actual_col] > ATT_SPEED_MIN) & (df[actual_col] < ATT_SPEED_MAX)]
      elif physical_col == 'att_touch_height':
        df = df[df[actual_col] < ATT_TOUCH_HT_MAX]
      elif physical_col == 'pass_height':
        df = df[(df[actual_col] > PASS_HEIGHT_MIN) & (df[actual_col] < PASS_HEIGHT_MAX)]

    elif event_type == 'transition':
      mask = ((ppr_df[pcol] == player_name) &
              (ppr_df['point_outcome'].isin(TRANSITION_OUTCOMES)) &
              (ppr_df['dig_dur'] > DIG_DUR_MIN) &
              (ppr_df['dig_dur'] < DIG_DUR_MAX))
      df = ppr_df[mask][[actual_col, outcome_col]].copy().dropna()

    elif event_type == 'serve':
      mask = ((ppr_df[pcol] == player_name) &
              (ppr_df[actual_col] > SERVE_SPEED_MIN) &
              (ppr_df[actual_col] < SERVE_SPEED_MAX))
      # Also require serve_dur > 0 if that column exists (removes corrupt records)
      if 'serve_dur' in ppr_df.columns:
        mask = mask & (ppr_df['serve_dur'] > SERVE_DUR_MIN)
      df = ppr_df[mask][[actual_col, outcome_col]].copy().dropna()

    else:
      return None, None

    if len(df) == 0:
      return None, None

    x = df[actual_col].to_numpy(float)
    if multiplier != 1.0:
      x = x * multiplier    # convert m/s → mph so units are consistent

    y = df[outcome_col].to_numpy(float)
    return x, y

  except Exception as e:
    print(f"  Event error for {player_name}: {e}")
    return None, None


# ─────────────────────────────────────────────────────────────────
#  ANALYSIS 1: Linear baseline (Pearson r)
#  — establishes whether any relationship exists before threshold testing
# ─────────────────────────────────────────────────────────────────
def _linear_analysis(x, y):
  try:
    r, p = stats.pearsonr(x, y)
    return {
      'linear_r':   round(float(r), 4),
      'linear_p':   round(float(p), 4),
      'linear_sig': bool(p < 0.05),
    }
  except Exception as e:
    print(f"  Linear failed: {e}")
    return {'linear_r': None, 'linear_p': None, 'linear_sig': False}


# ─────────────────────────────────────────────────────────────────
#  ANALYSIS 2: Segmented regression (piecewise linear)
#
#  Model: y = b0 + b1*x + b2*(x - psi)_+   where (z)_+ = max(0,z)
#
#  psi (the breakpoint) is found by:
#    1. Grid search across 100 candidate values between the 15th and
#       85th percentiles of x (ensures ≥15% of data on each side)
#    2. Refinement with scipy minimize_scalar around the grid minimum
#    3. 200-iteration bootstrap to get 95% CI on the breakpoint
#
#  Significance: F-test comparing piecewise model (3 params) to linear
#  model (2 params). df1=1 (one extra parameter), df2=n-4.
# ─────────────────────────────────────────────────────────────────
def _segmented_regression(x, y, n_bootstrap=200, min_pct=0.15):
  result = {
    'breakpoint':       None, 'breakpoint_ci_lo': None,
    'breakpoint_ci_hi': None, 'slope_below':      None,
    'slope_above':      None, 'slope_change_p':   None,
    'breakpoint_sig':   False,
    'outcome_below':    None, 'outcome_above':    None,
    'outcome_change':   None,
  }
  n = len(x)
  if n < 50:
    print(f"  Segmented: n={n} < 50, skipping")
    return result

  try:
    x_lo = np.percentile(x, min_pct * 100)
    x_hi = np.percentile(x, (1 - min_pct) * 100)
    if x_hi <= x_lo:
      print("  Segmented: insufficient x range")
      return result

    def rss_at(psi):
      h = np.maximum(0, x - psi)
      X = np.column_stack([np.ones(n), x, h])
      c, res, _, _ = np.linalg.lstsq(X, y, rcond=None)
      return float(res[0]) if len(res) else float(np.sum((y - X @ c) ** 2))

        # Grid search
    grid    = np.linspace(x_lo, x_hi, 100)
    rss_g   = [rss_at(g) for g in grid]
    best_i  = int(np.argmin(rss_g))

        # Refine
    lo_b = grid[max(0, best_i - 5)]
    hi_b = grid[min(99, best_i + 5)]
    opt  = minimize_scalar(rss_at, bounds=(lo_b, hi_b), method='bounded')
    psi  = float(opt.x if opt.success else grid[best_i])

        # Fit piecewise model at best psi
    h    = np.maximum(0, x - psi)
    Xpw  = np.column_stack([np.ones(n), x, h])
    c_pw, _, _, _ = np.linalg.lstsq(Xpw, y, rcond=None)
    b0, b1, b2 = float(c_pw[0]), float(c_pw[1]), float(c_pw[2])

        # F-test vs plain linear model
    Xlin = np.column_stack([np.ones(n), x])
    c_ln, _, _, _ = np.linalg.lstsq(Xlin, y, rcond=None)
    rss_lin = float(np.sum((y - Xlin @ c_ln) ** 2))
    rss_seg = float(np.sum((y - Xpw  @ c_pw) ** 2))
    df2     = n - 4
    p_chg   = 1.0
    if rss_lin > rss_seg > 0 and df2 > 0:
            f     = ((rss_lin - rss_seg) / 1.0) / (rss_seg / df2)
            p_chg = float(1 - stats.f.cdf(f, 1, df2))

        # Bootstrap CI on breakpoint
    rng   = np.random.default_rng(42)
    boots = []
    for _ in range(n_bootstrap):
            idx  = rng.integers(0, n, n)
            xb, yb = x[idx], y[idx]
            lo_bb = np.percentile(xb, min_pct * 100)
            hi_bb = np.percentile(xb, (1 - min_pct) * 100)
            if hi_bb <= lo_bb:
                continue
            gb  = np.linspace(lo_bb, hi_bb, 60)
            rb  = []
            for g in gb:
                hb = np.maximum(0, xb - g)
                Xb = np.column_stack([np.ones(n), xb, hb])
                cb, resb, _, _ = np.linalg.lstsq(Xb, yb, rcond=None)
                rb.append(float(resb[0]) if len(resb)
                          else float(np.sum((yb - Xb @ cb) ** 2)))
            boots.append(float(gb[int(np.argmin(rb))]))

    ci_lo = float(np.percentile(boots, 2.5))  if boots else psi
    ci_hi = float(np.percentile(boots, 97.5)) if boots else psi

    below = y[x <= psi]
    above = y[x >  psi]
    ob    = float(np.mean(below)) if len(below) else None
    oa    = float(np.mean(above)) if len(above) else None

    result.update({
            'breakpoint':       round(psi, 3),
            'breakpoint_ci_lo': round(ci_lo, 3),
            'breakpoint_ci_hi': round(ci_hi, 3),
            'slope_below':      round(b1, 6),
            'slope_above':      round(b1 + b2, 6),
            'slope_change_p':   round(p_chg, 4),
            'breakpoint_sig':   bool(p_chg < 0.05),
            'outcome_below':    round(ob, 4) if ob is not None else None,
            'outcome_above':    round(oa, 4) if oa is not None else None,
            'outcome_change':   round(oa - ob, 4) if (ob is not None and oa is not None) else None,
        })
    print(f"    Breakpoint={psi:.2f} [{ci_lo:.2f}–{ci_hi:.2f}]  "
              f"p_change={p_chg:.4f}  "
              + (f"outcome {ob:.3f}→{oa:.3f}" if ob is not None else ""))

  except Exception as e:
        print(f"  Segmented failed: {e}")

  return result


# ─────────────────────────────────────────────────────────────────
#  ANALYSIS 3: ROC / optimal cutpoint  (binary outcomes only)
#
#  Finds the threshold of x that best discriminates outcome=1 from
#  outcome=0 by maximising the Youden index (sensitivity + specificity - 1).
#  Implemented without sklearn — uses a sort-and-accumulate approach.
#  Returns AUC (trapezoidal), optimal cutpoint, sensitivity, specificity.
# ─────────────────────────────────────────────────────────────────
def _roc_analysis(x, y_binary):
    result = {
        'auc': None, 'roc_threshold':   None,
        'roc_sensitivity': None, 'roc_specificity': None,
    }
    try:
        y     = y_binary.astype(int)
        n_pos = int(y.sum())
        n_neg = int(len(y) - n_pos)
        if n_pos < 5 or n_neg < 5:
            print("  ROC: insufficient positive/negative events")
            return result

        # Sort descending by x — sweep thresholds from high to low
        order    = np.argsort(-x)
        xs, ys   = x[order], y[order]
        tprs, fprs, threshs = [0.0], [0.0], []
        tp = fp  = 0
        for xi, yi in zip(xs, ys):
            if yi: tp += 1
            else:  fp += 1
            tprs.append(tp / n_pos)
            fprs.append(fp / n_neg)
            threshs.append(float(xi))

        tprs = np.array(tprs)
        fprs = np.array(fprs)
        auc  = float(abs(np.trapz(tprs, fprs)))
        if auc < 0.5:
            auc = 1.0 - auc   # convention: AUC ≥ 0.5

        # Youden index = TPR - FPR
        youden = tprs[1:] - fprs[1:]
        bi     = int(np.argmax(youden))
        result.update({
            'auc':             round(auc, 4),
            'roc_threshold':   round(threshs[bi], 3),
            'roc_sensitivity': round(float(tprs[bi + 1]), 4),
            'roc_specificity': round(float(1 - fprs[bi + 1]), 4),
        })
        print(f"    ROC AUC={auc:.3f}  cut={threshs[bi]:.2f}  "
              f"sens={tprs[bi+1]:.3f}  spec={1-fprs[bi+1]:.3f}")

    except Exception as e:
        print(f"  ROC failed: {e}")

    return result


# ─────────────────────────────────────────────────────────────────
#  ANALYSIS 4: Quadratic regression  (optimal range detection)
#
#  Fits: y = b0 + b1*x_norm + b2*x_norm²   (x normalised for stability)
#  then back-transforms to original x units.
#
#  Shape classification:
#    inverted_U — b2 < 0 with |b2| > 10% of |b1|  → sweet spot exists
#    U_shape    — b2 > 0 with |b2| > 10% of |b1|  → extremes are better
#    linear     — |b2| ≤ 10% of |b1|               → relationship is linear
#
#  optimal_x = vertex of parabola = -b1 / (2*b2) in original units
#  (meaningful only for inverted_U and U_shape)
# ─────────────────────────────────────────────────────────────────
def _quadratic_analysis(x, y):
    result = {
        'quad_beta1': None, 'quad_beta2':    None,
        'quad_optimal_x': None, 'quad_r2':  None,
        'quad_shape': None,
    }
    if len(x) < 50:
        return result

    try:
        xm, xs = float(np.mean(x)), float(np.std(x))
        if xs == 0:
            return result
        xn = (x - xm) / xs

        X   = np.column_stack([np.ones(len(x)), xn, xn ** 2])
        c, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        b0, b1, b2 = float(c[0]), float(c[1]), float(c[2])

        yh   = X @ c
        ss_r = float(np.sum((y - yh) ** 2))
        ss_t = float(np.sum((y - np.mean(y)) ** 2))
        r2   = float(1 - ss_r / ss_t) if ss_t > 0 else 0.0

        # Vertex in original x units
        opt_x = (float(-b1 / (2 * b2) * xs + xm)
                 if abs(b2) > 1e-10 else None)

        # Shape: b2 must be > 10% of |b1| to be considered non-linear
        if abs(b2) < 0.1 * (abs(b1) + 1e-10):
            shape = 'linear'
        elif b2 < 0:
            shape = 'inverted_U'
        else:
            shape = 'U_shape'

        result.update({
            'quad_beta1':     round(b1, 6),
            'quad_beta2':     round(b2, 6),
            'quad_optimal_x': round(opt_x, 3) if opt_x is not None else None,
            'quad_r2':        round(r2, 4),
            'quad_shape':     shape,
        })
        print(f"    Quadratic shape={shape}  R²={r2:.3f}"
              + (f"  optimal_x={opt_x:.2f}" if opt_x else ""))

    except Exception as e:
        print(f"  Quadratic failed: {e}")

    return result


# ─────────────────────────────────────────────────────────────────
#  MAIN CALCULATION
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def calc_threshold_analysis():
    """
    For each active row in threshold_analysis_def, and each skill level
    in skill_level_def, pools raw PPR rally events across all players
    at that level and runs four analyses:
      1. Linear baseline (Pearson r)
      2. Segmented regression (piecewise linear, bootstrap breakpoint CI)
      3. ROC / optimal cutpoint  (binary outcomes only)
      4. Quadratic regression    (optimal range / inverted-U detection)

    Player matching: master_player['player_name'] must match the PPR
    name strings exactly (e.g. "STETSON 22 Zoe").

    Deletes all existing results before recalculating.
    Returns a summary string.
    """
    print("=== calc_threshold_analysis: starting ===")

    defs = [r for r in app_tables.threshold_analysis_def.search()
            if r['active']]
    if not defs:
        return "No active threshold analysis definitions found"
    print(f"Found {len(defs)} active definitions")

    skill_levels = list(app_tables.skill_level_def.search())
    if not skill_levels:
        return "No skill levels found"
    print(f"Found {len(skill_levels)} skill levels")

    deleted = 0
    for row in app_tables.threshold_analysis_results.search():
        row.delete()
        deleted += 1
    print(f"Deleted {deleted} existing results")

    saved = skipped = 0

    for ad in defs:
        physical_col = ad['physical_metric']
        outcome_col  = ad['outcome_metric']
        event_type   = ad['event_type']
        outcome_type = ad['outcome_type']   # "binary" or "continuous"
        min_events   = ad['min_events'] or 30
        name         = ad['analysis_name']

        print(f"\n=== {name}  ({physical_col} → {outcome_col}, {event_type}) ===")

        for sl in skill_levels:
            level_name  = sl['level_name']
            player_list = sl['player_list']

            if not player_list or len(player_list) < 3:
                print(f"  Skipping {level_name}: fewer than 3 players")
                skipped += 1
                continue

            all_x  = []
            all_y  = []
            n_inc  = 0
            n_exc  = 0

            cur_lgy = None
            cur_ppr = None

            for pr in player_list:
                try:
                    league      = pr['league']
                    gender      = pr['gender']
                    year        = pr['year']
                    player_name = _get_player_name(pr)
                except Exception as e:
                    print(f"  Player row error: {e}")
                    continue

                if not player_name:
                    n_exc += 1
                    continue

                lgy = f"{league}|{gender}|{year}"
                if lgy != cur_lgy:
                    raw = _load_ppr_df(league, gender, year)
                    cur_ppr = _add_outcome_cols(raw) if raw is not None else None
                    cur_lgy = lgy

                if cur_ppr is None:
                    continue

                xv, yv = _get_player_events(
                    cur_ppr, player_name,
                    physical_col, outcome_col, event_type
                )

                if xv is None or len(xv) < min_events:
                    n_exc += 1
                    continue

                all_x.extend(xv.tolist())
                all_y.extend(yv.tolist())
                n_inc += 1

            n_events = len(all_x)
            print(f"  {level_name}: {n_inc} players, "
                  f"{n_exc} excluded, {n_events} events")

            # Need at least 3× min_events pooled, and at least 100 total
            min_pooled = max(min_events * 3, 100)
            if n_events < min_pooled:
                print(f"  Skipping: {n_events} events (need {min_pooled})")
                skipped += 1
                continue

            xa = np.array(all_x, dtype=float)
            ya = np.array(all_y, dtype=float)

            # Descriptive stats on the physical variable
            desc = {
                'physical_min':  round(float(np.min(xa)), 4),
                'physical_max':  round(float(np.max(xa)), 4),
                'physical_mean': round(float(np.mean(xa)), 4),
                'physical_p25':  round(float(np.percentile(xa, 25)), 4),
                'physical_p75':  round(float(np.percentile(xa, 75)), 4),
            }

            lin  = _linear_analysis(xa, ya)
            seg  = _segmented_regression(xa, ya)

            roc = {
                'auc': None, 'roc_threshold': None,
                'roc_sensitivity': None, 'roc_specificity': None,
            }
            if outcome_type == 'binary':
                roc = _roc_analysis(xa, ya)

            quad = _quadratic_analysis(xa, ya)

            try:
                app_tables.threshold_analysis_results.add_row(
                    analysis_def     = ad,
                    skill_level      = level_name,
                    n_events         = n_events,
                    n_players        = n_inc,
                    n_excluded       = n_exc,
                    physical_min     = desc['physical_min'],
                    physical_max     = desc['physical_max'],
                    physical_mean    = desc['physical_mean'],
                    physical_p25     = desc['physical_p25'],
                    physical_p75     = desc['physical_p75'],
                    linear_r         = lin['linear_r'],
                    linear_p         = lin['linear_p'],
                    linear_sig       = lin['linear_sig'],
                    breakpoint       = seg['breakpoint'],
                    breakpoint_ci_lo = seg['breakpoint_ci_lo'],
                    breakpoint_ci_hi = seg['breakpoint_ci_hi'],
                    slope_below      = seg['slope_below'],
                    slope_above      = seg['slope_above'],
                    slope_change_p   = seg['slope_change_p'],
                    breakpoint_sig   = seg['breakpoint_sig'],
                    outcome_below    = seg['outcome_below'],
                    outcome_above    = seg['outcome_above'],
                    outcome_change   = seg['outcome_change'],
                    auc              = roc['auc'],
                    roc_threshold    = roc['roc_threshold'],
                    roc_sensitivity  = roc['roc_sensitivity'],
                    roc_specificity  = roc['roc_specificity'],
                    quad_beta1       = quad['quad_beta1'],
                    quad_beta2       = quad['quad_beta2'],
                    quad_optimal_x   = quad['quad_optimal_x'],
                    quad_r2          = quad['quad_r2'],
                    quad_shape       = quad['quad_shape'],
                    calculated_at    = datetime.now(),
                    notes            = (f"{name} | {level_name} | "
                                        f"n={n_events} events / {n_inc} players")
                )
                saved += 1
            except Exception as e:
                print(f"  Save failed: {e}")
                skipped += 1

    summary = f"Done. {saved} results saved, {skipped} skipped."
    print(summary)
    return summary


# ─────────────────────────────────────────────────────────────────
#  BACKGROUND TASK + LAUNCHER  (matches calc_metric_correlations pattern)
# ─────────────────────────────────────────────────────────────────
@anvil.server.background_task
def bg_calc_threshold_analysis():
    result = calc_threshold_analysis()
    anvil.server.task_state['status']  = 'complete'
    anvil.server.task_state['message'] = result


@anvil.server.callable
def launch_threshold_analysis():
    task = anvil.server.launch_background_task('bg_calc_threshold_analysis')
    return task.get_id()


@anvil.server.callable
def get_threshold_task_status(task_id):
    try:
        task = anvil.server.get_background_task(task_id)
        if task.is_completed():
            state = task.get_state()
            return {'done':    True,
                    'status':  state.get('status', 'complete'),
                    'message': state.get('message', 'Done')}
        elif task.get_error():
            return {'done':    True,
                    'status':  'error',
                    'message': str(task.get_error())}
        else:
            return {'done':    False,
                    'status':  'running',
                    'message': 'Calculating...'}
    except Exception as e:
        return {'done': True, 'status': 'error', 'message': str(e)}


# ─────────────────────────────────────────────────────────────────
#  FETCH RESULTS FOR DISPLAY
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def get_threshold_analysis_results(analysis_name=None, skill_level=None):
    """
    Returns all threshold analysis results as a list of dicts.
    Optional filters: analysis_name, skill_level.
    Sorted: significant breakpoints first, then by outcome_change magnitude.
    """
    results = []
    for row in app_tables.threshold_analysis_results.search():
        ad = row['analysis_def']
        entry = {
            'analysis_name':    ad['analysis_name']   if ad else '',
            'physical_metric':  ad['physical_metric']  if ad else '',
            'outcome_metric':   ad['outcome_metric']   if ad else '',
            'event_type':       ad['event_type']        if ad else '',
            'outcome_type':     ad['outcome_type']      if ad else '',
            'skill_level':      row['skill_level'],
            'n_events':         row['n_events'],
            'n_players':        row['n_players'],
            'n_excluded':       row['n_excluded'],
            'physical_min':     row['physical_min'],
            'physical_max':     row['physical_max'],
            'physical_mean':    row['physical_mean'],
            'physical_p25':     row['physical_p25'],
            'physical_p75':     row['physical_p75'],
            'linear_r':         row['linear_r'],
            'linear_p':         row['linear_p'],
            'linear_sig':       row['linear_sig'],
            'breakpoint':       row['breakpoint'],
            'breakpoint_ci_lo': row['breakpoint_ci_lo'],
            'breakpoint_ci_hi': row['breakpoint_ci_hi'],
            'slope_below':      row['slope_below'],
            'slope_above':      row['slope_above'],
            'slope_change_p':   row['slope_change_p'],
            'breakpoint_sig':   row['breakpoint_sig'],
            'outcome_below':    row['outcome_below'],
            'outcome_above':    row['outcome_above'],
            'outcome_change':   row['outcome_change'],
            'auc':              row['auc'],
            'roc_threshold':    row['roc_threshold'],
            'roc_sensitivity':  row['roc_sensitivity'],
            'roc_specificity':  row['roc_specificity'],
            'quad_beta1':       row['quad_beta1'],
            'quad_beta2':       row['quad_beta2'],
            'quad_optimal_x':   row['quad_optimal_x'],
            'quad_r2':          row['quad_r2'],
            'quad_shape':       row['quad_shape'],
            'calculated_at':    str(row['calculated_at']),
            'notes':            row['notes'],
        }
        if analysis_name and entry['analysis_name'] != analysis_name:
            continue
        if skill_level and entry['skill_level'] != skill_level:
            continue
        results.append(entry)

    results.sort(key=lambda r: (
        0 if r['breakpoint_sig'] else 1,
        -abs(r['outcome_change'] or 0)
    ))
    return results