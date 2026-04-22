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
#    point_outcome               — 'FBK','FBE','TK','TE','TSE','TSA'
#    point_outcome_team          — team string of the winning team
#    att_player / pass_player / dig_player / serve_player — name strings
#                                   e.g. "STETSON 22 Zoe"
#
#  Physical metrics available:
#    att_speed_mph       attack speed in mph   (0 = not tracked)
#    att_touch_height    contact height in m   (outlier cap: 5.0m)
#    serve_speed_mph     serve speed in mph    (0 = not tracked)
#    pass_height         pass peak height m    (outlier cap: 10.0m)
#    dig_height          dig peak height m     (outlier cap: 10.0m)
#    dig_dur             dig flight time s     (outlier cap: 10.0s)
#
#  Computed outcome columns (added by _add_outcome_cols):
#    att_kill        binary — att_player scored FBK
#    att_point_won   binary — att_player's team won the point
#    serve_knockout  binary — serve forced OOS or opponent error (TSA)
#    trans_point_won binary — att_player's team scored TK
#
#  EVENT TYPE FILTERING:
#    "first_ball" : pass_yn=='Y' AND att_yn=='Y'
#    "transition" : dig_yn=='Y'  AND att_yn=='Y'
#    "serve"      : serve_speed_mph > 0
#
#  PLAYER MATCHING:
#    PPR uses name strings matching master_player['player_name']
#    e.g. "STETSON 22 Zoe" must match exactly.
#
#  threshold_analysis_def columns (from export__38_):
#    active, analysis_name, physical_metric, outcome_metric,
#    event_type, outcome_type ("binary"/"continuous"), min_events,
#    description
#
#  threshold_analysis_results columns (from export__37_):
#    analysis_def (link), skill_level, n_events, n_players, n_excluded,
#    physical_min/max/mean/p25/p75,
#    linear_r/p/sig,
#    breakpoint/ci_lo/ci_hi, slope_below/above, slope_change_p,
#    breakpoint_sig, outcome_below/above/change,
#    auc, roc_threshold/sensitivity/specificity,
#    quad_beta1/beta2/optimal_x/r2/shape,
#    calculated_at, notes
# ─────────────────────────────────────────────────────────────────


# Player column to use when filtering by event type
_PLAYER_COL = {
  'first_ball': 'att_player',
  'transition': 'att_player',
  'serve':      'serve_player',
}

# Outlier caps per physical metric (rows >= cap are dropped)
_OUTLIER_CAP = {
  'pass_height':      10.0,
  'dig_height':       10.0,
  'att_touch_height':  5.0,
  'dig_dur':          10.0,
  # speed metrics handled by > 0 filter instead
  'att_speed_mph':    None,
  'serve_speed_mph':  None,
}


# ─────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────
def _load_ppr_df(league, gender, year):
  try:
    from server_functions import get_ppr_data
    df = get_ppr_data(league, gender, year, 'League', scout=False)
    if isinstance(df, list):
      return None
    print(f"  Loaded PPR {league} {gender} {year}: {df.shape[0]} rows")
    return df
  except Exception as e:
    print(f"  PPR load error: {e}")
    return None


def _add_outcome_cols(df):
  """Add binary outcome columns. Called once per PPR load."""

  def in_team(player, team_str):
    return isinstance(player, str) and isinstance(team_str, str) and player in team_str

  df['att_kill'] = df.apply(
    lambda r: 1 if r['point_outcome'] == 'FBK' and
    in_team(r['att_player'], r['point_outcome_team']) else 0,
    axis=1
  )
  df['att_point_won'] = df.apply(
    lambda r: 1 if r['point_outcome'] in ('FBK', 'TK', 'TSA') and
    in_team(r['att_player'], r['point_outcome_team']) else 0,
    axis=1
  )
  df['serve_knockout'] = (
    (df.get('pass_oos', pd.Series(0, index=df.index)).fillna(0) > 0) |
    (df['point_outcome'] == 'TSA')
  ).astype(int)

  df['trans_point_won'] = df.apply(
    lambda r: 1 if r['point_outcome'] == 'TK' and
    in_team(r['att_player'], r['point_outcome_team']) else 0,
    axis=1
  )
  return df


def _get_player_name(player_row):
  """Return PPR-format name string from a master_player row."""
  try:
    if player_row.get('player_name'):
      return player_row['player_name']
    team  = player_row.get('team', '')
    num   = player_row.get('jersey_number', '')
    fname = player_row.get('first_name', '')
    if team and fname:
      return f"{team} {num} {fname}".strip()
  except Exception:
    pass
  return None


def _get_player_events(ppr_df, player_name, physical_col, outcome_col, event_type):
  """Return (x_array, y_array) for one player, or (None, None)."""
  try:
    pcol = _PLAYER_COL.get(event_type, 'att_player')

    if event_type == 'first_ball':
      mask = ((ppr_df[pcol] == player_name) &
              (ppr_df['pass_yn'] == 'Y') & (ppr_df['att_yn'] == 'Y'))
    elif event_type == 'transition':
      mask = ((ppr_df[pcol] == player_name) &
              (ppr_df['dig_yn'] == 'Y') & (ppr_df['att_yn'] == 'Y'))
    elif event_type == 'serve':
      mask = ((ppr_df[pcol] == player_name) &
              (ppr_df['serve_speed_mph'] > 0))
    else:
      return None, None

    df = ppr_df[mask][[physical_col, outcome_col]].copy().dropna()

    # Outlier cap
    cap = _OUTLIER_CAP.get(physical_col)
    if cap is not None:
      df = df[df[physical_col] < cap]

      # Drop zero-speed rows
    if physical_col in ('att_speed_mph', 'serve_speed_mph'):
      df = df[df[physical_col] > 0]

    if len(df) == 0:
      return None, None
    return df[physical_col].to_numpy(float), df[outcome_col].to_numpy(float)

  except Exception as e:
    print(f"  Event error for {player_name}: {e}")
    return None, None


# ─────────────────────────────────────────────────────────────────
#  ANALYSIS FUNCTIONS
# ─────────────────────────────────────────────────────────────────
def _linear_analysis(x, y):
  try:
    r, p = stats.pearsonr(x, y)
    return {'linear_r': round(float(r), 4),
            'linear_p': round(float(p), 4),
            'linear_sig': bool(p < 0.05)}
  except Exception as e:
    print(f"  Linear failed: {e}")
    return {'linear_r': None, 'linear_p': None, 'linear_sig': False}


def _segmented_regression(x, y, n_bootstrap=200, min_pct=0.15):
  result = {k: None for k in [
    'breakpoint','breakpoint_ci_lo','breakpoint_ci_hi',
    'slope_below','slope_above','slope_change_p','breakpoint_sig',
    'outcome_below','outcome_above','outcome_change']}
  result['breakpoint_sig'] = False
  n = len(x)
  if n < 50:
    return result
  try:
    x_lo = np.percentile(x, min_pct * 100)
    x_hi = np.percentile(x, (1 - min_pct) * 100)
    if x_hi <= x_lo:
      return result

    def rss(psi):
      h = np.maximum(0, x - psi)
      X = np.column_stack([np.ones(n), x, h])
      c, res, _, _ = np.linalg.lstsq(X, y, rcond=None)
      return float(res[0]) if len(res) else float(np.sum((y - X @ c) ** 2))

    grid = np.linspace(x_lo, x_hi, 100)
    rss_grid = [rss(g) for g in grid]
    bi = int(np.argmin(rss_grid))
    opt = minimize_scalar(rss,
                          bounds=(grid[max(0, bi-5)], grid[min(99, bi+5)]),
                          method='bounded')
    psi = float(opt.x if opt.success else grid[bi])

    h = np.maximum(0, x - psi)
    Xpw = np.column_stack([np.ones(n), x, h])
    c_pw, _, _, _ = np.linalg.lstsq(Xpw, y, rcond=None)
    b0, b1, b2 = float(c_pw[0]), float(c_pw[1]), float(c_pw[2])

    Xlin = np.column_stack([np.ones(n), x])
    c_lin, _, _, _ = np.linalg.lstsq(Xlin, y, rcond=None)
    rss_lin = float(np.sum((y - Xlin @ c_lin) ** 2))
    rss_seg = float(np.sum((y - Xpw  @ c_pw)  ** 2))
    df2 = n - 4
    p_chg = 1.0
    if rss_lin > rss_seg > 0 and df2 > 0:
      f = ((rss_lin - rss_seg) / 1) / (rss_seg / df2)
      p_chg = float(1 - stats.f.cdf(f, 1, df2))

    rng = np.random.default_rng(42)
    boots = []
    for _ in range(n_bootstrap):
      idx = rng.integers(0, n, n)
      xb, yb = x[idx], y[idx]
      lo_b = np.percentile(xb, min_pct * 100)
      hi_b = np.percentile(xb, (1 - min_pct) * 100)
      if hi_b <= lo_b:
        continue
      gb = np.linspace(lo_b, hi_b, 60)
      rb = []
      for g in gb:
        hb = np.maximum(0, xb - g)
        Xb = np.column_stack([np.ones(n), xb, hb])
        cb, resb, _, _ = np.linalg.lstsq(Xb, yb, rcond=None)
        rb.append(float(resb[0]) if len(resb) else float(np.sum((yb - Xb @ cb) ** 2)))
      boots.append(float(gb[int(np.argmin(rb))]))

    ci_lo = float(np.percentile(boots, 2.5))  if boots else psi
    ci_hi = float(np.percentile(boots, 97.5)) if boots else psi

    below = y[x <= psi]
    above = y[x >  psi]
    ob = float(np.mean(below)) if len(below) else None
    oa = float(np.mean(above)) if len(above) else None

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
    print(f"    Breakpoint {psi:.2f} [{ci_lo:.2f}–{ci_hi:.2f}] "
          f"p_change={p_chg:.4f} "
          + (f"outcome {ob:.3f}→{oa:.3f}" if ob is not None else ""))
  except Exception as e:
    print(f"  Segmented failed: {e}")
  return result


def _roc_analysis(x, y_binary):
  result = {'auc': None, 'roc_threshold': None,
            'roc_sensitivity': None, 'roc_specificity': None}
  try:
    y = y_binary.astype(int)
    n_pos, n_neg = int(y.sum()), int(len(y) - y.sum())
    if n_pos < 5 or n_neg < 5:
      return result

    order = np.argsort(-x)
    xs, ys = x[order], y[order]
        tprs, fprs, threshs = [0.0], [0.0], []
        tp = fp = 0
        for xi, yi in zip(xs, ys):
            if yi: tp += 1
            else:  fp += 1
            tprs.append(tp / n_pos)
            fprs.append(fp / n_neg)
            threshs.append(float(xi))

        tprs, fprs = np.array(tprs), np.array(fprs)
        auc = float(abs(np.trapz(tprs, fprs)))
        if auc < 0.5:
            auc = 1.0 - auc

        youden = tprs[1:] - fprs[1:]
        bi = int(np.argmax(youden))
        result.update({
            'auc':             round(auc, 4),
            'roc_threshold':   round(threshs[bi], 3),
            'roc_sensitivity': round(float(tprs[bi + 1]), 4),
            'roc_specificity': round(float(1 - fprs[bi + 1]), 4),
        })
        print(f"    ROC AUC={auc:.3f}  cut={threshs[bi]:.2f}  "
              f"sens={tprs[bi+1]:.3f} spec={1-fprs[bi+1]:.3f}")
    except Exception as e:
        print(f"  ROC failed: {e}")
    return result


def _quadratic_analysis(x, y):
    result = {'quad_beta1': None, 'quad_beta2': None,
              'quad_optimal_x': None, 'quad_r2': None, 'quad_shape': None}
    if len(x) < 50:
        return result
    try:
        xm, xs = float(np.mean(x)), float(np.std(x))
        if xs == 0:
            return result
        xn = (x - xm) / xs
        X  = np.column_stack([np.ones(len(x)), xn, xn ** 2])
        c, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        b0, b1, b2 = float(c[0]), float(c[1]), float(c[2])

        yh   = X @ c
        ss_r = float(np.sum((y - yh) ** 2))
        ss_t = float(np.sum((y - np.mean(y)) ** 2))
        r2   = float(1 - ss_r / ss_t) if ss_t > 0 else 0.0

        opt_x = float(-b1 / (2 * b2) * xs + xm) if abs(b2) > 1e-10 else None
        shape = ('linear' if abs(b2) < 0.1 * (abs(b1) + 1e-10)
                 else 'inverted_U' if b2 < 0 else 'U_shape')

        result.update({
            'quad_beta1':     round(b1, 6),
            'quad_beta2':     round(b2, 6),
            'quad_optimal_x': round(opt_x, 3) if opt_x is not None else None,
            'quad_r2':        round(r2, 4),
            'quad_shape':     shape,
        })
        print(f"    Quad shape={shape}  R²={r2:.3f}"
              + (f"  optimal={opt_x:.2f}" if opt_x else ""))
    except Exception as e:
        print(f"  Quadratic failed: {e}")
    return result


# ─────────────────────────────────────────────────────────────────
#  MAIN CALCULATION
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def calc_threshold_analysis():
    """
    For each active row in threshold_analysis_def and each skill level,
    pool raw PPR rally events across all players and run four analyses:
    linear baseline, segmented regression, ROC (binary), quadratic.

    Player matching uses master_player['player_name'] which must match
    the PPR name strings exactly (e.g. "STETSON 22 Zoe").

    Deletes all existing results before recalculating.
    Returns a summary string.
    """
    print("=== calc_threshold_analysis: starting ===")

    defs = [r for r in app_tables.threshold_analysis_def.search() if r['active']]
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
        outcome_type = ad['outcome_type']
        min_events   = ad['min_events'] or 30
        name         = ad['analysis_name']
        print(f"\n=== {name} ({physical_col}→{outcome_col}, {event_type}) ===")

        for sl in skill_levels:
            level_name  = sl['level_name']
            player_list = sl['player_list']

            if not player_list or len(player_list) < 3:
                print(f"  Skipping {level_name}: <3 players")
                skipped += 1
                continue

            all_x = []
            all_y = []
            n_inc = n_exc = 0
            cur_lgy = None
            cur_ppr = None

            for pr in player_list:
                try:
                    league       = pr['league']
                    gender       = pr['gender']
                    year         = pr['year']
                    player_name  = _get_player_name(pr)
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

                xv, yv = _get_player_events(cur_ppr, player_name,
                                             physical_col, outcome_col, event_type)
                if xv is None or len(xv) < min_events:
                    n_exc += 1
                    continue

                all_x.extend(xv.tolist())
                all_y.extend(yv.tolist())
                n_inc += 1

            n_events = len(all_x)
            print(f"  {level_name}: {n_inc} players, {n_exc} excl, {n_events} events")

            min_pooled = max(min_events * 3, 100)
            if n_events < min_pooled:
                print(f"  Skipping: {n_events} events (need {min_pooled})")
                skipped += 1
                continue

            xa = np.array(all_x, float)
            ya = np.array(all_y, float)

            desc   = {
                'physical_min':  round(float(np.min(xa)), 4),
                'physical_max':  round(float(np.max(xa)), 4),
                'physical_mean': round(float(np.mean(xa)), 4),
                'physical_p25':  round(float(np.percentile(xa, 25)), 4),
                'physical_p75':  round(float(np.percentile(xa, 75)), 4),
            }
            lin  = _linear_analysis(xa, ya)
            seg  = _segmented_regression(xa, ya)
            roc  = (_roc_analysis(xa, ya)
                    if outcome_type == 'binary'
                    else {'auc': None, 'roc_threshold': None,
                          'roc_sensitivity': None, 'roc_specificity': None})
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
#  BACKGROUND TASK + LAUNCHER  (same pattern as calc_metric_correlations)
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
            return {'done': True,  'status': state.get('status', 'complete'),
                    'message': state.get('message', 'Done')}
        elif task.get_error():
            return {'done': True,  'status': 'error',
                    'message': str(task.get_error())}
        else:
            return {'done': False, 'status': 'running', 'message': 'Calculating...'}
    except Exception as e:
        return {'done': True, 'status': 'error', 'message': str(e)}


# ─────────────────────────────────────────────────────────────────
#  FETCH RESULTS FOR DISPLAY
# ─────────────────────────────────────────────────────────────────
@anvil.server.callable
def get_threshold_analysis_results(analysis_name=None, skill_level=None):
    """
    Returns all results as list of dicts.
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