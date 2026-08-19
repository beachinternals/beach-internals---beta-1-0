import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import json
import re

from logger_utils import log_info, log_error

# ============================================================================
#
#  Weekly Data Quality Report
#
#  Two email kinds, built from btd_files rows loaded (btd_file_date) in a
#  Tue 6:00am -> following Tue 6:00am window:
#    1. One internal summary, all teams at a glance.
#    2. One per-team detail email, with a per-error breakdown and a video
#       link for every remaining error in that team's non-clean files.
#
#  SENDING STATUS (see trigger_weekly_data_quality_report at the bottom):
#    - The internal summary email to the system administrator
#      (info@beachinternals.com) is sent for real.
#    - Per-team emails are NOT sent yet -- SEND_TEAM_EMAILS stays False
#      until that's explicitly turned on. build_weekly_report() still
#      builds the per-team content so it can be reviewed/previewed.
#
# ============================================================================

SEND_TEAM_EMAILS = False
ADMIN_EMAIL = "info@beachinternals.com"

# Matches the exact lines error_check_ppr() (btd_ppr_conversion.py) writes
# into error_str, e.g.:
#   "|- Pass and  Set Same Player       -| a,b Point Number:12"
#   "|- Unmatched Player (pass_player)               -| Point Number:7"
# Deliberately loose on internal whitespace since error_check_ppr's own
# padding is inconsistent between error types.
_ERROR_LINE_RE = re.compile(r"\|-\s*(.+?)\s*-\|(.*?)Point Number:\s*(\d+)")


def _require_internals():
  """Verify the caller is logged in AND is on the INTERNALS team."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS':
    raise Exception("Access denied: this function is for admins only.")
  return user


def get_report_window(reference_time=None):
  """
  Returns (start, end) for the most recently completed Tuesday-6am-to-
  Tuesday-6am window, ending at or before reference_time (defaults to now).
  """
  now = reference_time or datetime.now()
  days_since_tuesday = (now.weekday() - 1) % 7  # Monday=0 ... Tuesday=1
  this_tuesday = (now - timedelta(days=days_since_tuesday)).replace(
    hour=6, minute=0, second=0, microsecond=0)
  if this_tuesday > now:
    this_tuesday -= timedelta(days=7)
  end = this_tuesday
  start = end - timedelta(days=7)
  return start, end


def parse_error_str(error_str):
  """
  Extracts {error_type, detail, point_no} per line from a btd_files.error_str
  value, using the exact format error_check_ppr() (btd_ppr_conversion.py)
  produces. Skips the trailing "Total Errors Found:N" line, which doesn't
  match the "|- ... -|" shape.
  """
  if not error_str:
    return []
  lines = []
  for match in _ERROR_LINE_RE.finditer(error_str):
    error_type, detail, point_no = match.groups()
    lines.append({
      'error_type': error_type.strip(),
      'detail': detail.strip(' ,'),
      'point_no': int(point_no),
    })
  return lines


def parse_corrections_json(corrections_json):
  if not corrections_json:
    return []
  try:
    return json.loads(corrections_json)
  except (ValueError, TypeError):
    return []


def find_video_link(point_no, corrections, video_id):
  """
  Priority order:
  1. A corrections_json entry for this point_no with a video_link (some
     already carry ?actionIds=... for a precise jump point).
  2. A base match link built from video_id.
  3. None -- caller renders "no video available".
  """
  for c in corrections:
    if c.get('point_no') == point_no and c.get('video_link'):
      return c['video_link']
  if video_id and video_id != 'No Video Id':
    return f"https://app.balltime.com/video/{video_id}"
  return None


def get_team_contact_email(team):
  """
  Looks up the per-team contact from teams.admin_email (keyed by
  teams.team). NOTE: as of this writing nothing else in the app reads or
  writes teams.admin_email, so it may be unpopulated for some/all teams --
  callers should treat a missing address as a data-entry gap to flag, not
  assume it's actionable yet.
  """
  team_row = app_tables.teams.get(team=team)
  if team_row and team_row['admin_email']:
    return team_row['admin_email']
  return None


def build_team_detail(team, files):
  """
  Per-file, per-error breakdown (with video links) for one team's files
  loaded in the report window.
  """
  detail = []
  for f in files:
    if not f['no_errors']:
      detail.append({'filename': f['filename'], 'clean': True, 'errors': []})
      continue
    corrections = parse_corrections_json(f['corrections_json'])
    error_lines = parse_error_str(f['error_str'])
    errors_with_links = []
    for line in error_lines:
      link = find_video_link(line['point_no'], corrections, f['video_id'])
      errors_with_links.append({**line, 'video_link': link})
    detail.append({'filename': f['filename'], 'clean': False, 'errors': errors_with_links})
  return detail


def build_weekly_report(reference_time=None):
  """
  Preview-only: builds and returns the report content for the most recently
  completed Tue 6am -> Tue 6am window. Does not send any email.

  Returns a dict:
    {
      'start': datetime, 'end': datetime,
      'summary_rows': [{'team', 'n_files', 'n_clean', 'pct_clean', 'total_errors'}, ...],
      'totals': {'n_files', 'n_clean', 'pct_clean', 'total_errors'},
      'team_reports': {team: {'contact_email': str|None, 'detail': [...]}, ...},
    }
  """
  start, end = get_report_window(reference_time)

  files_this_week = list(app_tables.btd_files.search(
    btd_file_date=q.all_of(q.greater_than_or_equal_to(start), q.less_than(end))
  ))

  by_team = {}
  for r in files_this_week:
    by_team.setdefault(r['team'], []).append(r)

  summary_rows = []
  team_reports = {}

  for team, files in sorted(by_team.items(), key=lambda kv: kv[0] or ''):
    n = len(files)
    n_clean = sum(1 for f in files if not f['no_errors'])
    total_errors = sum(f['no_errors'] or 0 for f in files)
    summary_rows.append({
      'team': team,
      'n_files': n,
      'n_clean': n_clean,
      'pct_clean': round(n_clean / n * 100, 1) if n else 0,
      'total_errors': total_errors,
    })
    team_reports[team] = {
      'contact_email': get_team_contact_email(team),
      'detail': build_team_detail(team, files),
    }

  total_n_files = sum(row['n_files'] for row in summary_rows)
  total_n_clean = sum(row['n_clean'] for row in summary_rows)
  totals = {
    'n_files': total_n_files,
    'n_clean': total_n_clean,
    'total_errors': sum(row['total_errors'] for row in summary_rows),
    'pct_clean': round(total_n_clean / total_n_files * 100, 1) if total_n_files else 0,
  }

  return {
    'start': start,
    'end': end,
    'summary_rows': summary_rows,
    'totals': totals,
    'team_reports': team_reports,
  }


def render_internal_summary_html(report):
  start, end = report['start'], report['end']
  rows_html = "".join(
    f"<tr><td>{row['team'] or 'None'}</td><td>{row['n_files']}</td>"
    f"<td>{row['pct_clean']}%</td><td>{row['total_errors']}</td></tr>"
    for row in report['summary_rows']
  )
  totals = report['totals']
  totals_html = (
    f"<tr><td><b>Total</b></td><td><b>{totals['n_files']}</b></td>"
    f"<td><b>{totals['pct_clean']}%</b></td><td><b>{totals['total_errors']}</b></td></tr>"
  )
  return (
    f"<h2>Weekly Data Quality Report</h2>"
    f"<p>Files loaded {start.strftime('%Y-%m-%d %H:%M')} &ndash; {end.strftime('%Y-%m-%d %H:%M')}</p>"
    f"<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>"
    f"<tr><th>Team</th><th>Files loaded</th><th>% error-free</th><th>Total remaining errors</th></tr>"
    f"{rows_html}{totals_html}"
    f"</table>"
  )


def build_error_buckets(detail):
  """
  Buckets each file by its remaining error count: clean (0), 1-5, 6-10,
  11-20, 21+. Uses len(f['errors']) (the parsed error lines actually
  listed below) rather than the raw no_errors column, so the summary
  table always agrees with the per-file breakdown under it.
  """
  buckets = {'clean': 0, 'le_5': 0, 'g6_10': 0, 'g11_20': 0, 'over_20': 0}
  for f in detail:
    n = len(f['errors'])
    if f['clean'] or n == 0:
      buckets['clean'] += 1
    elif n <= 5:
      buckets['le_5'] += 1
    elif n <= 10:
      buckets['g6_10'] += 1
    elif n <= 20:
      buckets['g11_20'] += 1
    else:
      buckets['over_20'] += 1
  return buckets


def render_error_bucket_summary_html(detail):
  buckets = build_error_buckets(detail)
  return (
    "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>"
    "<tr><th>Total files</th><th>Clean</th><th>1&ndash;5 errors</th>"
    "<th>6&ndash;10 errors</th><th>11&ndash;20 errors</th><th>21+ errors</th></tr>"
    f"<tr><td>{len(detail)}</td><td>{buckets['clean']}</td><td>{buckets['le_5']}</td>"
    f"<td>{buckets['g6_10']}</td><td>{buckets['g11_20']}</td><td>{buckets['over_20']}</td></tr>"
    "</table>"
  )


def render_team_detail_html(team, detail, subtitle):
  """
  Shared per-file/per-error HTML body for a team, used by both the weekly
  windowed report and the ad hoc league/gender/year+team report below.
  Leads with an error-count summary table, then the full per-file/per-error
  breakdown.
  """
  files_html = []
  for f in detail:
    if f['clean']:
      files_html.append(f"<h3>{f['filename']} &mdash; clean</h3>")
      continue
    error_items = []
    for e in f['errors']:
      if e['video_link']:
        link_html = f"<a href='{e['video_link']}'>video</a>"
      else:
        link_html = "no video available for this match"
      detail_str = f" ({e['detail']})" if e['detail'] else ""
      error_items.append(
        f"<li>{e['error_type']}{detail_str} &mdash; Point {e['point_no']} &mdash; {link_html}</li>"
      )
    files_html.append(
      f"<h3>{f['filename']} &mdash; {len(f['errors'])} error(s)</h3><ul>{''.join(error_items)}</ul>"
    )

  return (
    f"<h2>Data Quality Report &mdash; {team or 'None'}</h2>"
    f"<p>{subtitle}</p>"
    f"{render_error_bucket_summary_html(detail)}"
    f"{''.join(files_html)}"
  )


def render_team_email_html(team, report):
  start, end = report['start'], report['end']
  detail = report['team_reports'][team]['detail']
  subtitle = f"Files loaded {start.strftime('%Y-%m-%d %H:%M')} &ndash; {end.strftime('%Y-%m-%d %H:%M')}"
  return render_team_detail_html(team, detail, subtitle)


def build_team_quality_report(league, gender, year, team):
  """
  Ad hoc, on-demand version of the per-team report: every btd_files row
  matching this league/gender/year/team, with no date-window restriction
  (unlike build_weekly_report, which only looks at the current Tue-Tue
  window). Useful for reviewing a specific team's data quality at any time.
  """
  files = list(app_tables.btd_files.search(
    league=league, gender=gender, year=str(year), team=team
  ))
  n = len(files)
  n_clean = sum(1 for f in files if not f['no_errors'])
  return {
    'league': league,
    'gender': gender,
    'year': year,
    'team': team,
    'n_files': n,
    'n_clean': n_clean,
    'pct_clean': round(n_clean / n * 100, 1) if n else 0,
    'total_errors': sum(f['no_errors'] or 0 for f in files),
    'detail': build_team_detail(team, files),
  }


def _team_quality_report_subtitle(league, gender, year):
  return f"League {league} | Gender {gender} | Year {year} &mdash; all files on file (no date filter)"


@anvil.server.callable
def preview_team_quality_report(league, gender, year, team):
  """
  INTERNALS only. Same per-file/per-error breakdown and video links as the
  weekly per-team email, but for a caller-chosen league/gender/year/team
  and ALL files on file for that combination (not just the current week).
  Returns the summary counts plus rendered HTML; does not send any email.
  """
  _require_internals()
  report = build_team_quality_report(league, gender, year, team)
  return {
    **report,
    'html': render_team_detail_html(team, report['detail'], _team_quality_report_subtitle(league, gender, year)),
  }


@anvil.server.callable
def trigger_team_quality_report(league, gender, year, team):
  """INTERNALS only. Launches the background task that emails this report."""
  _require_internals()
  anvil.server.launch_background_task('send_team_quality_report', league, gender, year, team)
  return {"status": f"Team data quality report triggered for {team} ({league} {gender} {year})"}


@anvil.server.background_task
def send_team_quality_report(league, gender, year, team):
  """
  Builds the ad hoc league/gender/year+team report (all files on file, no
  date window) and emails it to the system administrator.
  """
  try:
    report = build_team_quality_report(league, gender, year, team)
    html = render_team_detail_html(team, report['detail'], _team_quality_report_subtitle(league, gender, year))

    anvil.email.send(
      to=ADMIN_EMAIL,
      from_address="no-reply",
      subject=f"Data Quality Report - {team} - {league} {gender} {year}",
      html=html
    )
    log_info(f"Team data quality report sent to {ADMIN_EMAIL} for {team} ({league} {gender} {year})")

  except Exception as e:
    log_error(f"Error in send_team_quality_report: {str(e)}")
    anvil.email.send(
      to=ADMIN_EMAIL,
      from_address="no-reply",
      subject="Team Data Quality Report Error",
      html=f"<h2>Error building/sending the Team Data Quality Report</h2><p>League {league} | Gender {gender} | Year {year} | Team {team}</p><p>{str(e)}</p>"
    )


# ============================================================================
#
#  Data Corrections Debug Report
#
#  Ad hoc, on-demand email (same league/gender/year/team shape as the team
#  quality report above) that surfaces what correct_pass_attribution(),
#  correct_serve_pass_same_team(), correct_missing_touches() and
#  resolve_serve_players() (see pass_attribution_correction.py and
#  btd_ppr_conversion.py) actually did with each btd_files.corrections_json
#  entry -- corrected, flagged for manual review, or left alone -- with a
#  video link per entry so a human can go watch the point and look for new
#  correction rules. This is a debugging aid for those routines, not a data
#  quality signal for teams.
#
# ============================================================================

# Correction entries carry a 'classification' dict, but only some of the
# correction functions stamp an explicit classification['error_type'] (see
# correct_serve_pass_same_team, correct_missing_touches and
# resolve_serve_players). correct_pass_attribution's entries don't -- they're
# the pass/set/att mis-attribution case identified by _touch_players/
# classify_pass_player, so that's the label used when error_type is absent.
_DEFAULT_CORRECTION_ERROR_TYPE = 'pass_set_att_attribution'

_STATUS_LABELS = {
  'corrected': 'CORRECTED',
  'flagged': 'FLAGGED',
  'no_change_needed': 'NO CHANGE NEEDED',
}
_STATUS_COLORS = {
  'corrected': '#2e7d32',
  'flagged': '#b00020',
  'no_change_needed': '#757575',
}


def get_correction_video_link(entry, file_video_id):
  """
  Priority order, same idea as find_video_link() above:
  1. The entry's own video_link (some correction functions already build a
     precise ?actionIds=... link).
  2. A base match link built from the entry's own video_id, or else the
     file's video_id (resolve_serve_players entries carry neither).
  3. None -- caller renders "no video available".
  """
  if entry.get('video_link'):
    return entry['video_link']
  video_id = entry.get('video_id') or file_video_id
  if video_id and video_id != 'No Video Id':
    return f"https://app.balltime.com/video/{video_id}"
  return None


def build_corrections_detail(files):
  """
  Per-file, per-correction-entry breakdown (with video links) for one
  team's files, from btd_files.corrections_json.
  """
  detail = []
  for f in files:
    corrections = parse_corrections_json(f['corrections_json'])
    if not corrections:
      detail.append({'filename': f['filename'], 'clean': True, 'corrections': []})
      continue
    entries = []
    for c in corrections:
      classification = c.get('classification') or {}
      entries.append({
        'point_id': c.get('point_no', c.get('rally_id')),
        'status': c.get('status', 'unknown'),
        'error_type': classification.get('error_type', _DEFAULT_CORRECTION_ERROR_TYPE),
        'reason': classification.get('reason'),
        # Whatever classify_pass_player()/correct_missing_touches() etc left
        # in classification besides error_type/reason -- e.g. candidates,
        # z_distance, confidence_ratio, sample_sizes (pass/set/att
        # attribution) or notes (missing/unmatched touch, one line per
        # field derivation). Kept generic and rendered as-is so newly added
        # classification detail shows up here without more code changes.
        'classification_extra': {
          k: v for k, v in classification.items() if k not in ('error_type', 'reason')
        },
        'before': c.get('before') or {},
        'changes': c.get('changes') or [],
        'video_link': get_correction_video_link(c, f['video_id']),
      })
    detail.append({'filename': f['filename'], 'clean': False, 'corrections': entries})
  return detail


def build_team_corrections_report(league, gender, year, team):
  """
  Ad hoc, on-demand corrections debug report: every btd_files row matching
  this league/gender/year/team, with no date-window restriction. Mirrors
  build_team_quality_report() above but surfaces corrections_json instead
  of error_str.
  """
  files = list(app_tables.btd_files.search(
    league=league, gender=gender, year=str(year), team=team
  ))
  detail = build_corrections_detail(files)
  total_entries = sum(len(f['corrections']) for f in detail)
  return {
    'league': league,
    'gender': gender,
    'year': year,
    'team': team,
    'n_files': len(files),
    'n_files_with_corrections': sum(1 for f in detail if not f['clean']),
    'total_entries': total_entries,
    'detail': detail,
  }


def _team_corrections_report_subtitle(league, gender, year):
  return f"League {league} | Gender {gender} | Year {year} &mdash; all files on file (no date filter)"


def _format_classification_value(value):
  if isinstance(value, float):
    return str(round(value, 3))
  if isinstance(value, dict):
    return "{" + ", ".join(f"{k}={_format_classification_value(v)}" for k, v in value.items()) + "}"
  if isinstance(value, list):
    return "[" + ", ".join(_format_classification_value(v) for v in value) + "]"
  return str(value)


def render_correction_entry_html(entry):
  status = entry['status']
  status_label = _STATUS_LABELS.get(status, status.upper())
  status_color = _STATUS_COLORS.get(status, '#333')
  point_label = f"Point {entry['point_id']}" if entry['point_id'] is not None else "Point ?"
  link_html = f"<a href='{entry['video_link']}'>video</a>" if entry['video_link'] else "no video available for this match"
  reason_html = f" &mdash; {entry['reason']}" if entry['reason'] else ""

  extra = []
  if entry['before']:
    before_str = ", ".join(f"{k}={v}" for k, v in entry['before'].items())
    extra.append(f"Before: {before_str}")

  # notes (correct_missing_touches) is a list of one reasoning line per
  # field it derived/flagged -- surface those as their own line rather than
  # folding them into the generic classification-detail line below.
  classification_extra = dict(entry['classification_extra'])
  notes = classification_extra.pop('notes', None)
  if classification_extra:
    details_str = ", ".join(f"{k}={_format_classification_value(v)}" for k, v in classification_extra.items())
    extra.append(f"Details: {details_str}")
  if notes:
    extra.append(f"Notes: {'; '.join(str(n) for n in notes)}")

  if entry['changes']:
    changes_str = "; ".join(f"{c[0]}: {c[1]} &rarr; {c[2]}" for c in entry['changes'])
    extra.append(f"Changes: {changes_str}")
  extra_html = f"<br><small>{' &nbsp;|&nbsp; '.join(extra)}</small>" if extra else ""

  return (
    f"<li><span style='color:{status_color};font-weight:bold'>{status_label}</span> "
    f"&mdash; {entry['error_type']}{reason_html} &mdash; {point_label} &mdash; {link_html}"
    f"{extra_html}</li>"
  )


def build_corrections_breakdown(detail):
  """
  status totals, plus a per-error_type x status count grid -- the error
  types with the most FLAGGED entries are the best candidates for new
  correction rules.
  """
  status_totals = Counter()
  by_error_type = defaultdict(Counter)
  for f in detail:
    for c in f['corrections']:
      status_totals[c['status']] += 1
      by_error_type[c['error_type']][c['status']] += 1
  return status_totals, by_error_type


def render_corrections_summary_html(detail):
  status_totals, by_error_type = build_corrections_breakdown(detail)
  total = sum(status_totals.values())

  status_rows = "".join(
    f"<tr><td>{_STATUS_LABELS.get(status, status.upper())}</td><td>{status_totals.get(status, 0)}</td></tr>"
    for status in ('flagged', 'corrected', 'no_change_needed')
  )

  error_type_rows = "".join(
    f"<tr><td>{error_type}</td><td>{counts.get('flagged', 0)}</td>"
    f"<td>{counts.get('corrected', 0)}</td><td>{counts.get('no_change_needed', 0)}</td>"
    f"<td>{sum(counts.values())}</td></tr>"
    for error_type, counts in sorted(
      by_error_type.items(), key=lambda kv: kv[1].get('flagged', 0), reverse=True
    )
  )

  return (
    "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>"
    "<tr><th>Status</th><th>Count</th></tr>"
    f"{status_rows}"
    f"<tr><td><b>Total</b></td><td><b>{total}</b></td></tr>"
    "</table>"
    "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;margin-top:12px'>"
    "<tr><th>Error type</th><th>Flagged</th><th>Corrected</th><th>No change needed</th><th>Total</th></tr>"
    f"{error_type_rows}"
    "</table>"
  )


def render_team_corrections_html(team, detail, subtitle):
  files_html = []
  for f in detail:
    if f['clean']:
      files_html.append(f"<h3>{f['filename']} &mdash; no corrections needed</h3>")
      continue
    entries_html = "".join(render_correction_entry_html(e) for e in f['corrections'])
    files_html.append(
      f"<h3>{f['filename']} &mdash; {len(f['corrections'])} correction entr"
      f"{'y' if len(f['corrections']) == 1 else 'ies'}</h3><ul>{entries_html}</ul>"
    )

  return (
    f"<h2>Data Corrections Debug Report &mdash; {team or 'None'}</h2>"
    f"<p>{subtitle}</p>"
    f"{render_corrections_summary_html(detail)}"
    f"{''.join(files_html)}"
  )


@anvil.server.callable
def preview_team_corrections_report(league, gender, year, team):
  """
  INTERNALS only. Same per-file/per-correction breakdown and video links as
  the emailed version, but returns the rendered HTML without sending
  anything, so it can be reviewed first.
  """
  _require_internals()
  report = build_team_corrections_report(league, gender, year, team)
  return {
    **report,
    'html': render_team_corrections_html(team, report['detail'], _team_corrections_report_subtitle(league, gender, year)),
  }


@anvil.server.callable
def trigger_team_corrections_report(league, gender, year, team):
  """INTERNALS only. Launches the background task that emails this report."""
  _require_internals()
  anvil.server.launch_background_task('send_team_corrections_report', league, gender, year, team)
  return {"status": f"Data corrections debug report triggered for {team} ({league} {gender} {year})"}


@anvil.server.background_task
def send_team_corrections_report(league, gender, year, team):
  """
  Builds the ad hoc league/gender/year+team corrections debug report (all
  files on file, no date window) and emails it to the system administrator.
  """
  try:
    report = build_team_corrections_report(league, gender, year, team)
    html = render_team_corrections_html(team, report['detail'], _team_corrections_report_subtitle(league, gender, year))

    anvil.email.send(
      to=ADMIN_EMAIL,
      from_address="no-reply",
      subject=f"Data Corrections Debug Report - {team} - {league} {gender} {year}",
      html=html
    )
    log_info(f"Data corrections debug report sent to {ADMIN_EMAIL} for {team} ({league} {gender} {year})")

  except Exception as e:
    log_error(f"Error in send_team_corrections_report: {str(e)}")
    anvil.email.send(
      to=ADMIN_EMAIL,
      from_address="no-reply",
      subject="Data Corrections Debug Report Error",
      html=f"<h2>Error building/sending the Data Corrections Debug Report</h2><p>League {league} | Gender {gender} | Year {year} | Team {team}</p><p>{str(e)}</p>"
    )


@anvil.server.callable
def preview_weekly_data_quality_report(reference_time=None):
  """
  INTERNALS only. Builds this week's report and returns the rendered HTML
  for every email (internal summary + each per-team email) WITHOUT sending
  anything, so the content/formatting can be reviewed first.
  """
  _require_internals()
  report = build_weekly_report(reference_time)
  return {
    'start': report['start'],
    'end': report['end'],
    'internal_summary_html': render_internal_summary_html(report),
    'team_emails': {
      team: {
        'contact_email': team_report['contact_email'],
        'html': render_team_email_html(team, report),
      }
      for team, team_report in report['team_reports'].items()
    },
  }


@anvil.server.callable
def trigger_weekly_data_quality_report():
  """INTERNALS only. Launches the background task that sends the report."""
  _require_internals()
  anvil.server.launch_background_task('send_weekly_data_quality_report')
  return {"status": "Weekly data quality report triggered"}


@anvil.server.background_task
def send_weekly_data_quality_report():
  """
  Builds the weekly report and emails the internal summary to the system
  administrator. Per-team emails are built but NOT sent while
  SEND_TEAM_EMAILS is False -- see the module docstring.
  """
  try:
    report = build_weekly_report()

    anvil.email.send(
      to=ADMIN_EMAIL,
      from_address="no-reply",
      subject=f"Weekly Data Quality Report - {report['end'].strftime('%Y-%m-%d')}",
      html=render_internal_summary_html(report)
    )
    log_info("Weekly data quality report: internal summary sent to %s" % ADMIN_EMAIL)

    missing_contacts = [
      team for team, team_report in report['team_reports'].items()
      if not team_report['contact_email']
    ]
    if missing_contacts:
      log_info(
        "Weekly data quality report: no teams.admin_email on file for: %s"
        % ", ".join(str(t) for t in missing_contacts)
      )

    if SEND_TEAM_EMAILS:
      for team, team_report in report['team_reports'].items():
        contact_email = team_report['contact_email']
        if not contact_email:
          continue
        anvil.email.send(
          to=contact_email,
          from_address="no-reply",
          subject=f"Weekly Data Quality Report - {team} - {report['end'].strftime('%Y-%m-%d')}",
          html=render_team_email_html(team, report)
        )
      log_info("Weekly data quality report: per-team emails sent")
    else:
      log_info("Weekly data quality report: per-team emails NOT sent (SEND_TEAM_EMAILS is False)")

  except Exception as e:
    log_error(f"Error in send_weekly_data_quality_report: {str(e)}")
    anvil.email.send(
      to=ADMIN_EMAIL,
      from_address="no-reply",
      subject="Weekly Data Quality Report Error",
      html=f"<h2>Error building/sending the Weekly Data Quality Report</h2><p>{str(e)}</p>"
    )
