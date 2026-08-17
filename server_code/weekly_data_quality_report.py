import anvil.email
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta
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
  if video_id and video_id != 'empty':
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


def render_team_email_html(team, report):
  start, end = report['start'], report['end']
  detail = report['team_reports'][team]['detail']

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
    f"<h2>Weekly Data Quality Report &mdash; {team or 'None'}</h2>"
    f"<p>Files loaded {start.strftime('%Y-%m-%d %H:%M')} &ndash; {end.strftime('%Y-%m-%d %H:%M')}</p>"
    f"{''.join(files_html)}"
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
