import anvil.server
from anvil.tables import app_tables
from anvil import BlobMedia
import csv
import io
import pandas as pd

from logger_utils import log_info, log_error, log_debug, log_critical

def sanitize_quotes(text):
  """Replace ALL types of Unicode quotes with regular ASCII quotes."""
  # Check for None or empty
  if text is None or text == '' or text == 'nan':
    return ''

  text = str(text)

  # Replace using Unicode code points (more reliable than copying characters)
  # Single quotes
  text = text.replace('\u2018', "'").replace('\u2019', "'")  # Left/right single quote
  text = text.replace('\u201a', "'").replace('\u201b', "'")  # Low/high single quote

  # Double quotes
  text = text.replace('\u201c', '"').replace('\u201d', '"')  # Left/right double quote
  text = text.replace('\u201e', '"').replace('\u201f', '"')  # Low/high double quote

  # Other quote-like characters
  text = text.replace('\u2039', "'").replace('\u203a', "'")  # Angle quotes
  text = text.replace('`', "'").replace('\u00b4', "'")       # Backtick/acute

  return text


@anvil.server.callable
def import_metric_dictionary_from_csv(csv_file):
  """Import metric dictionary from an uploaded CSV file."""

  imported_count = 0
  errors = []

  try:
    # Read the CSV file
    log_info("Reading CSV file...")
    csv_bytes = csv_file.get_bytes()
    csv_content = csv_bytes.decode('utf-8')

    # DEBUG: Show first 500 characters
    log_debug(f"CSV content preview: {csv_content[:500]}")

    csv_reader = csv.DictReader(io.StringIO(csv_content))

    log_info("Starting import...")

    # DEBUG: Check if reader has any rows
    rows_list = list(csv_reader)
    log_info(f"Found {len(rows_list)} rows in CSV")

    if len(rows_list) == 0:
      log_error("No rows found in CSV!", with_traceback=False)
      return {
        'success': False,
        'imported': 0,
        'errors': ['No rows found in CSV file']
      }

    # DEBUG: Show first row
    if len(rows_list) > 0:
      log_debug(f"First row keys: {list(rows_list[0].keys())}")
      log_debug(f"First row sample: {list(rows_list[0].items())[:5]}")

    # Import each row
    for row_num, row in enumerate(rows_list, start=2):
      try:
        # Convert empty strings to None
        def to_number(value):
          if value == '' or value is None or value == 'nan':
            return None
          try:
            return float(value)
          except:
            return None

        # DEBUG: Show what we're trying to import
        if row_num == 2:  # First data row
          log_info(f"Importing first row: metric_id={row.get('metric_id')}, name={row.get('metric_name')}")

        # Sanitize critical fields that contain code/formulas
        function_name_clean       = sanitize_quotes(row.get('function_name', ''))
        result_path_clean         = sanitize_quotes(row.get('result_path', ''))
        attempts_path_clean       = sanitize_quotes(row.get('attempts_path', ''))
        data_filter_clean         = sanitize_quotes(row.get('data_filter', ''))
        calculation_formula_clean = sanitize_quotes(row.get('calculation_formula', ''))
        video_path_clean          = sanitize_quotes(row.get('video_path', ''))

        # Add row to table
        app_tables.metric_dictionary.add_row(
          metric_id=row.get('metric_id', ''),
          coach_alias=row.get('coach_alias', ''),          # ← NEW COLUMN
          metric_name=row.get('metric_name', ''),
          metric_type=row.get('metric_type', ''),
          metric_role=row.get('metric_role', ''),
          parent_metric=row.get('parent_metric', ''),
          metric_category=row.get('metric_category', ''),
          min_attempts_for_ci=to_number(row.get('min_attempts_for_ci')),
          ci_calculation_method=row.get('ci_calculation_method', ''),
          data_filter=data_filter_clean,
          return_type=row.get('return_type', ''),
          result_path=result_path_clean,
          attempts_path=attempts_path_clean,
          function_name=function_name_clean,
          calculation_formula=calculation_formula_clean,
          data_range_min=to_number(row.get('data_range_min')),
          data_range_max=to_number(row.get('data_range_max')),
          coach_speak_elite=row.get('coach_speak_elite', ''),
          coach_speak_good=row.get('coach_speak_good', ''),
          coach_speak_average=row.get('coach_speak_average', ''),
          coach_speak_poor=row.get('coach_speak_poor', ''),
          coach_view=row.get('coach_view', ''),
          video_path=video_path_clean,
          drill_down_report=row.get('drill_down_report', '')
        )

        imported_count += 1

        if imported_count % 50 == 0:
          log_info(f"Progress: Imported {imported_count} rows...")

      except Exception as e:
        error_msg = f"Row {row_num} ({row.get('metric_id', 'unknown')}): {str(e)}"
        errors.append(error_msg)
        log_error(f"Row import failed: {error_msg}", with_traceback=True)
        continue

    log_info(f"✓ Import complete! Imported {imported_count} rows.")
    if errors:
      log_info(f"Encountered {len(errors)} errors during import.")

    return {
      'success': True,
      'imported': imported_count,
      'errors': errors
    }

  except Exception as e:
    log_error(f"CSV import failed: {str(e)}", with_traceback=True)
    return {
      'success': False,
      'imported': imported_count,
      'errors': [f"Critical error: {str(e)}"]
    }


@anvil.server.callable
def cleanup_metric_dictionary_quotes():
  """One-time cleanup to fix Unicode quotes in existing data."""

  def sanitize_quotes(text):
    """Replace ALL types of Unicode quotes with regular ASCII quotes."""
    if text is None or text == '' or text == 'nan':
      return text

    text = str(text)

    # Single quotes
    text = text.replace('\u2018', "'")  # Left single quote
    text = text.replace('\u2019', "'")  # Right single quote
    text = text.replace('\u201a', "'")  # Single low quote
    text = text.replace('\u201b', "'")  # Single high-reversed quote

    # Double quotes
    text = text.replace('\u201c', '"')  # Left double quote
    text = text.replace('\u201d', '"')  # Right double quote
    text = text.replace('\u201e', '"')  # Double low quote
    text = text.replace('\u201f', '"')  # Double high-reversed quote

    # Other quote-like characters
    text = text.replace('\u2039', "'")  # Single left-pointing angle quote
    text = text.replace('\u203a', "'")  # Single right-pointing angle quote
    text = text.replace('`', "'")       # Backtick
    text = text.replace('´', "'")       # Acute accent

    return text

  updated_count = 0

  log_info("Starting quote cleanup...")

  for row in app_tables.metric_dictionary.search():
    # Clean all code/path fields including the new attempts_path
    row['function_name']       = sanitize_quotes(row['function_name'])
    row['result_path']         = sanitize_quotes(row['result_path'])
    row['attempts_path']       = sanitize_quotes(row['attempts_path'])
    row['data_filter']         = sanitize_quotes(row['data_filter'])
    row['calculation_formula'] = sanitize_quotes(row['calculation_formula'])
    row['video_path']          = sanitize_quotes(row['video_path'])

    updated_count += 1

    if updated_count % 50 == 0:
      log_info(f"Cleaned {updated_count} rows...")

  log_info(f"✓ Cleanup complete! Updated {updated_count} rows.")

  return {
    'success': True,
    'updated': updated_count
  }


@anvil.server.callable
def generate_slim_metric_dictionary_md():
  """
  Converts the metric_dictionary Anvil table into a slimmed-down Markdown 
  file optimized for AI context caching.
  
  Includes coach_alias so AI responses can use plain-language metric names
  that coaches will recognize.
  """
  # Define columns critical for AI logic
  # coach_alias added so the AI can refer to metrics by the name coaches use
  cols_to_include = [
    'metric_id', 'coach_alias', 'metric_name', 'coach_view',
    'coach_speak_elite', 'coach_speak_good',
    'coach_speak_average', 'coach_speak_poor'
  ]

  # Header for Markdown Table
  md_output = "| " + " | ".join(cols_to_include) + " |\n"
  md_output += "| " + " | ".join(["---"] * len(cols_to_include)) + " |\n"

  # Rows
  for row in app_tables.metric_dictionary.search():
    row_data = []
    for col in cols_to_include:
      val = str(row[col]) if row[col] is not None else ""
      # Sanitize for MD table (remove pipes and newlines)
      val = val.replace("|", "\\|").replace("\n", " ")
      row_data.append(val)
    md_output += "| " + " | ".join(row_data) + " |\n"

  # Return as downloadable Media Object
  return BlobMedia("text/markdown", md_output.encode('utf-8'), name="metric_dictionary_logic.md")

"""
Import Competitive Level (comp_level) Data into master_player

Reads a CSV containing player rankings from an external source (e.g., TVR)
and updates the master_player table with two new columns:
  - comp_level_rank  : integer rank (1 = best)
  - comp_level_score : float score

Matching strategy (in order of preference):
  1. player_uuid  — most reliable, used when present in both CSV and table
  2. league + team + number — fallback for rows without player_uuid

The CSV is expected to have at minimum these columns:
  player_uuid, league, team, number, tvr_rank, tvr

Usage (call from a server module or a DataMgr form):
    result = import_comp_level_from_csv(csv_text)
    print(result['summary'])

Author: Beach Volleyball Analytics
Created: 2026-05
"""


# ============================================================================
# MAIN IMPORT FUNCTION
# ============================================================================
@anvil.server.callable
def import_comp_level_from_csv(csv_text):
  """
  Read comp_level data from CSV text and update master_player rows.

  Args:
      csv_text (str): Raw CSV content (read from an uploaded file or a string).

  Returns:
      dict: {
          'updated'  : int  - rows successfully updated,
          'skipped'  : int  - rows in CSV with no match in master_player,
          'no_data'  : int  - rows where tvr_rank AND tvr were both blank,
          'errors'   : int  - rows that raised an exception,
          'detail'   : list - one string per row describing what happened,
          'summary'  : str  - human-readable summary paragraph
      }
  """
  log_info("Starting comp_level import from CSV...")

  # ── Load CSV ──────────────────────────────────────────────────────────────
  try:
    df = pd.read_csv(io.StringIO(csv_text))
  except Exception as e:
    msg = f"Could not parse CSV: {e}"
    log_error(msg)
    return {
      'success':  False,
      'imported': 0,
      'skipped':  0,
      'no_data':  0,
      'errors':   [msg],
      'summary':  msg
    }

  log_info(f"CSV loaded: {len(df)} rows, columns: {list(df.columns)}")

  # ── Validate required columns ─────────────────────────────────────────────
  required = {'league', 'team', 'number'}
  missing = required - set(df.columns)
  if missing:
    msg = f"CSV is missing required columns: {missing}"
    log_error(msg)
    return {
      'success':  False,
      'imported': 0,
      'skipped':  0,
      'no_data':  0,
      'errors':   [msg],
      'summary':  msg
    }

  has_uuid_col = 'player_uuid' in df.columns

  # ── Counters & detail log ─────────────────────────────────────────────────
  imported = 0   # rows successfully written (was 'updated')
  skipped  = 0
  no_data  = 0
  errors   = []  # list of error strings (was a count)

  # ── Process each row ──────────────────────────────────────────────────────
  for idx, row in df.iterrows():

    # --- Extract rank / score (either tvr_rank/tvr or comp_level_rank/comp_level_score) ---
    rank_val  = _safe_int(row.get('tvr_rank')  or row.get('comp_level_rank'))
    score_val = _safe_float(row.get('tvr')     or row.get('comp_level_score'))

    if rank_val is None and score_val is None:
      # Nothing to import for this row
      no_data += 1
      log_debug(f"Row {idx}: no rank or score data — skipped")
      continue

    # --- Find matching master_player row ---
    player_row = None
    match_method = None

    # Try player_uuid first
    if has_uuid_col:
      uuid_val = str(row.get('player_uuid', '') or '').strip()
      if uuid_val and uuid_val not in ('nan', ''):
        try:
          player_row = app_tables.master_player.get(player_uuid=uuid_val)
          if player_row:
            match_method = f"player_uuid={uuid_val}"
        except Exception as e:
          log_debug(f"Row {idx}: uuid lookup failed ({e}), will try fallback")

    # Fallback: league + team + number
    if not player_row:
      try:
        league_val = str(row.get('league', '') or '').strip()
        team_val   = str(row.get('team',   '') or '').strip()
        number_val = _safe_int(row.get('number'))

        if league_val and team_val and number_val is not None:
          player_row = app_tables.master_player.get(
            league=league_val,
            team=team_val,
            number=number_val
          )
          if player_row:
            match_method = f"league={league_val} team={team_val} number={number_val}"
      except Exception as e:
        log_debug(f"Row {idx}: fallback lookup failed ({e})")

    if not player_row:
      skipped += 1
      name_hint = str(row.get('fullname') or row.get('shortname') or f"row {idx}")
      log_debug(f"Row {idx} ({name_hint}): no matching player in master_player — skipped")
      continue

    # --- Update the row ---
    try:
      player_row['comp_level_rank']  = rank_val
      player_row['comp_level_score'] = score_val
      imported += 1
      name_hint = str(row.get('fullname') or row.get('shortname') or f"row {idx}")
      log_debug(f"Updated {name_hint}: rank={rank_val}, score={score_val}")
    except Exception as e:
      name_hint = str(row.get('fullname') or row.get('shortname') or f"row {idx}")
      errors.append(f"Row {idx} ({name_hint}): error writing to table — {e}")
      log_error(f"Row {idx}: write error — {e}")

  # ── Summary ───────────────────────────────────────────────────────────────
  summary = (
    f"comp_level import complete. "
    f"Imported: {imported} | Skipped (no match): {skipped} | "
    f"No data: {no_data} | Errors: {len(errors)} | Total CSV rows: {len(df)}"
  )
  log_info(summary)

  return {
    'success':  len(errors) == 0,
    'imported': imported,
    'skipped':  skipped,
    'no_data':  no_data,
    'errors':   errors,
    'summary':  summary
  }


# ============================================================================
# SAFE TYPE HELPERS
# ============================================================================

def _safe_int(val):
  """Return int or None."""
  try:
    if val is None or (isinstance(val, float) and pd.isna(val)):
      return None
    return int(float(val))
  except (ValueError, TypeError):
    return None


def _safe_float(val):
  """Return float or None."""
  try:
    if val is None or (isinstance(val, float) and pd.isna(val)):
      return None
    f = float(val)
    return round(f, 4)
  except (ValueError, TypeError):
    return None