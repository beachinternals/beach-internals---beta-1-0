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
            
        def to_flag(value):
          # Yes/No flag columns (set_level_whole, set_level_half, correlation).
          # Blank/None/No -> 'No'.  'Yes' (any case) -> 'Yes'.
          # Stored as a normalized string so the table is consistent
          # whether the cell came from Numbers as Yes/No or blank.
          if value is None:
            return 'No'
          s = str(value).strip().lower()
          return 'Yes' if s in ('yes', 'y', 'true', '1') else 'No'
  
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
          drill_down_report=row.get('drill_down_report', ''),
          set_level_whole=to_flag(row.get('set_level_whole')),
          set_level_half=to_flag(row.get('set_level_half')),
          correlation=to_flag(row.get('correlation'))
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

  Scalar metrics -> one table row each (as before).
  Distribution metrics (return_type='distribution') -> excluded from the
  table and instead documented in a DISTRIBUTION LEGEND appended after it,
  which teaches the AI how to decode any DIST| line. One legend covers all
  cells of all distribution families, and scales to future families.
  """
  cols_to_include = [
    'metric_id', 'coach_alias', 'metric_name', 'coach_view',
    'coach_speak_elite', 'coach_speak_good',
    'coach_speak_average', 'coach_speak_poor'
  ]

  md_output = "| " + " | ".join(cols_to_include) + " |\n"
  md_output += "| " + " | ".join(["---"] * len(cols_to_include)) + " |\n"

  # Collect distribution rows to document separately
  distribution_rows = []

  for row in app_tables.metric_dictionary.search():
    if str(row['return_type']).strip() == 'distribution':
      distribution_rows.append(row)
      continue  # not a table row — documented in the legend below

    row_data = []
    for col in cols_to_include:
      val = str(row[col]) if row[col] is not None else ""
      val = val.replace("|", "\\|").replace("\n", " ")
      row_data.append(val)
    md_output += "| " + " | ".join(row_data) + " |\n"

  # ── Distribution legend (only if any distribution metrics exist) ────────
  if distribution_rows:
    md_output += _build_distribution_legend(distribution_rows)
    md_output += _build_phase_legend()
    md_output += _build_setheight_legend()
    md_output += _build_serves_received_note()

  return BlobMedia("text/markdown", md_output.encode('utf-8'),
                   name="metric_dictionary_logic.md")


def _build_distribution_legend(distribution_rows):
  """
  Build the legend that teaches the AI to decode DIST| lines.
  Generated from the distribution rows present, so adding a new distribution
  family to the dictionary automatically adds its entry here.
  """
  L = []
  L.append("\n\n## DISTRIBUTION METRICS\n")
  L.append("Some metrics are VOLUME DISTRIBUTIONS, not single numbers. They "
           "appear in data files on their own line, not as a scalar token:\n")
  L.append("```")
  L.append("DIST|<metric_id>|n=<total>|err:<rate>|<cell>:<pct> <cell>:<pct> ...")
  L.append("```")
  L.append("- Each cell is a fraction of n (the cells for one metric sum to ~1.0).")
  L.append("- Cells that round to 0.00 are omitted (sparse). Absent = ~zero.")
  L.append("- `n` is the sample size; weight conclusions by it.")
  L.append("- `err:` (when present) is a rate reported alongside, not part of the distribution.\n")

  L.append("### Cell code convention\n")
  L.append("Serve-destination cells use `{source}_{width}{depth}` "
           "(e.g. `3_4d` = served from source 3, landed width 4 depth D):\n")
  L.append("**Serve source** (where the server stands on the endline):")
  L.append("- `1` = Left endline | `3` = Middle | `5` = Right endline")
  L.append("- `0` = serve origin NOT captured (tracking gap) — landing still known")
  L.append("")
  L.append("**Destination width** (attacker's perspective, left→right):")
  L.append("- `1` = Left Pin | `2` = Left Slot | `3` = Middle | `4` = Right Slot | `5` = Right Pin")
  L.append("")
  L.append("**Destination depth** (legal serves land only C/D/E; A/B fold into C):")
  L.append("- `c` = Mid-Short (4–6m) — includes any short serve | `d` = Mid-Deep (6–8m) | `e` = Deep Baseline (8m+)")
  L.append("")
  L.append("**`other`** (e.g. `3_other`) = served from that source but the LANDING "
           "zone was not captured (a serve in play, location unknown — often an "
           "untracked ace). Counts in n; just unplaceable on the grid.\n")

  L.append("### Distribution families present\n")
  for row in distribution_rows:
    mid   = str(row['metric_id'])
    alias = str(row['coach_alias']) if row['coach_alias'] else mid
    desc  = str(row['metric_name']) if row['metric_name'] else ""
    desc  = desc.replace("\n", " ")
    L.append(f"- **{mid}** ({alias}): {desc}")
  L.append("")

  return "\n".join(L)

def _build_phase_legend():
  """
  Legend for HALF| lines and *_1a.._3b phase metrics. Teaches the phase codes,
  the population-vs-sample reading (SCOPED tightly to per-set/half values),
  and gives the AI coach-friendly language so non-statisticians understand it.
  """
  L = []
  L.append("\n\n## PER-SET PHASE METRICS (HALF lines and *_1a.._3b)\n")
  L.append("In the set-by-set data, each set block may carry a HALF line:\n")
  L.append("```")
  L.append("HALF|metric_1a:value(n=) metric_1b:value(n=) ...")
  L.append("```")
  L.append("These split a metric into the two halves of THAT set, divided at "
           "the technical timeout:\n")
  L.append("**Phase codes** — `{set}{half}`:")
  L.append("- `1a`/`1b` = Set 1 first/second half (split at combined score 20)")
  L.append("- `2a`/`2b` = Set 2 first/second half (split at 20)")
  L.append("- `3a`/`3b` = Set 3 first/second half (split at 14; set 3 plays to 15)")
  L.append("A set block shows only its own set's phases (a Set 1 block shows 1a/1b).\n")

  L.append("### How to read the sample size (n) — READ CAREFULLY, IT DIFFERS BY CONTEXT\n")
  L.append("**Per-set and per-half values (SET blocks, HALF lines): n is the "
           "COMPLETE record of that one set/half, not a sample.**")
  L.append("A value like `fbhe_1a_match:-0.500(n=2)` means she attacked exactly "
           "twice in that half and both were stuffed. It is the precise, whole "
           "truth of that half — not a noisy estimate. So it is a firm FACT "
           "about WHAT HAPPENED in that specific half.")
  L.append("BUT: a single small half says nothing about her TENDENCIES. Two "
           "swings cannot describe a habit. To speak about what she TENDS to do, "
           "use the player-level aggregate (large n across all matches).\n")
  L.append("**This 'it's a complete fact, not a sample' reading applies ONLY to "
           "per-set and per-half numbers** — because a single set/half is a "
           "complete, bounded event with every contact present.\n")
  L.append("**Aggregate metrics (the Aggregate Performance section) are the "
           "OPPOSITE case: there, n matters in the ordinary way.** A low-n "
           "aggregate means FEW events were observed all season, so the value is "
           "genuinely thin evidence and should be treated cautiously. Do NOT "
           "carry the 'low n is still a firm fact' reading over to aggregate "
           "metrics — it is false there.\n")

  L.append("### Explaining this to coaches (they are not statisticians)\n")
  L.append("Translate to plain coaching language, never statistics jargon:")
  L.append("- Small-n per-half result -> frame as WHAT HAPPENED, with the swing "
           "count: e.g. \"In that first half she got just two swings and both "
           "were stuffed — that's the whole story of that half, but it's only "
           "two balls, so don't read it as a trend; her season first-half "
           "number is the one to judge her by.\"")
  L.append("- Low-n AGGREGATE -> frame as TENTATIVE: e.g. \"We've only seen this "
           "a handful of times all year, so treat it as a maybe, not a fact.\"")
  L.append("- The core distinction in coach terms: **\"what happened\" (a single "
           "event, however few balls) vs **\"what she tends to do\"** (needs "
           "volume). Per-set/half = what happened. Aggregate = the tendency.\n")

  L.append("Denominator-rich metrics (err_den, tcr, goodpass, knockout) stay "
           "meaningful per half even at modest n; efficiency ratios (fbhe) at "
           "very low n are exact facts about that half but should never be "
           "generalized into a tendency.\n")
  return "\n".join(L)

def _build_setheight_legend():
  """Legend for the SETHEIGHT line (set-height mismatch: volume + fbhe per
     tempo bucket). Explains tempo names map to set AIR TIME (not guessed
     height), the cell format, the mismatch read, and the ~ directional mark."""
  L = []
  L.append("\n\n## SET-HEIGHT MISMATCH (SETHEIGHT line)\n")
  L.append("A player's set-height profile appears on its own line:\n")
  L.append("```")
  L.append("SETHEIGHT|total=N|quick:v0.03/fbhe0.429(n7) low:v0.17/fbhe0.439(n41) medium:v0.59/fbhe0.500(n142) ...")
  L.append("```")
  L.append("It describes the sets DELIVERED TO this player (the sets she "
           "attacks off of), bucketed by tempo. For each bucket:")
  L.append("- `v` = VOLUME share — fraction of her sets in this tempo bucket "
           "(the buckets sum to ~1.0). This is where she is being set.")
  L.append("- `fbhe` = her hitting efficiency on sets of that tempo. This is "
           "how well she hits it.")
  L.append("- `(n..)` = attacks in that bucket.\n")
  L.append("**Tempo buckets are SET AIR TIME (flight-time), a robust apex-height "
           "proxy — NOT a guessed height in meters.** Low time = quick/flat set; "
           "high time = higher/floatier set. Names, fastest to highest:")
  L.append("- `quick` (very fast/flat) < `low` < `medium` < `high` < `floaty` (highest)\n")
  L.append("**The MISMATCH read (the point of this line):** compare VOLUME vs "
           "FBHE across buckets. Where she is set most (high v) versus where she "
           "hits best (high fbhe) may not match. Example: if she is set `high` "
           "40% of the time but her fbhe is much better on `low`/`medium` sets, "
           "that is a coachable mismatch — the setter should deliver lower. If "
           "her best-fbhe bucket is also her highest-volume bucket, she is "
           "well-matched.\n")
  L.append("**`fbhe~` (tilde) = directional only:** the bucket has too few "
           "attacks (below the metric's minimum) for a trustworthy efficiency, "
           "so fbhe is suppressed. Volume is still shown (a clean count at any "
           "n). Treat `fbhe~` as 'we see a few but not enough to judge.'\n")
  return "\n".join(L)


def _build_serves_received_note():
  """One-block note on interpreting serves_received_pct (targeting signal)."""
  L = []
  L.append("\n\n## SERVES RECEIVED (serves_received_pct)\n")
  L.append("`serves_received_pct` = of the serves this player's SIDE received, "
           "the fraction SHE took (her receives / her+partner receives). It "
           "measures OPPONENT TARGETING:")
  L.append("- High (e.g. 0.65) = opponents are serving AT her (hunting her).")
  L.append("- Low (e.g. 0.35) = opponents serve AWAY from her (toward her "
           "partner) — often because she is the stronger passer.")
  L.append("- ~0.50 = balanced.\n")
  L.append("The `_1a.._3b` per-half versions reveal MID-MATCH targeting shifts: "
           "if her share jumps from set 1 to set 3, the opponent adjusted to "
           "target (or avoid) her — a scouting-response signal. Per the phase "
           "rules above, a single set/half value is the exact record of that "
           "segment; read trends across the aggregate.\n")
  return "\n".join(L)
  
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