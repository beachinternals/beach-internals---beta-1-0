import anvil.server
from anvil.tables import app_tables
from anvil import BlobMedia
import csv
import io

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
        function_name_clean = sanitize_quotes(row.get('function_name', ''))
        result_path_clean = sanitize_quotes(row.get('result_path', ''))
        data_filter_clean = sanitize_quotes(row.get('data_filter', ''))
        calculation_formula_clean = sanitize_quotes(row.get('calculation_formula', ''))

        # Add row to table
        app_tables.metric_dictionary.add_row(
          metric_id=row.get('metric_id', ''),
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
          function_name=function_name_clean,
          calculation_formula=calculation_formula_clean,
          data_range_min=to_number(row.get('data_range_min')),
          data_range_max=to_number(row.get('data_range_max')),
          coach_speak_elite=row.get('coach_speak_elite', ''),
          coach_speak_good=row.get('coach_speak_good', ''),
          coach_speak_average=row.get('coach_speak_average', ''),
          coach_speak_poor=row.get('coach_speak_poor', '')
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
    # Check for None or empty without using pandas
    if text is None or text == '' or text == 'nan':
      return text

    text = str(text)
      
    # Replace using Unicode code points (more reliable than copying characters)
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
    # Clean the critical fields
    row['function_name'] = sanitize_quotes(row['function_name'])
    row['result_path'] = sanitize_quotes(row['result_path'])
    row['data_filter'] = sanitize_quotes(row['data_filter'])
    row['calculation_formula'] = sanitize_quotes(row['calculation_formula'])

    updated_count += 1

    if updated_count % 50 == 0:
      log_info(f"Cleaned {updated_count} rows...")

  log_info(f"✓ Cleanup complete! Updated {updated_count} rows.")

  return {
    'success': True,
    'updated': updated_count
  }