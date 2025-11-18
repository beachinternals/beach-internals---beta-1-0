# logger_utils.py
import traceback
from anvil_extras.logging import Logger, DEBUG
import json
from datetime import datetime
import inspect

# Import here to avoid circular imports
# We'll do lazy import inside functions
# import anvil.tables as tables
# from anvil.tables import app_tables

# -----------------------------------------------------------------------------
# Logger setup
# -----------------------------------------------------------------------------
critical_logger = Logger(
  name="critical",
  level=DEBUG,
  format="{name}-{level} {datetime:%Y-%m-%d %H:%M:%S}: {msg}"
)

# -----------------------------------------------------------------------------
# Helper function to write to error_log table
# -----------------------------------------------------------------------------
def _write_to_error_log(severity: str, message: str, source: str = None, traceback_text: str = None):
  """
  Internal function to write log entries to the error_log database table.
  
  Args:
    severity: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    message: The log message
    source: Source of the log (function name, module, etc.)
    traceback_text: Stack trace if applicable
  """
  try:
    # Lazy import to avoid circular dependencies
    import anvil.tables as tables
    from anvil.tables import app_tables

    # Only write ERROR and CRITICAL to database to avoid clutter
    if severity not in ['ERROR', 'CRITICAL']:
      return

    # Get source if not provided
    if source is None:
      frame = inspect.currentframe()
      try:
        # Go back 2 frames: _write_to_error_log -> log_error/log_critical -> actual caller
        caller_frame = frame.f_back.f_back
        source = f"{caller_frame.f_code.co_filename}:{caller_frame.f_code.co_name}:{caller_frame.f_lineno}"
      except:
        source = "Unknown"
      finally:
        del frame

    # Write to error_log table
    app_tables.error_log.add_row(
      timestamp=datetime.now(),
      severity=severity,
      source=source,
      message=message,
      traceback_text=traceback_text
    )
  except Exception as e:
    # If database write fails, log to console but don't raise exception
    # This prevents logging failures from breaking the application
    critical_logger.error(f"Failed to write to error_log table: {str(e)}")

# -----------------------------------------------------------------------------
# Logging helpers - Enhanced to write to database
# -----------------------------------------------------------------------------
def log_debug(msg: str, source: str = None):
  """
  Log debug with traceback (if one exists in the current context).
  DEBUG messages are NOT written to error_log table.
  """
  tb_str = traceback.format_exc()
  full_msg = f"{msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else msg
  critical_logger.debug(full_msg)
  # Debug messages not written to database

def log_info(msg: str, source: str = None):
  """
  Log info without traceback.
  INFO messages are NOT written to error_log table.
  """
  critical_logger.info(msg)
  # Info messages not written to database

def log_error(msg: str, with_traceback=True, source: str = None):
  """
  Log error. By default includes traceback, unless with_traceback=False.
  ERROR messages ARE automatically written to error_log table.
  
  Args:
    msg: Error message
    with_traceback: Whether to include traceback (default: True)
    source: Optional source identifier (auto-detected if not provided)
  """
  tb_str = None
  if with_traceback:
    tb_str = traceback.format_exc()
    full_msg = f"{msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else msg
  else:
    full_msg = msg

  # Log to console
  critical_logger.error(full_msg)

  # Write to database
  _write_to_error_log(
    severity='ERROR',
    message=msg,
    source=source,
    traceback_text=tb_str if with_traceback else None
  )

def log_critical(msg: str, source: str = None):
  """
  Log critical error with traceback.
  CRITICAL messages ARE automatically written to error_log table.
  
  Args:
    msg: Critical error message
    source: Optional source identifier (auto-detected if not provided)
  """
  tb_str = traceback.format_exc()
  full_msg = f"CRITICAL: {msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else f"CRITICAL: {msg}"

  # Log to console
  critical_logger.error(full_msg)

  # Write to database
  _write_to_error_log(
    severity='CRITICAL',
    message=f"CRITICAL: {msg}",
    source=source,
    traceback_text=tb_str
  )

def log_row(label: str, row):
  """
  Log the contents of a data row (dict or LiveObjectProxy).
  This is a DEBUG level log and is NOT written to error_log table.
  """
  try:
    row_dict = dict(row)
    formatted = json.dumps(row_dict, indent=2)
  except Exception:
    formatted = str(row)
  log_debug(f"{label}: {formatted}")

# -----------------------------------------------------------------------------
# Additional utility function for querying error logs
# -----------------------------------------------------------------------------
def get_recent_errors(limit: int = 50, severity: str = None):
  """
  Retrieve recent errors from error_log table.
  
  Args:
    limit: Maximum number of errors to return
    severity: Filter by severity (ERROR, CRITICAL, etc.) or None for all
  
  Returns:
    List of error log rows
  """
  try:
    import anvil.tables as tables
    from anvil.tables import app_tables

    query = app_tables.error_log.search(
      tables.order_by("timestamp", ascending=False)
    )

    if severity:
      query = [row for row in query if row['severity'] == severity]

    return list(query)[:limit]
  except Exception as e:
    critical_logger.error(f"Failed to retrieve error logs: {str(e)}")
    return []