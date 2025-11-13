# logger_utils.py
import traceback
from anvil_extras.logging import Logger, DEBUG
import json

# At the very top, add these imports
from datetime import datetime
import anvil.tables as tables
from anvil.tables import app_tables

# Add this flag to enable/disable database logging
ENABLE_DB_LOGGING = True  # Set to False if you want to disable

# -----------------------------------------------------------------------------
# Logger setup
# -----------------------------------------------------------------------------
critical_logger = Logger(
  name="critical",
  level=DEBUG,
  format="{name}-{level} {datetime:%Y-%m-%d %H:%M:%S}: {msg}"
)

def log_debug(msg: str):
  """
    Log debug with traceback (if one exists in the current context).
    """
  tb_str = traceback.format_exc()
  full_msg = f"{msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else msg
  critical_logger.debug(full_msg)

  # NEW: Also store in database
  if ENABLE_DB_LOGGING:
    _store_log_to_db('debug', msg, tb_str if tb_str.strip() != "NoneType: None" else None)


def log_info(msg: str):
  """
    Log info without traceback.
    """
  critical_logger.info(msg)

  # NEW: Also store in database
  if ENABLE_DB_LOGGING:
    _store_log_to_db('info', msg, None)


def log_error(msg: str, with_traceback=True):
  """
    Log error. By default includes traceback, unless with_traceback=False.
    """
  tb_str = None
  if with_traceback:
    tb_str = traceback.format_exc()
    full_msg = f"{msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else msg
    critical_logger.error(full_msg)
  else:
    critical_logger.error(msg)

    # NEW: Also store in database
  if ENABLE_DB_LOGGING:
    _store_log_to_db('error', msg, tb_str if with_traceback and tb_str and tb_str.strip() != "NoneType: None" else None)


def log_critical(msg: str):
  """
    Log critical error with traceback.
    """
  tb_str = traceback.format_exc()
  full_msg = f"CRITICAL: {msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else f"CRITICAL: {msg}"
  critical_logger.error(full_msg)

  # NEW: Also store in database
  if ENABLE_DB_LOGGING:
    _store_log_to_db('critical', msg, tb_str if tb_str.strip() != "NoneType: None" else None)


# NEW HELPER FUNCTION: Add this at the bottom
def _store_log_to_db(severity, message, traceback_text):
  """
    Internal helper to store log entry in database.
    Fails silently to avoid breaking logging if database is unavailable.
    """
  try:
    # Extract source from traceback or use "unknown"
    source = "unknown"
    if traceback_text:
      # Try to extract function name from traceback
      lines = traceback_text.split('\n')
      for line in lines:
        if 'in ' in line and 'File ' in line:
          # Extract function name from line like: '  File "...", line X, in function_name'
          parts = line.split('in ')
          if len(parts) > 1:
            source = parts[-1].strip()
            break

        # Truncate message if too long (database column limit)
    message_truncated = message[:500] if len(message) > 500 else message
    traceback_truncated = traceback_text[:2000] if traceback_text and len(traceback_text) > 2000 else traceback_text

    # Store in database
    app_tables.error_log.add_row(
      timestamp=datetime.now(),
      severity=severity,
      source=source,
      message=message_truncated,
      traceback_text=traceback_truncated
    )
  except Exception as e:
    # NEVER let database logging break the actual logging
    print(f"⚠️ Failed to store log to database: {e}")


