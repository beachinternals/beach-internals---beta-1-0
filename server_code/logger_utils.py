# logger_utils.py - IMPROVED VERSION
# Replace your existing logger_utils.py with this version
# ============================================================================
import traceback
from anvil_extras.logging import Logger, DEBUG
from anvil.tables import app_tables
import json
import inspect
from datetime import datetime

# -----------------------------------------------------------------------------
# Logger setup
# -----------------------------------------------------------------------------
critical_logger = Logger(
  name="critical",
  level=DEBUG,
  format="{name}-{level} {datetime:%Y-%m-%d %H:%M:%S}: {msg}"
)

# -----------------------------------------------------------------------------
# Helper function to auto-detect calling function
# -----------------------------------------------------------------------------
def _get_calling_function(stack_offset=2):
  """
    Automatically detect which function called the logging function.
    
    Args:
        stack_offset: How far up the stack to look (default 2)
        
    Returns:
        String with format "filename.function_name"
    """
  try:
    stack = inspect.stack()

    # stack[0] = _get_calling_function (this function)
    # stack[1] = log_error/log_critical/etc (the logging function)
    # stack[2] = the function that called log_error (what we want!)

    if len(stack) > stack_offset:
      frame = stack[stack_offset]
      function_name = frame.function
      filename = frame.filename.split('/')[-1].replace('.py', '')
      return f"{filename}.{function_name}"

    return "unknown"

  except Exception:
    return "unknown"

# -----------------------------------------------------------------------------
# Logging helpers - IMPROVED with auto-detection and database logging
# -----------------------------------------------------------------------------

def log_debug(msg: str, source=None):
  """
    Log debug with traceback (if one exists in the current context).
    
    Args:
        msg: Debug message
        source: Optional source identifier (auto-detected if not provided)
    """
  if source is None:
    source = _get_calling_function(stack_offset=2)

  tb_str = traceback.format_exc()
  full_msg = f"[{source}] {msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else f"[{source}] {msg}"
  critical_logger.debug(full_msg)


def log_info(msg: str, source=None):
  """
    Log info without traceback.
    
    Args:
        msg: Info message
        source: Optional source identifier (auto-detected if not provided)
    """
  if source is None:
    source = _get_calling_function(stack_offset=2)

  critical_logger.info(f"[{source}] {msg}")


def log_error(msg: str, with_traceback=True, source=None, save_to_db=True, context=None):
  """
    Log error. By default includes traceback and saves to error_log table.
    
    Args:
        msg: Error message
        with_traceback: Include traceback (default True)
        source: Optional source identifier (auto-detected if not provided)
        save_to_db: Save to error_log table (default True)
        context: Optional dict with additional context
        
    Example:
        try:
            risky_operation()
        except Exception as e:
            log_error(f"Failed to process: {e}", context={'filename': filename})
    """
  if source is None:
    source = _get_calling_function(stack_offset=2)

    # Log to anvil_extras logger
  if with_traceback:
    tb_str = traceback.format_exc()
    full_msg = f"[{source}] {msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else f"[{source}] {msg}"
    critical_logger.error(full_msg)
  else:
    critical_logger.error(f"[{source}] {msg}")

    # Also save to error_log database table
  if save_to_db:
    try:
      # Extract error type from traceback if available
      tb_str = traceback.format_exc()
      error_type = "Error"
      if "Error:" in tb_str:
        # Try to extract the exception type
        lines = tb_str.split('\n')
        for line in reversed(lines):
          if 'Error:' in line or 'Exception:' in line:
            error_type = line.split(':')[0].strip().split()[-1]
            break

            # Build context JSON
      context_dict = context or {}
      if with_traceback:
        context_dict['traceback'] = tb_str[:1000]  # Limit size

      app_tables.error_log.add_row(
        timestamp=datetime.now(),
        source=source,
        error_type=error_type,
        error_message=msg[:500],  # Limit message length
        context_json=json.dumps(context_dict),
        severity='error'
      )

    except Exception as db_error:
      # Don't let database logging break the application
      critical_logger.error(f"Failed to save error to database: {db_error}")


def log_critical(msg: str, source=None, save_to_db=True, context=None):
  """
    Log critical error with traceback and save to database.
    
    Args:
        msg: Critical error message
        source: Optional source identifier (auto-detected if not provided)
        save_to_db: Save to error_log table (default True)
        context: Optional dict with additional context
        
    Example:
        try:
            critical_operation()
        except Exception as e:
            log_critical(f"System failure: {e}", context={'user': user_id})
    """
  if source is None:
    source = _get_calling_function(stack_offset=2)

    # Log to anvil_extras logger
  tb_str = traceback.format_exc()
  full_msg = f"[{source}] CRITICAL: {msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else f"[{source}] CRITICAL: {msg}"
  critical_logger.error(full_msg)

  # Also save to error_log database table
  if save_to_db:
    try:
      # Extract error type from traceback
      error_type = "CriticalError"
      if "Error:" in tb_str:
        lines = tb_str.split('\n')
        for line in reversed(lines):
          if 'Error:' in line or 'Exception:' in line:
            error_type = line.split(':')[0].strip().split()[-1]
            break

            # Build context JSON
      context_dict = context or {}
      context_dict['traceback'] = tb_str[:1000]  # Limit size

      app_tables.error_log.add_row(
        timestamp=datetime.now(),
        source=source,
        error_type=error_type,
        error_message=msg[:500],  # Limit message length
        context_json=json.dumps(context_dict),
        severity='critical'
      )

    except Exception as db_error:
      critical_logger.error(f"Failed to save critical error to database: {db_error}")


def log_row(label: str, row, source=None):
  """
    Log the contents of a data row (dict or LiveObjectProxy).
    
    Args:
        label: Description of the row being logged
        row: The data row to log
        source: Optional source identifier (auto-detected if not provided)
    """
  if source is None:
    source = _get_calling_function(stack_offset=2)

  try:
    row_dict = dict(row)
    formatted = json.dumps(row_dict, indent=2)
  except Exception:
    formatted = str(row)

  log_debug(f"{label}: {formatted}", source=source)


# -----------------------------------------------------------------------------
# NEW: Convenience function for logging exceptions
# -----------------------------------------------------------------------------

def log_exception(exception, message=None, source=None, severity='error', context=None):
  """
    Convenience function to log an exception with full context.
    
    Args:
        exception: The exception object
        message: Optional custom message (uses str(exception) if not provided)
        source: Optional source identifier (auto-detected if not provided)
        severity: 'error' or 'critical' (default 'error')
        context: Optional dict with additional context
        
    Example:
        try:
            process_btd_file(filename)
        except Exception as e:
            log_exception(e, context={'filename': filename})
            raise
    """
  if source is None:
    source = _get_calling_function(stack_offset=2)

  msg = message or str(exception)

  if severity == 'critical':
    log_critical(msg, source=source, context=context)
  else:
    log_error(msg, with_traceback=True, source=source, context=context)


# -----------------------------------------------------------------------------
# USAGE EXAMPLES
# -----------------------------------------------------------------------------

"""
EXAMPLE 1: Auto-detect source (recommended)
-------------------------------------------
def my_function(player_name):
    try:
        data = load_player_data(player_name)
    except Exception as e:
        log_error(f"Failed to load player data: {e}")  
        # Source auto-detected as "my_module.my_function"
        return None


EXAMPLE 2: Add context for debugging
-------------------------------------
def process_btd_file(filename):
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        log_error(
            f"Failed to process BTD file: {e}",
            context={'filename': filename, 'size': os.path.getsize(filename)}
        )
        return None


EXAMPLE 3: Use log_exception convenience function
--------------------------------------------------
def generate_report(player_name, report_type):
    try:
        report = create_report(player_name, report_type)
    except Exception as e:
        log_exception(e, context={'player': player_name, 'type': report_type})
        raise


EXAMPLE 4: Manual source specification
---------------------------------------
def complex_function():
    try:
        step_one()
    except Exception as e:
        log_error(f"Step 1 failed: {e}", source="complex_function.step_one")


EXAMPLE 5: Critical errors
---------------------------
try:
    critical_system_operation()
except Exception as e:
    log_critical(f"System failure: {e}", context={'user': current_user})
    # This logs with severity='critical' and saves to database
"""