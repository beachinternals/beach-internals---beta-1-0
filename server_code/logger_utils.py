# logger_utils.py
import traceback
from anvil_extras.logging import Logger, DEBUG
import json

# -----------------------------------------------------------------------------
# Logger setup
# -----------------------------------------------------------------------------
critical_logger = Logger(
  name="critical",
  level=DEBUG,
  format="{name}-{level} {datetime:%Y-%m-%d %H:%M:%S}: {msg}"
)

# -----------------------------------------------------------------------------
# Logging helpers
# -----------------------------------------------------------------------------
def log_debug(msg: str):
  """
    Log debug with traceback (if one exists in the current context).
    """
  tb_str = traceback.format_exc()
  critical_logger.debug(f"{msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else msg)

def log_info(msg: str):
  """
    Log info without traceback.
    """
  critical_logger.info(msg)

def log_error(msg: str, with_traceback=True):
  """
    Log error. By default includes traceback, unless with_traceback=False.
    """
  if with_traceback:
    tb_str = traceback.format_exc()
    critical_logger.error(f"{msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else msg)
  else:
    critical_logger.error(msg)

def log_critical(msg: str):
  """
    Log critical error with traceback.
    """
  tb_str = traceback.format_exc()
  critical_logger.error(f"CRITICAL: {msg}\n{tb_str}" if tb_str.strip() != "NoneType: None" else f"CRITICAL: {msg}")

def log_row(label: str, row):
  """
    Log the contents of a data row (dict or LiveObjectProxy).
    """
  try:
    row_dict = dict(row)
    formatted = json.dumps(row_dict, indent=2)
  except Exception:
    formatted = str(row)
  log_debug(f"{label}: {formatted}")

