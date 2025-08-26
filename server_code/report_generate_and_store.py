import anvil.server
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables.query as q
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from PIL import Image
from io import BytesIO
import base64
import pandas as pd
import uuid
import json
from datetime import datetime, timedelta, date
import inspect
import matplotlib.pyplot as plt
import matplotlib as mpl
import math
import scipy.stats as stats
import numpy as np
import importlib
import sys
from typing import Tuple, List, Dict, Any

from plot_functions import *
from server_functions import *
#from reports_player_new import *
from reports_player import *
from reports_dashboard import *
from reports_league import *
from reports_pair import *
from reports_scouting import *

# Option A: Check if it's just called Logger
from anvil_extras.logging import Logger
import logging
# Create a detailed formatter
logger = Logger()
formatter = logging.Formatter('%(levelname)s - %(funcName)s:%(lineno)d - %(message)s')





# Use it in your code
logger.info("Application started")
logger.debug("Debug information")
logger.warning("This is a warning")
logger.error("An error occurred")


# Cache for valid function names
_valid_functions_cache = None

def get_valid_functions() -> set:
  """Retrieve valid function names from the report_list table, with caching."""
  global _valid_functions_cache
  if _valid_functions_cache is None:
    _valid_functions_cache = {row['function_name'] for row in tables.app_tables.report_list.search()}
  return _valid_functions_cache

  
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
#
#        Generate and Store Report
#
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
@anvil.server.callable
def generate_and_store_report(fnct_name: str, lgy: str, team: str, **rpt_filters) -> str:
  """
    Generate a report using the specified function, store results in report_data table, and return the report ID.
    
    Args:
        fnct_name: Name of the report function to call.
        lgy: League identifier.
        team: Team identifier.
        **rpt_filters: Additional report filters.
    
    Returns:
        report_id: Unique ID of the stored report.
    
    Raises:
        ValueError: If function name is invalid or data processing fails.
    """
  # Initialize result lists
  title_list: List[str] = []
  label_list: List[str] = []
  image_list: List[Any] = []
  df_list: List[Dict] = []
  df_desc_list: List[str] = []
  image_desc_list: List[str] = []

  # Log the request
  logger.info(f"Generating report: fnct_name: {fnct_name}, lgy: {lgy}, team: {team}, filters: {rpt_filters}")

  # Validate function name against report_list table
  valid_functions = get_valid_functions()
  if fnct_name not in valid_functions:
    logger.error(f"Function name '{fnct_name}' not found in report_list table")
    title_list.append(f"Report Not Found: {fnct_name}")
  else:
    # Dynamically get the function from globals
    try:
      func = globals()[fnct_name]
    except KeyError:
      logger.error(f"Function '{fnct_name}' is not implemented in the global namespace")
      title_list.append(f"Function Error: {fnct_name}")
    else:
      # Call the function
      try:
        title_list, label_list, image_list, df_list, df_desc_list, image_desc_list = func(lgy, team, **rpt_filters)
      except Exception as e:
        logger.error(f"Error executing {fnct_name}: {str(e)}")
        title_list.append(f"Execution Error: {fnct_name}")

    # Close any open matplotlib plots
  try:
    import matplotlib.pyplot as plt
    plt.close('all')
  except ImportError:
    pass  # Matplotlib may not be used

    # Generate unique report ID
  report_id = str(uuid.uuid4())

  # Store in report_data table
  rpt_data_row = tables.app_tables.report_data.add_row(
    report_id=report_id,
    created_at=datetime.now()
  )

  # Store titles (max 10)
  rpt_data_row['no_title'] = min(len(title_list), 10)
  for i, title in enumerate(title_list[:10]):
    rpt_data_row[f'title_{i+1}'] = title

    # Store league in title_4
  rpt_data_row['title_4'] = lgy

  # Store filter text in title_7
  try:
    rpt_data_row['title_7'] = make_filter_text(lgy, **rpt_filters)
  except NameError:
    logger.warning("make_filter_text not defined; skipping filter text storage")
    rpt_data_row['title_7'] = str(rpt_filters)

  # Store player in title_9
  rpt_data_row['title_9'] = rpt_filters.get('player')

  # Store pair in title_10
  rpt_data_row['title_10'] = rpt_filters.get('pair')

    # Store labels (max 10)
  rpt_data_row['no_label'] = min(len(label_list), 10)
  for i, label in enumerate(label_list[:10]):
    rpt_data_row[f'label_{i+1}'] = label

    # Store images (max 10, only non-string types)
  no_images = 0
  for i, image in enumerate(image_list[:10]):
    if not isinstance(image, str):
      rpt_data_row[f'image_{i+1}'] = image
      no_images += 1
  rpt_data_row['no_image'] = no_images

  # Store image descriptions (max 10)
  rpt_data_row['no_image_desc'] = min(len(image_desc_list), 10)
  for i, image_desc in enumerate(image_desc_list[:10]):
    rpt_data_row[f'image_desc_{i+1}'] = image_desc

  # Store DataFrames as markdown (max 10)
  no_dfs = 0
  for i, df_data in enumerate(df_list[:10]):
    if df_data:  # Check if not empty
      try:
        if not isinstance(df_data, list) or not all(isinstance(row, dict) for row in df_data):
          raise ValueError(f"Expected list of dictionaries for df_{i+1}, got {type(df_data)}")
        df_tmp = pd.DataFrame(df_data)
        mkdn_file = df_tmp.to_markdown(index=False)
        rpt_data_row[f'df_{i+1}'] = anvil.BlobMedia(
          content_type="text/plain",
          content=mkdn_file.encode(),
          name=f'df_{i+1}.mkdn'
        )
        no_dfs += 1
      except Exception as e:
        logger.error(f"Failed to store DataFrame {i+1}: {str(e)}")
  rpt_data_row['no_df'] = no_dfs

  # Store DataFrame descriptions (max 10)
  rpt_data_row['no_df_desc'] = min(len(df_desc_list), 10)
  for i, df_desc in enumerate(df_desc_list[:10]):
    rpt_data_row[f'df_desc_{i+1}'] = df_desc

  logger.info(f"Stored report with ID: {report_id}")
  return report_id

  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
  #
  #        Get Report Data
  #
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
@anvil.server.callable
def get_report_data(report_id):

  row = app_tables.report_data.get(report_id=report_id)

  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  if not row:
    return None
  else:
    for i in range(0,row['no_title']):
      title_var = 'title_'+str(i+1)
      title_list[i] = row[title_var]

    for i in range(0,row['no_label']):
      label_var = 'label_'+str(i+1)
      label_list[i] = row[label_var]

    for i in range(0,row['no_image']):
      image_var = 'image_'+str(i+1)
      image_list[i] = row[image_var]

    for i in range(0,row['no_df']):
      df_var = 'df_'+str(i+1)
      mkdn_file = row[df_var].get_bytes().decode('utf-8')
      df_list[i] = mkdn_file

    for i in range(0,row['no_df_desc']):
      df_desc_var = 'df_desc_'+str(i+1)
      df_desc_list[i] = row[df_desc_var]

    for i in range(0,row['no_image_desc']):
      image_desc_var = 'image_desc_'+str(i+1)
      image_desc_list[i] = row[image_desc_var]

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
  #
  #        Generic Report Stub - Copy this as a starting Point
  #
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
def report_test(lgy, team, **rpt_filters):
  """
  Test report function - serves as a stub/template for other report functions.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

  # Example: Create a simple DataFrame from filtered data
  if not ppr_df.empty:
    # Convert first 10 rows to dict format for storage
    sample_data = ppr_df.head(10).to_dict('records')
    df_list[0] = sample_data

  # Example: Create a summary DataFrame
  if not ppr_df.empty and len(ppr_df) > 1:
    summary_data = [
      {'Metric': 'Total Records', 'Value': len(ppr_df)},
      {'Metric': 'Team', 'Value': team},
      {'Metric': 'League', 'Value': disp_league},
      {'Metric': 'Gender', 'Value': disp_gender},
      {'Metric': 'Year', 'Value': disp_year}
    ]
    df_list[1] = summary_data

  # Example: You would generate actual plots/images here
  # For now, just adding placeholder descriptions
  if not ppr_df.empty:
    _ = None
    # image_list[0] = your_generated_plot_here
    # image_list[1] = your_other_plot_here

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list




  

def make_filter_text( lgy, **rpt_filters):
  
  filter_text = f"""
Data Filters:
- Date Created : {datetime.today().strftime('%Y-%m-%d')}
- League / Gender / Year : {lgy}
    """
  filter_text = filter_text + '\n'

  filter_text = filter_text if 'filter_text' in locals() else ""
  if rpt_filters.get("pair") is not None and rpt_filters.get("pair") != "":
    filter_text += f"- Pair = {str(rpt_filters['pair'])}\n"
  if rpt_filters.get("player") is not None and rpt_filters.get("player") != "":
    filter_text += f"- Player = {str(rpt_filters['player'])}\n"
  if rpt_filters.get("comp_l1") is not None and rpt_filters.get("comp_l1") != "":
    filter_text += f"- Competition Level 1 = {str(rpt_filters['comp_l1'])}\n"
  if rpt_filters.get("comp_l2") is not None and rpt_filters.get("comp_l2") != "":
    filter_text += f"- Competition Level 2 = {str(rpt_filters['comp_l2'])}\n"
  if rpt_filters.get("comp_l3") is not None and rpt_filters.get("comp_l3") != "":
    filter_text += f"- Competition Level 3 = {str(rpt_filters['comp_l3'])}\n"
  if rpt_filters.get("start_date") is not None and rpt_filters.get("start_date") != "":
    filter_text += f"- Start Date = {str(rpt_filters['start_date'])}\n"
  if rpt_filters.get("end_date") is not None and rpt_filters.get("end_date") != "":
    filter_text += f"- End Date = {str(rpt_filters['end_date'])}\n"
  if rpt_filters.get("set") is not None and rpt_filters.get("set") != "":
    filter_text += f"- Set = {str(rpt_filters['set'])}\n"
  if rpt_filters.get("set_ht_low") is not None and rpt_filters.get("set_ht_low") != "":
    filter_text += f"- Set Height, Low  = {str(rpt_filters['set_ht_low'])}\n"
  if rpt_filters.get("set_ht_high") is not None and rpt_filters.get("set_ht_high") != "":
    filter_text += f"- Set Height, High  = {str(rpt_filters['set_ht_high'])}\n"
  if rpt_filters.get("set_touch_type") is not None and rpt_filters.get("set_touch_type") != "":
    filter_text += f"- Set Touch Type = {str(rpt_filters['set_touch_type'])}\n"
  if rpt_filters.get("pass_oos") is not None and rpt_filters.get("pass_oos") != "":
    filter_text += f"- Pass = {' Out System Only' if rpt_filters['pass_oos'] == 1 else ' In of System Only' }\n"
  if rpt_filters.get("att_ht_low") is not None and rpt_filters.get("att_ht_low") != "":
    filter_text += f"- Attack Height, Low  = {str(rpt_filters['att_ht_low'])}\n"
  if rpt_filters.get("att_ht_high") is not None and rpt_filters.get("att_ht_high") != "":
    filter_text += f"- Attack Height, High = {str(rpt_filters['att_ht_high'])}\n"
  if rpt_filters.get("att_speed_low") is not None and rpt_filters.get("att_speed_low") != "":
    filter_text += f"- Attack Speed, Low = {str(rpt_filters['att_speed_low'])}\n"
  if rpt_filters.get("att_speed_high") is not None and rpt_filters.get("att_speed_high") != "":
    filter_text += f"- Attack Speed, High = {str(rpt_filters['att_speed_high'])}\n"
  if rpt_filters.get("pass_ht_low") is not None and rpt_filters.get("pass_ht_low") != "":
    filter_text += f"- Pass Height, Low = {str(rpt_filters['pass_ht_low'])}\n"
  if rpt_filters.get("pass_ht_high") is not None and rpt_filters.get("pass_ht_high") != "":
    filter_text += f"- Pass Height, High = {str(rpt_filters['pass_ht_high'])}\n"
  if rpt_filters.get("srv_fr") is not None and rpt_filters.get("srv_fr") != "":
    filter_text += f"- Serve From (Zones) = {str(rpt_filters['srv_fr'])}\n"
  if rpt_filters.get("srv_to") is not None and rpt_filters.get("srv_to") != "":
    filter_text += f"- Serve to (Zones) = {str(rpt_filters['srv_to'])}\n"

  #print(f" Filter Text: \n {filter_text}")
  return filter_text