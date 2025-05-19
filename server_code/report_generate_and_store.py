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
import datetime
from server_functions import *

@anvil.server.callable
def generate_and_store_report( fnct_name, lgy, team, **rpt_filters ):
  #
  # this routine runs the given report (fnct_name) using the parameters in *(**kwargs) and 
  # stores the data in the report_data table, returngin the index to that table
  #

  #
  # Call the report function storing the results in three lists:
  #      label_list[ ] - list of text tables
  #.     image_list[] - list of images, mainly plots
  #.     df_list[] - a list of dataframes (to be converted to mkdn), but kep as a df for now
  #
  #.     Th emax we can store are 10 items in each list, that is the max in the report_data tables, and the hmtl forms are configured for a max of 10 in each
  label_list = []
  image_list = []
  df_list = []

  label_list, image_list, df_list = anvil.server.call( fnct_name, lgy, team, **rpt_filters )

  print(f"Label List returned, Length: {len(label_list)}, list: {label_list}")
  # now store the returned data in the report_data table
  # Generate unique report ID
  report_id = str(uuid.uuid4())

  # Store in temporary Data Table row
  app_tables.report_data.add_row(
    report_id=report_id,
    created_at=datetime.datetime.now()
  )

  # get this record and store the labels , images, and dfs
  rpt_data_row = app_tables.report_data.get(report_id = report_id)
  rpt_data_row['no_label'] = len(label_list)
  if len(label_list) > 0:
    for i in range(0,len(label_list)):
      var = 'label_'+str(i+1)
      print(f" label to be stored: {var}, label: {label_list[i]}")
      rpt_data_row[var] = label_list[i]
  rpt_data_row['no_image'] = len(image_list)
  if len(image_list) > 0:
    for i in range(0,len(image_list)):
      var = 'image_'+str(i+1)
      rpt_data_row[var] = image_list[i]
  rpt_data_row['no_df'] = len(df_list)
  if len(df_list) > 0:
    for i in range(0,len(df_list)):
      var = 'df_'+str(i+1)
      csv_file = pd.DataFrame.to_csv(df_list[i])
      rpt_data_row[var] = anvil.BlobMedia(content_type="text/plain", content=csv_file.encode(), name=var+'.csv')

  return report_id

@anvil.server.callable
def get_report_data(report_id):

  row = app_tables.report_data.get(report_id=report_id)

  label_list = []
  image_list = []
  df_list = []
  
  if not row:
    return None
  else:
    for i in range(0,row['no_label']):
      label_var = 'label_'+str(i+1)
      label_list.append(row[label_var])
      
    for i in range(0,row['no_image']):
      image_var = 'image_'+str(i+1)
      image_list.append(row[image_var])

    for i in range(0,row['no_df']):
      df_var = 'df_'+str(i+1)
      df_list.append(row[df_var])

        
  return label_list, image_list, df_list

@anvil.server.callable
def report_test( lgy, team, **rpt_filters):
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  
  # now filter the ppr dataframe
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
    
  # initiate return lists
  label_list = ['','','','','','','','','','']
  image_list = []
  df_list = []

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name='report_test')
  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']
  
  
  return label_list, image_list, df_list

@anvil.server.callable
def filter_ppr_df( dataframe, **kwargs):
  # given the dataframe, filter it by rpt_filters
  """
    Generate a report by filtering the DataFrame based on kwargs.
    Args:
        dataframe: pandas DataFrame to filter
        **kwargs: Keyword arguments where key=column_name, value=filter_value
    Returns:
        Filtered DataFrame
    """
  result = dataframe.copy()  # Avoid modifying the original DataFrame
  for column, value in kwargs.items():
    #if column in dataframe.columns:
     # we'll check for ones not set up as equal to
    # this includes: Date Range, srv_to, srv_fr, srv speed, pass height, set height, att speed, att height
      
    if column == 'comp_l1':
      result = result[ result['comp_l1'] == value ]
    if column == 'comp_l2':
      result = result[ result['comp_l2'] == value ]
    if column == 'comp_l3':
      result = result[ result['comp_l3'] == value ]
      
    if column == 'start_date':
      result = result[ result['date'] >= value ]
    if column == 'end_date':
      result = result[ result['date'] <= value ]

    # opponent pair for matchu0ps
    if column == 'opp_pair':
      result = result[ result['paira'] == value ) | result['pairb'] == value ]

    # set, these are setup as a radio button, so only one can be chceked.  We pass the 'set' as either 1,2,3, or it is not in the list
    if column == 'set':
      result = result[ result[column] == value ]

    # serving to, this is a list of up to 15 zones
    if column == 'srv_fr':
      result = result[ result['serve_src_zone_net'].isin(value) ]
      
    # serving from, this is a list of 3 zones
    if column == 'srv_to':
      result['srv_to'] = str(result['serve_dest_zone_net'])+result['serve_dest_zone_depth']
      result = result[ result['srv_to'].isin(value) ]

        
    # serve speed
    if column == 'srv_speed_low':
      result = result[ result['srv_speed'] >= value ]
    if column == 'srv_speed_high':
      result = result[ result['srv_speed'] <= value ]

    # pass out of system
    if column == 'pass_oos':
      if value == 0:
        result = result[ result['pass_height'] == value ]
      else:
        result = result[ result['pass_height'] >= 1 ]

    # pass height
    if column == 'pass_ht_low':
      result = result[ result['pass_height'] >= value ]
    if column == 'pass_ht_high':
      result = result[ result['pass_height'] <= value ]
        
    # set height
    if column == 'set_ht_low':
      result = result[ result['set_height'] >= value ]
    if column == 'set_ht_high':
      result = result[ result['set_height'] <= value ]
        
    # set type : bump, hand, unknownn, where only one can be selected
    if column == 'set_touch_type':
      if value == ' unkown':
        result = result[ ( result[column] == value | result[column] == 'empty' ) ]
      else:
        result = result[ result[column] == value ]
        
        
    # att height
    if column == 'att_ht_low':
      result = result[ result['att_height'] >= value ]
    if column == 'att_ht_high':
      result = result[ result['att_height'] <= value ]
        
    # att speed
    if column == 'att_speed_low':
      result = result[ result['att_speed'] >= value ]
    if column == 'att_speed_high':
      result = result[ result['att_speed'] <= value ]
        
        
    #result = result[result[column] == value]
    print(f"Number of Rows: {result.shape[0]}")
    
    #else:
      #print(f"Warning: Column '{column}' not found in DataFrame")
    
  return result

@anvil.server.callable
def unpack_lgy(lgy):
  # unpacks the league/year/gender string into three
  # extract league, gender, year from league selected value
  str_loc = lgy.index("|")
  disp_league = lgy[: str_loc - 1].strip()
  lgy = lgy[str_loc + 1 :]
  str_loc = lgy.index("|")
  disp_gender = lgy[: str_loc - 1].strip()
  disp_year = lgy[str_loc + 1 :].strip()

  return disp_league, disp_gender, disp_year