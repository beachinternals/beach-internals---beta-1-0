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

@anvil.server.callable
def generate_and_store_report( fnct_name ):
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

  label_list, image_list, df_list = anvil.server.call( fnct_name, d_league='NCAA', d_gender='W', d_year='2025' )

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
def report_test( **kwargs):
  for key, value in kwargs.items():
    print(f"{key} = {value}")
    
  # a test/dummy stub for reports
  label_list = []
  image_list = []
  df_list = []

  label_list.append('Label 0')
  #print(f"Label list: {label_list}, length : {len(label_list)}") 
  label_list.append('Label 1')
  #print(f"Label list: {label_list}, length : {len(label_list)}") 
  label_list.append('Label 2')
  #print(f"Label list: {label_list}, length : {len(label_list)}") 
  label_list.append('Label 3')
  #print(f"Label list: {label_list}, length : {len(label_list)}") 
  
  return label_list, image_list, df_list