import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from anvil.pdf import PDFRenderer
from PyPDF2 import PdfMerger
import io
import json
import re
import base64
from datetime import datetime, timedelta, date
from pair_functions import *
from pair_reports import *
from server_functions import *
from scouting_reports import *
from player_reports import *

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

'''

@anvil.server.callable
def generate_pdf_report( rpt_form, report_id):
  # need to look into report data file to make a useful pdf file name
  try:
    # Get report data row
    rpt_data_row = app_tables.report_data.get(report_id=report_id)
    if not rpt_data_row:
      return {'error': f'Report ID {report_id} not found'}

      # Determine file names for PDF and JSON
    if rpt_data_row['title_6'] == 'pair':
      base_name = f"{rpt_data_row['title_10']} {rpt_data_row['title_1']}"
      pdf_file = f"{base_name}.pdf"
      json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    elif rpt_data_row['title_6'] == 'player':
      base_name = f"{rpt_data_row['title_9']} {rpt_data_row['title_1']}"
      pdf_file = f"{base_name}.pdf"
      json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    else:
      base_name = report_id
      pdf_file = f"{base_name}.pdf"
      json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

  
    rpt_pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form,report_id)

  except Exception as e:
    return {'pdf': None, 'json_file_name': None, 'error': f'Error generating report: {str(e)}'}
    
  return rpt_pdf 
'''


@anvil.server.callable
def generate_pdf_report(rpt_form, report_id):
  """
    Generate a PDF report and save all report data as JSON to Google Drive.
    Args:
        rpt_form: Anvil form or string identifier to render as PDF
        report_id: ID of the report in app_tables.report_data
    Returns:
        dict: {'pdf': BlobMedia, 'json_file_name': str, 'error': str or None}
    """
  # Get report data row
  rpt_data_row = app_tables.report_data.get(report_id=report_id)
  if not rpt_data_row:
    return {'error': f'Report ID {report_id} not found'}

  # Determine file names for PDF 
  if rpt_data_row['title_6'] == 'pair':
    base_name = f"{rpt_data_row['title_10'] or 'Pair'} {rpt_data_row['title_1'] or 'Report'}"
    pdf_file = f"{base_name}.pdf"
  elif rpt_data_row['title_6'] == 'player':
    base_name = f"{rpt_data_row['title_9'] or 'Player'} {rpt_data_row['title_1'] or 'Report'}"
    pdf_file = f"{base_name}.pdf"
  elif rpt_data_row['title_6'] == 'league':
    base_name = f"{rpt_data_row['title_4'] or 'League'} {rpt_data_row['title_1'] or 'Report'}"
    pdf_file = f"{base_name}.pdf"
  elif rpt_data_row['title_6'] == 'dashboard':
    base_name = f"{rpt_data_row['title_9'] or 'Dashboard'} {rpt_data_row['title_1'] or 'Report'}"
    pdf_file = f"{base_name}.pdf"
  elif rpt_data_row['title_6'] == 'scouting':
    base_name = f"{rpt_data_row['title_9'] or 'Scouting'} {rpt_data_row['title_1'] or 'Report'}"
    pdf_file = f"{base_name}.pdf"
  elif rpt_data_row['title_6'] == 'diagnostic':
    base_name = f"{rpt_data_row['title_9'] or 'Diagnostic'} {rpt_data_row['title_1'] or 'Report'}"
    pdf_file = f"{base_name}.pdf"
  else:
    base_name = report_id
    pdf_file = f"{base_name}.pdf"

  # Generate PDF
  rpt_pdf = PDFRenderer(filename=pdf_file, landscape=False).render_form(rpt_form, report_id)

  return rpt_pdf


@anvil.server.callable
def generate_json_report(rpt_form, report_id):
  """
    Generate a PDF report and save all report data as JSON to Google Drive.
    Args:
        rpt_form: Anvil form or string identifier to render as PDF
        report_id: ID of the report in app_tables.report_data
    Returns:
        dict: {'pdf': BlobMedia, 'json_file_name': str, 'error': str or None}
    """
    # Get report data row
  rpt_data_row = app_tables.report_data.get(report_id=report_id)
  if not rpt_data_row:
    return {'error': f'Report ID {report_id} not found'}

  # Determine file names for JSON
  if rpt_data_row['title_6'] == 'pair':
    base_name = f"{rpt_data_row['title_10'] or 'Pair'} {rpt_data_row['title_1'] or 'Report'}"
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
  elif rpt_data_row['title_6'] == 'player':
    base_name = f"{rpt_data_row['title_9'] or 'Player'} {rpt_data_row['title_1'] or 'Report'}"
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
  elif rpt_data_row['title_6'] == 'league':
    base_name = f"{rpt_data_row['title_4'] or 'League'} {rpt_data_row['title_1'] or 'Report'}"
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
  elif rpt_data_row['title_6'] == 'dashboard':
    base_name = f"{rpt_data_row['title_9'] or 'Dashboard'} {rpt_data_row['title_1'] or 'Report'}"
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
  elif rpt_data_row['title_6'] == 'scouting':
    base_name = f"{rpt_data_row['title_9'] or 'Scouting'} {rpt_data_row['title_1'] or 'Report'}"
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
  elif rpt_data_row['title_6'] == 'diagnostic':
    base_name = f"{rpt_data_row['title_9'] or 'Diagnostic'} {rpt_data_row['title_1'] or 'Report'}"
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
  else:
    base_name = report_id
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

  # Prepare report data for JSON
  report_data = {
    'report_id': report_id,
    'timestamp': datetime.now().isoformat(),
    'titles': {},
    'labels': {},
    'dataframes': {},
    'images': {}
  }

  # Extract titles (title_1 to title_10)
  for i in range(1, 11):
    title_key = f'title_{i}'
    report_data['titles'][title_key] = rpt_data_row[title_key]

  # Extract labels (label_1 to label_10)
  for i in range(1, 11):
    label_key = f'label_{i}'
    report_data['labels'][label_key] = rpt_data_row[label_key]

    # Extract dataframes (df_1 to df_10)
    for i in range(1, 11):
      df_key = f'df_{i}'
      try:
        df_value = rpt_data_row[df_key]
        if isinstance(df_value, anvil._server.LazyMedia):
          # Get markdown content
          content_type = df_value.content_type
          if ('markdown' in content_type.lower()) or ('plain' in content_type.lower()):
            markdown_bytes = df_value.get_bytes()
            markdown_text = markdown_bytes.decode('utf-8')
            # Try to parse markdown table
            df = parse_markdown_table(markdown_text)
            if df is not None:
              report_data['dataframes'][df_key] = df.to_dict('records')
            else:
              report_data['dataframes'][df_key] = {'raw_markdown': markdown_text}
          else:
            report_data['dataframes'][df_key] = f"Unsupported content type: {content_type}"
        elif isinstance(df_value, pd.DataFrame):
          report_data['dataframes'][df_key] = df_value.to_dict('records')
        elif isinstance(df_value, dict):
          report_data['dataframes'][df_key] = df_value
        elif df_value is not None:
          report_data['dataframes'][df_key] = str(df_value)
        else:
          report_data['dataframes'][df_key] = None
      except KeyError:
        report_data['dataframes'][df_key] = None

    # Extract images (image_1 to image_10)
    for i in range(1, 11):
      img_key = f'image_{i}'
      try:
        img_value = rpt_data_row[img_key]
        if isinstance(img_value, (anvil.BlobMedia, anvil._server.LazyMedia)):
          # Get image bytes and content type
          img_bytes = img_value.get_bytes()
          content_type = img_value.content_type
          # Encode as base64 with data URI
          img_base64 = base64.b64encode(img_bytes).decode('utf-8')
          data_uri = f"data:{content_type};base64,{img_base64}"
          report_data['images'][img_key] = {
            'name': img_value.name,
            'content_type': content_type,
            'data_uri': data_uri
          }
        elif isinstance(img_value, str):
          report_data['images'][img_key] = {'url': img_value}
        else:
          report_data['images'][img_key] = None
      except KeyError:
        report_data['images'][img_key] = None

  # Convert to JSON
  json_str = json.dumps(report_data, indent=2, default=str)
  json_bytes = json_str.encode('utf-8')
  json_media = anvil.BlobMedia(content_type='application/json', content=json_bytes, name=json_file)

  return json_media

def parse_markdown_table(markdown_text):
  """
    Parse a markdown table into a pandas DataFrame.
    Args:
        markdown_text (str): Markdown content containing a table
    Returns:
        pd.DataFrame or None: Parsed DataFrame or None if parsing fails
    """
  try:
    # Split markdown into lines
    lines = markdown_text.strip().split('\n')
    if len(lines) < 2:
      return None

      # Find table headers and separator
    headers = None
    separator_line = None
    for i, line in enumerate(lines):
      if line.strip().startswith('|') and line.strip().endswith('|'):
        if i + 1 < len(lines) and re.match(r'^\|[-:\s\|]+$', lines[i + 1]):
          headers = [h.strip() for h in line.strip('|').split('|')]
          separator_line = i + 1
          break
    if not headers:
      return None

      # Extract table rows
    rows = []
    for line in lines[separator_line + 1:]:
      if line.strip().startswith('|') and line.strip().endswith('|'):
        row = [cell.strip() for cell in line.strip('|').split('|')]
        if len(row) == len(headers):
          rows.append(row)

    if not rows:
      return None

      # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    return df
  except Exception:
    return None


  
    


@anvil.server.callable
def create_pdf(email, text1):
  pdf = PDFRenderer.render_form('Homepage.PDF_Rpt', email, text1)
  return pdf

@anvil.server.callable
def send_pdf_email(email, email_message, pdf ):
  pdf = create_pdf(email, text1 )
  anvil.email.send(
    from_address='no-reply',
    from_name='Beach Internals', 
    to=email, 
    subject='Your PDF Report',
    text='Thanks for being a Beach Internals Partner.  Attached is your PDF Player Report.',
    attachments=pdf
  )
  return pdf  


#---------------------------------------------------------------------
#
#          Render Player Reports as PDF Files
#
#----------------------------------------------------------------------
@anvil.server.callable
def create_pdf_reports(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # call report function
  #print(f'Calling Function:{fnct_name}')
  table_data1, table_data2, table_data3, table_data4, table_data5, table_data6, table_data7, table_data8 = anvil.server.call(fnct_name, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_player + ' ' + report_row['report_name'] 
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
                                table_data1, 
                                table_data2, 
                                table_data3, 
                                table_data4,
                                table_data5,
                                table_data6,                                
                                table_data7,
                                table_data8,
                                filter_text, 
                                report_row['explain_text'], 
                                disp_player, 
                                report_row['report_name'], 
                                report_row['box1_title'], 
                                report_row['box2_title'], 
                                report_row['box3_title'],
                                report_row['box4_title'],
                                report_row['box5_title'],
                                report_row['box6_title'],
                                report_row['box7_title'],
                                report_row['box8_title']
                               )
  return pdf

#---------------------------------------------
# Render Scouting Reports to PDF
#
#.  Needed a seperate function due to the increased nubmer of parameters for all the plots
#
#----------------------------------------------
@anvil.server.callable
def create_scouting_pdf_reports(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, title_text,
                    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                    ):

  # call report function
  #print(f'Calling Function:{fnct_name}')
  serve_strategy_title,table_data,plot1,plot2,plot3,z1_att,z2_att,z3_att,z4_att,z5_att,opt_att,z1_tbl,z2_tbl,z3_tbl,z4_tbl,z5_tbl,opt_tbl = anvil.server.call(
    fnct_name, 
    disp_league, 
    disp_gender, 
    disp_year, 
    disp_team, 
    disp_pair,
    disp_player,
    comp_l1_checked, disp_comp_l1,
    comp_l2_checked, disp_comp_l2,
    comp_l3_checked, disp_comp_l3,
    date_checked, disp_start_date, disp_end_date,
    scout, explain_text, title_text,
    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
  )
                                                                                                                                                                                  

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)

  # define some variable, for now, for consistency across calls
  plot1_title = 'Serves'
  plot2_title = 'Passes'
  plot3_title = 'Sets'
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_player + ' ' + report_row['report_name'] 
  #print(f"filter text : {filter_text}, explain text : {explain_text} report form: {rpt_form}")
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
    disp_pair,
    disp_player,
    serve_strategy_title,
    table_data,
    plot1_title,
    plot1,
    plot2_title,
    plot2,
    plot3_title,
    plot3,
    z1_att,
    z2_att,
    z3_att,
    z4_att,
    z5_att,
    opt_att,
    z1_tbl,
    z2_tbl,
    z3_tbl,
    z4_tbl,
    z5_tbl,
    opt_tbl,
    filter_text,
    explain_text,
    title_text
             )
  
  return pdf


#---------------------------------------------
# Render Scouting Reports to PDF
#
#.  Needed a seperate function for each PDF Report form
#
#----------------------------------------------
@anvil.server.callable
def render_3court_plot_to_pdf(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, title_text,
                    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                    ):

  # call report function
  #print(f'Calling Function:{fnct_name}')
  serve_strategy_title,table_data,srv_plot,pass_plot,set_plot,z1_att,z2_att,z3_att,z4_att,z5_att,opt_att,z1_tbl,z2_tbl,z3_tbl,z4_tbl,z5_tbl,opt_tbl = anvil.server.call(
    fnct_name, 
    disp_league, 
    disp_gender, 
    disp_year, 
    disp_team, 
    disp_pair,
    disp_player,
    comp_l1_checked, disp_comp_l1,
    comp_l2_checked, disp_comp_l2,
    comp_l3_checked, disp_comp_l3,
    date_checked, disp_start_date, disp_end_date,
    scout, explain_text, title_text,
    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
  )
                                                                                                                                                                                  

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_player + ' ' + report_row['report_name'] 
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
    serve_strategy_title,
    disp_player,
    table_data,
    srv_plot,
    pass_plot,
    set_plot,
    z1_att,
    z2_att,
    z3_att,
    z4_att,
    z5_att,
    opt_att,
    z1_tbl,
    z2_tbl,
    z3_tbl,
    z4_tbl,
    z5_tbl,
    opt_tbl,
    disp_pair,
    filter_text,
    explain_text,
    title_text
             )
  
  return pdf
  
@anvil.server.callable
def send_email(email_subj, email_body, email_attachment, email_to, email_from):
  if not email_to:
    email_to = anvil.users.get_user()['email']

  if not email_from:
    email_from = 'no-reply'

  result = anvil.email.send(
    from_address=email_from,
    from_name='Beach Internals', 
    to=email_to,
    subject=email_subj,
    text=email_body,
    attachments = email_attachment
  )

  return result

@anvil.server.callable
def render_all_rpts_pdf_callable(
                    disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, player_pair, user_email
                    ):
  # just kick off the background task to do this
  return_value = anvil.server.launch_background_task('render_all_rpts_pdf_background',
                    disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, player_pair, user_email
                    )

  return return_value
  
@anvil.server.background_task
def  render_all_rpts_pdf_background(
                    disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, player_pair, user_email
                    ):

  # get all the reports out of the table, then loop thruy them all for the disp player
  function_list = [(f_row['function_name']) for f_row in app_tables.report_list.search(tables.order_by("order"),private=False,rpt_type=player_pair)]
  text_list = [(f_row['explain_text']) for f_row in app_tables.report_list.search(private=False,rpt_type=player_pair)]
  form_list = [(f_row['rpt_form']) for f_row in app_tables.report_list.search(private=False,rpt_type=player_pair)]
  print(function_list)
  full_rpt_pdf = None
  pdf_name = disp_player + ' Summary.pdf'

  
  # now loop over the items in the functioj list
  for index, value in enumerate(function_list):
    #print(index,value)
    pdf1 = anvil.server.call('create_pdf_reports', value, form_list[index],
                          disp_league, disp_gender, disp_year, 
                          disp_team, disp_pair, disp_player,
                          comp_l1_checked, disp_comp_l1,
                          comp_l2_checked, disp_comp_l2,
                          comp_l3_checked, disp_comp_l3,
                          date_checked, disp_start_date, disp_end_date,
                          scout, text_list[0]
                          )
    #print(pdf1)
    if pdf1 and full_rpt_pdf:
      #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
      full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
    else:
      #print('no original pdf file, setting to pdf1')
      full_rpt_pdf = pdf1
      #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')

  # now that we are done, send this to the user
  return_value = send_email("Beach Internals - Detailed Report", "Please find the attached full player report.", full_rpt_pdf, user_email, 'no-reply')

  # and, let's write to the Google Drive
  #return_string = save_to_google_drive(full_rpt_pdf)
  #print(return_string)
  
  return return_value


def merge_pdfs( file1, file2, pdf_name):
  # initialize PdfMerger
  merger = PdfMerger()

  # print out key elements
  #print(f' Before io.Bytes(): file1: {file1}, File2: {file2}')
  
  # merge PDFs
  pdf1 = io.BytesIO(file1.get_bytes())
  pdf2 = io.BytesIO(file2.get_bytes())
  #print(f' After io.Bytes(): file1: {file1}, File2: {file2}')
  merger.append(pdf1)
  merger.append(pdf2)
  merged_pdf = io.BytesIO()
  merger.write(merged_pdf)
  merger.close()
  
  return anvil.BlobMedia('application/pdf',merged_pdf.getvalue(), name=pdf_name)

@anvil.server.callable
def save_to_google_drive(file):
   # Ensure the input is an Anvil Media object
   if not isinstance(file, anvil.Media):
       raise ValueError("Expected an Anvil Media object.")

   # Save the file to Google Drive
   google_file = app_files.drive.create_file(file.get_name(), file.get_bytes())
   return f"File '{google_file['name']}' saved to Google Drive!"


#---------------------------------------------------------------------
#
#          Render Player Reports as PDF Files
#
#----------------------------------------------------------------------
@anvil.server.callable
def create_pair_pdf_reports(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # call report function
  #print(f'Calling Function:{fnct_name}')
  table_data1, table_data2, table_data3, table_data4, table_data5, table_data6, table_data7, table_data8 = anvil.server.call(fnct_name, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Pair : {disp_pair}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)

  # get the two players in this pair
  player1, player2 = pair_players(disp_pair)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_pair + ' ' + report_row['report_name'] 
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
                                report_row['report_name'],
                                player1,
                                player2,
                                table_data1, 
                                table_data2, 
                                table_data3, 
                                table_data4,
                                table_data5,
                                table_data6,                                
                                table_data7,
                                table_data8,
                                report_row['box1_title'], 
                                report_row['box2_title'], 
                                report_row['box3_title'],
                                report_row['box4_title'],
                                report_row['box5_title'],
                                report_row['box6_title'],
                                report_row['box7_title'],
                                report_row['box8_title'],
                                filter_text, 
                                report_row['explain_text']
                               )
  return pdf


#---------------------------------------------------------------------
#
#          Render Player Reports as PDF Files
#
#.       pass this 17 variables
#.       returnn 3 to form
#.       work with function_rpt form and smiliar wiht three boxes on screen
#
#----------------------------------------------------------------------
@anvil.server.callable
def create_player_pdf_reports(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # call report function
  #print(f'Calling Function:{fnct_name}')
  table_data1, table_data2, table_data3, table_data4 = anvil.server.call(fnct_name, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_player + ' ' + report_row['report_name'] 
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
                                table_data1, 
                                table_data2, 
                                table_data3, 
                                filter_text, 
                                report_row['explain_text'], 
                                disp_player, 
                                report_row['report_name'], 
                                report_row['box1_title'], 
                                report_row['box2_title'], 
                                report_row['box3_title']
                               )
  return pdf

#---------------------------------------------------------------------
#
#          Render Player Reports as PDF Files
#
#.       pass this 17 variables
#.       returnn 3 to form
#.       work with function_rpt form and smiliar wiht three boxes on screen
#
#----------------------------------------------------------------------
@anvil.server.callable
def create_dashboard_pdf_reports(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # call report function
  #print(f'Calling Function:{fnct_name}')
  table_data1, table_data2, table_data3 = anvil.server.call(fnct_name, disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_team + ' ' + report_row['report_name'] 
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
                                table_data1, 
                                table_data2, 
                                table_data3, 
                                filter_text, 
                                report_row['explain_text'], 
                                disp_team, 
                                report_row['report_name'], 
                                report_row['box1_title'], 
                                report_row['box2_title'], 
                                report_row['box3_title']
                               )
  return pdf

#---------------------------------------------------------------------
#
#          Render Player Reports as PDF Files
#
#----------------------------------------------------------------------
@anvil.server.callable
def create_matchup_pdf_reports(
      fnct_name,
      rpt_form,
      disp_league,
      disp_gender,
      disp_year,
      disp_pair_a,
      disp_pair_b, 
      disp_team 
):


  # call report function
  #print(f'Calling Function:{fnct_name}')
  table_data1, table_data2, table_data3, table_data4, table_data5, table_data6, table_data7, table_data8, plt1, plt2, plt3, plt4, plt5, plt6 = anvil.server.call(fnct_name, disp_league, disp_gender, disp_year, disp_pair_a, disp_pair_b, disp_team )

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Pair A : {disp_pair_a}
    - Pair B : {disp_pair_b}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)

  # get the two players in this pair
  #player1, player2 = pair_players(disp_pair)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_pair_a+' v ' + disp_pair_b + ' ' + report_row['report_name'] 
  #print(f"pdf file name: {pdf_file}")
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
                                report_row['report_name'],
                                disp_pair_a,
                                disp_pair_b,
                                table_data1, 
                                table_data2, 
                                table_data3, 
                                table_data4,
                                table_data5,
                                table_data6,                                
                                table_data7,
                                table_data8,
                                plt1,
                                plt2,
                                plt3,
                                plt4,
                                plt5,
                                plt6,
                                report_row['box1_title'], 
                                report_row['box2_title'], 
                                report_row['box3_title'],
                                report_row['box4_title'],
                                report_row['box5_title'],
                                report_row['box6_title'],
                                report_row['box7_title'],
                                report_row['box8_title'],
                                filter_text, 
                                report_row['explain_text']
                               )
  return pdf