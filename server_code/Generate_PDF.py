import anvil.secrets
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
from logger_utils import log_info, log_error, log_critical, log_debug


# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

@anvil.server.callable
def generate_pdf_report(rpt_form, report_id):
  """
    Generate a PDF report and return it as a BlobMedia object.
    Args:
        rpt_form: Anvil form or string identifier to render as PDF
        report_id: ID of the report in app_tables.report_data
    Returns:
        dict: {'pdf': BlobMedia, 'json_file_name': str or None, 'error': str or None}
    """


  # Get report data row
  rpt_data_row = app_tables.report_data.get(report_id=report_id)
  if not rpt_data_row:
    log_error(f"Report ID {report_id} not found")
    return {'pdf': None, 'json_file_name': None, 'error': f'Report ID {report_id} not found'}

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
  try:
    rpt_pdf = PDFRenderer(filename=pdf_file, landscape=False).render_form(rpt_form, report_id)
    log_info(f"PDF generated: type={type(rpt_pdf)}, content_type={getattr(rpt_pdf, 'content_type', 'Unknown')}")

    # Convert StreamingMedia to BlobMedia if necessary
    if isinstance(rpt_pdf, anvil._serialise.StreamingMedia):
      log_info(f"Converting StreamingMedia to BlobMedia for report_id {report_id}")
      rpt_pdf = anvil.BlobMedia('application/pdf', rpt_pdf.get_bytes(), name=pdf_file)

    return {'pdf': rpt_pdf, 'json_file_name': None, 'error': None}

  except Exception as e:
    log_error(f"Error generating PDF for report_id {report_id}: {str(e)}")
    return {'pdf': None, 'json_file_name': None, 'error': f'Failed to generate PDF: {str(e)}'}



def safe_get(obj, key, default=None):
  """Safely get a key from a dict or attribute from Anvil Row/LiveObjectProxy."""
  log_msg = f"Safe Get Called, obj type={type(obj)}, key={key}"
  try:
    log_msg += f", obj[key] type={type(obj[key])}"
  except KeyError:
    log_msg += f", KeyError (key not found)"
  log_debug(log_msg)

  # Handle dict objects
  if isinstance(obj, dict):
    value = obj.get(key, default)
    log_debug(f"safe_get dict: key={key}, value={value} (type: {type(value)})")
    return value

    # Handle tables.Row (or LiveObjectProxy wrapping a Row)
  try:
    # Check if obj is a tables.Row or LiveObjectProxy
    is_row = isinstance(obj, tables.Row) or str(type(obj)).find('LiveObjectProxy') != -1
    if is_row:
      try:
        raw_value = obj[key]  # Access like a dict for tables.Row
        log_debug(f"safe_get Row: key={key}, raw_value type={type(raw_value)}")

        if raw_value is None:
          log_debug(f"NoneType: key={key}, value=None")
          value = None
        elif isinstance(raw_value, str):
          log_debug(f"String: key={key}, value={raw_value[:50]}...")
          value = raw_value
        elif isinstance(raw_value, float):
          log_debug(f"Float: key={key}, value={raw_value}")
          value = raw_value
        elif isinstance(raw_value, anvil.media.LazyMedia):
          log_debug(f"LazyMedia: key={key}, content_type={raw_value.content_type}")
          if 'json' in raw_value.content_type.lower() or 'text' in raw_value.content_type.lower():
            try:
              json_str = raw_value.get_str()
              log_debug(f"JSON preview: {json_str[:100]}...")
              data = json.loads(json_str)
              if not isinstance(data, list):
                raise ValueError(f"JSON for {key} is not a list of records: {type(data)}")
              value = pd.DataFrame(data)
              log_debug(f"Converted LazyMedia to DataFrame: shape={value.shape}")
            except json.JSONDecodeError as e:
              log_debug(f"JSON decode error for {key}: {e}")
              raise ValueError(f"Failed to parse JSON for {key}: {e}")
          else:
            raise ValueError(f"Unsupported LazyMedia content_type for {key}: {raw_value.content_type}")
        else:
          log_debug(f"Other type: key={key}, type={type(raw_value)}")
          value = raw_value

        return value
      except KeyError:
        log_debug(f"KeyError: key={key} not found in Row")
        return default
    else:
      # Non-Row objects
      value = getattr(obj, key, default)
      log_debug(f"safe_get getattr: key={key}, value={value} (type: {type(value)})")
      return value
  except Exception as e:
    log_debug(f"safe_get error: key={key}, error={e}")
    return default


def strip_nulls_safe(obj, path="root"):
  """Recursively set None for null/empty strings, keeping structure."""
  if isinstance(obj, dict):
    new_obj = {}
    for k, v in obj.items():
      full_path = f"{path}.{k}"
      if v is None or (isinstance(v, str) and v.strip() == ""):
        new_obj[k] = None
      else:
        new_obj[k] = strip_nulls_safe(v, full_path)
    return new_obj
  elif isinstance(obj, list):
    return [strip_nulls_safe(item, f"{path}[{i}]") for i, item in enumerate(obj)]
  else:
    return obj

# -----------------------------------------------------------------------------
# NEW FUNCTION: Add this after the strip_nulls_safe function (around line 168)
# -----------------------------------------------------------------------------

def strip_urls_safe(obj, path="root"):
  """
  Recursively strip URLs from strings in a data structure.
  Replaces URL-like strings with empty string or removes them entirely.
  
  Args:
    obj: The object to process (dict, list, or primitive)
    path: Current path for debugging (default: "root")
    
  Returns:
    The processed object with URLs removed
  """
  import re

  # URL detection pattern - matches http://, https://, and www. URLs
  url_pattern = r'https?://[^\s]+|www\.[^\s]+'

  if isinstance(obj, dict):
    new_obj = {}
    for k, v in obj.items():
      full_path = f"{path}.{k}"
      new_obj[k] = strip_urls_safe(v, full_path)
    return new_obj

  elif isinstance(obj, list):
    return [strip_urls_safe(item, f"{path}[{i}]") for i, item in enumerate(obj)]

  elif isinstance(obj, str):
    # Check if the string contains a URL
    if re.search(url_pattern, obj):
      # Option 1: Replace URL with empty string (keeps the field)
      cleaned = re.sub(url_pattern, '', obj).strip()
      return cleaned if cleaned else None

      # Option 2: Return None to remove the field entirely (uncomment to use)
      # return None
    return obj

  else:
    return obj
    
# -----------------------------------------------------------------------------
# Generate JSON from report_data_row
# -----------------------------------------------------------------------------
def generate_json_report(rpt_form, report_id, include_images=False, include_urls=False, include_nulls=True):
  """
  Generate a JSON report from report_data table.

  Args:
    rpt_form: Report form identifier
    report_id: ID of the report in app_tables.report_data
    include_images: If True, include base64-encoded images (default: False)
    include_urls: If True, keep URLs in the data; if False, strip them (default: False)
    include_nulls: If True, keep null/empty values; if False, strip them (default: True)
    
  Returns:
    tuple: (json_media, error_message)
      - json_media: anvil.BlobMedia object on success, None on error
      - error_message: None on success, error string on error
  """
  try:
    # Get report row
    rpt_data_row = tables.app_tables.report_data.get(report_id=report_id)
    if not rpt_data_row:
      log_error(f"Report ID {report_id} not found", with_traceback=False)
      return (None, f'Report ID {report_id} not found')

    # Determine file name for JSON safely from report_data_row
    # Add None-safe handling for all title fields
    rpt_type = rpt_data_row['title_6']
    rpt_type = rpt_type.strip() if rpt_type is not None else 'unknown'

    if rpt_type == 'player':
      base_name = rpt_data_row['title_9']
      base_name = base_name if base_name is not None else 'Unknown_Player'
    elif rpt_type == 'league':
      base_name = rpt_data_row['title_4']
      base_name = base_name if base_name is not None else 'Unknown_League'
    elif rpt_type == 'pair':
      base_name = rpt_data_row['title_10']
      base_name = base_name if base_name is not None else 'Unknown_Pair'
    elif rpt_type == 'dashboard':
      base_name = rpt_data_row['title_9']
      base_name = base_name if base_name is not None else 'Unknown_Dashboard'
    elif rpt_type == 'scouting':
      base_name = rpt_data_row['title_9']
      base_name = base_name if base_name is not None else 'Unknown_Scouting'
    else:
      base_name = 'Unknown'

    # Strip whitespace if base_name is a string
    base_name = base_name.strip() if isinstance(base_name, str) else str(base_name)

    # Construct JSON file name
    json_file = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    # Prepare report_data dictionary
    report_data = {
      'report_id': report_id,
      'timestamp': datetime.now().isoformat(),
      'titles': {f'title_{i}': rpt_data_row[f'title_{i}'] for i in range(1, 11)},
      'labels': {f'label_{i}': rpt_data_row[f'label_{i}'] for i in range(1, 11)},
      'dataframes': {},
      'dataframe_descriptions': {f'df_desc_{i}': rpt_data_row[f'df_desc_{i}'] for i in range(1, 11)},
      'images': {},
      'image_descriptions': {f'image_desc_{i}': rpt_data_row[f'image_desc_{i}'] for i in range(1, 11)}
    }

    # Extract dataframes
    for i in range(1, 11):
      df_key = f'df_{i}'
      if isinstance(rpt_data_row[df_key], type(None)):
        report_data['dataframes'][df_key] = None
      else:
        try:
          df_bytes = rpt_data_row[df_key].get_bytes()
          df_str = df_bytes.decode("utf-8")

          # Try multiple parsing strategies
          df = None
          parse_error = None

          # Strategy 1: Standard pipe-delimited with error handling
          try:
            df = pd.read_csv(
              io.StringIO(df_str),
              sep="|",
              engine="python",
              on_bad_lines='warn'  # Changed from error to warn
            )
          except Exception as e1:
            parse_error = str(e1)
            log_debug(f"Strategy 1 failed for {df_key}: {parse_error}")

            # Strategy 2: Try with skipinitialspace
            try:
              df = pd.read_csv(
                io.StringIO(df_str),
                sep="|",
                engine="python",
                skipinitialspace=True,
                on_bad_lines='skip'  # Skip bad lines instead of erroring
              )
            except Exception as e2:
              log_debug(f"Strategy 2 failed for {df_key}: {str(e2)}")

              # Strategy 3: Manual parsing from markdown
              try:
                lines = df_str.strip().split('\n')
                if len(lines) >= 2:
                  # Get headers from first line
                  headers = [h.strip() for h in lines[0].split('|') if h.strip()]

                  # Skip separator line (usually line 1 with :--- patterns)
                  data_lines = []
                  for line in lines[1:]:
                    # Skip separator lines
                    if re.match(r'^[\s|:-]+$', line):
                      continue
                    parts = [p.strip() for p in line.split('|') if p.strip() or p == '']
                    if len(parts) == len(headers):
                      data_lines.append(parts)

                  if data_lines:
                    df = pd.DataFrame(data_lines, columns=headers)
                  else:
                    log_warning(f"No valid data lines found in {df_key}")
                    report_data['dataframes'][df_key] = None
                    continue
              except Exception as e3:
                log_error(f"All parsing strategies failed for {df_key}: {str(e3)}")
                report_data['dataframes'][df_key] = None
                continue

          if df is not None:
            # Cleanup: strip whitespace from col names + values
            df = df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

            # Drop empty cols
            df = df.dropna(axis=1, how="all")

            # Drop markdown alignment rows like ":---", "---", ":---:"
            if len(df) > 0:
              mask = df.map(lambda x: bool(re.fullmatch(r":?-+:?", str(x).strip())))
              df = df[~mask.all(axis=1)].reset_index(drop=True)

            # Convert to JSON
            report_data['dataframes'][df_key] = df.to_dict(orient="records")
          else:
            report_data['dataframes'][df_key] = None

        except Exception as e:
          log_error(f"Error processing dataframe {df_key}: {str(e)}")
          report_data['dataframes'][df_key] = None

    # Extract images
    for i in range(1, 11):
      img_key = f'image_{i}'
      if isinstance(rpt_data_row[img_key], type(None)):
        report_data['images'][img_key] = None
      else:
        img_value = rpt_data_row[img_key]
        if include_images and isinstance(img_value, (anvil.BlobMedia, anvil._server.LazyMedia)):
          img_bytes = img_value.get_bytes()
          content_type = img_value.content_type
          img_base64 = base64.b64encode(img_bytes).decode('utf-8')
          report_data['images'][img_key] = {
            'name': img_value.name,
            'content_type': content_type,
            'data_uri': f"data:{content_type};base64,{img_base64}"
          }

    # Optionally strip nulls
    if not include_nulls:
      log_debug("Stripping nulls from report_data")
      report_data = strip_nulls_safe(report_data)

    # Convert to JSON media
    json_str = json.dumps(report_data, indent=2, default=str)
    json_bytes = json_str.encode('utf-8')
    json_media = anvil.BlobMedia(content_type='application/json', content=json_bytes, name=json_file)

    return (json_media, None)  # SUCCESS: return media and no error

  except Exception as e:
    log_critical(f"CRITICAL ERROR in generate_json_report: {e}")
    return (None, str(e))  # ERROR: return None and error message
    








  



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
      full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name=pdf_name)
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

def merge_pdfs(*files, pdf_name: str = None) -> anvil.BlobMedia:
  """
    Merge multiple PDF files into a single PDF.

    Args:
        *files: Variable number of anvil.BlobMedia or anvil._serialise.StreamingMedia objects representing PDFs.
        pdf_name (str): Name for the resulting merged PDF file.

    Returns:
        anvil.BlobMedia: Merged PDF as a BlobMedia object.

    Raises:
        ValueError: If fewer than two PDFs are provided or if inputs are not valid PDF media objects.
        Exception: For PDF merging errors (e.g., corrupted files).
    """
  # Input validation
  if len(files) < 2:
    raise ValueError("At least two PDF files are required for merging.")

  validated_files = []
  for file in files:
    # Log the type and content type of each file
    log_info(f"Processing file: type={type(file)}, content_type={getattr(file, 'content_type', 'Unknown')}")

    # Handle StreamingMedia or BlobMedia
    if isinstance(file, (anvil.BlobMedia, anvil._serialise.StreamingMedia)):
      content_type = getattr(file, 'content_type', None)
      if content_type != 'application/pdf':
        log_info(f"File has incorrect content_type: {content_type}. Attempting to treat as PDF.")
      validated_files.append(file)
    else:
      raise ValueError(f"Invalid input: {file} is not a valid PDF media object (type={type(file)}).")

    # Initialize PdfMerger
  merger = PdfMerger()
  merged_pdf = io.BytesIO()

  try:
    # Append each PDF to the merger
    for file in validated_files:
      pdf_bytes = file.get_bytes() if hasattr(file, 'get_bytes') else file.read()
      pdf_stream = io.BytesIO(pdf_bytes)
      merger.append(pdf_stream)

      # Write the merged PDF to the output stream
    merger.write(merged_pdf)
    merged_pdf.seek(0)  # Reset stream position for reading

    # Create and return BlobMedia object
    return anvil.BlobMedia('application/pdf', merged_pdf.getvalue(), name=pdf_name)

  except Exception as e:
    log_error(f"Error merging PDFs: {str(e)}")
    raise Exception(f"Failed to merge PDFs: {str(e)}")

  finally:
    merger.close()
    merged_pdf.close()
    



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