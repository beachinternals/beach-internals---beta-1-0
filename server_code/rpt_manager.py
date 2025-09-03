import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
import anvil.server
from Generate_PDF import *
from pair_functions import *
from server_functions import *
import pandas as pd
#from matchup_reports import *
from report_generate_and_store import *
from datetime import datetime, timedelta, date
import json
import base64
import re
from typing import Tuple
from collections import defaultdict

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

@anvil.server.callable
def rpt_mgr_generate():
  # client callable functiom to call the background function to generate reports
  #logger.info(" Report Manager  - Generate Called")
  #test = rpt_mgr_generate_background()
  anvil.server.launch_background_task('rpt_mgr_generate_background')
  return
  
#--------------------------------------------------------------
#
#. Report Manager - Generate reports based on the rpt_mgr data file
#
#---------------------------------------------------------------
@anvil.server.background_task
def rpt_mgr_generate_background():
  # Generate reports from the report mgt data file
  now = datetime.now()
  email_text = 'Report Manager Started at ' + str(now) + ' \n \n'
  print(f"Report Manager Started at {now}")

  print("Checking table access...")
  try:
    # Access app_tables through the tables module
    app_tables = tables.app_tables
    #print(f"app_tables type: {type(app_tables)}")
    #print(f"app_tables dir: {dir(app_tables)}")

    # Try to access rpt_mgr table
    rpt_rows = app_tables.rpt_mgr.search(active="Yes")
    rpt_rows_list = list(rpt_rows)
    print(f"SUCCESS: Found {len(rpt_rows_list)} active reports in rpt_mgr table")

  except AttributeError as e:
    print(f"AttributeError: {e}")
    # Try alternative access method
    try:
      print("Trying alternative access...")
      rpt_mgr_table = getattr(tables.app_tables, 'rpt_mgr', None)
      if rpt_mgr_table:
        rpt_rows = rpt_mgr_table.search(active="Yes")
        rpt_rows_list = list(rpt_rows)
        print(f"SUCCESS via getattr: Found {len(rpt_rows_list)} active reports")
      else:
        print("rpt_mgr table not found via getattr either")
        # List all available tables
        print("Available app tables:")
        for attr in dir(tables.app_tables):
          if not attr.startswith('_'):
            print(f"  - {attr}")
        return False
    except Exception as e2:
      print(f"Alternative access also failed: {e2}")
      return False
  except Exception as e:
    print(f"Other error: {e}")
    return False

  if not rpt_rows_list:
    print("No active reports found in rpt_mgr table")
    email_text += "No active reports found in rpt_mgr table\n"
    return False
    

    # Debug: Print first report structure for verification
  '''
  if rpt_rows_list:
    first_report = rpt_rows_list[0]
    print(f"First report structure check:")
    print(f"  Description: {first_report['Report Description']}")
    print(f"  Type: {first_report['rpt_type']}")
    print(f"  Team: {first_report['team']}")
    print(f"  DOW: {first_report['dow']}")
    print(f"  Email: {first_report['email']}")

    # Check rpts_inc structure
    if first_report['rpts_inc']:
      rpts_inc_list = list(first_report['rpts_inc'])
      print(f"  rpts_inc count: {len(rpts_inc_list)}")
      if rpts_inc_list:
        first_rpt_inc = rpts_inc_list[0]
        print(f"  First rpts_inc keys: {list(first_rpt_inc.keys()) if hasattr(first_rpt_inc, 'keys') else 'Not a dict-like object'}")
    else:
      print(f"  rpts_inc is empty or None")
  '''

    # Collect user report infos for batched emails
  user_reports = defaultdict(list)
  processed_count = 0
  total_reports = len(rpt_rows_list)

  # Open the data file, loop over rows
  for report_index, rpt_r in enumerate(rpt_rows_list):
    print(f"\n=== Processing report {report_index + 1} of {total_reports} ===")

    try:
      disp_team = rpt_r['team']
      #print(f"Report Description: {rpt_r['Report Description']}")
      #print(f"Report type: {rpt_r['rpt_type']}")
      #print(f"Team: {disp_team}")

      # Check if this report should be run today
      today = datetime.now()
      day_of_week = today.strftime("%A")
      #print(f"Today is: {day_of_week}")
      #print(f"Report scheduled for: {rpt_r['dow']}")

      if rpt_r['dow'] not in [day_of_week, 'Everyday']:
        print(f"SKIPPING: Report not scheduled for {day_of_week}")
        print(f"rpt_mgr_generate_background: Report {rpt_r['rpt_type']} not scheduled for {day_of_week}")
        continue

      #print(f"Report IS scheduled to run today")

      # Process different report types
      if rpt_r['rpt_type'] in ['player', 'pair', 'league', 'dashboard']:
        input_list = rpt_r['player_list'] if rpt_r['rpt_type'] != 'pair' else rpt_r['pair_list']

        print(f"Input list type: {type(input_list)}")
        if input_list:
          input_list_items = list(input_list)
          #print(f"Input list count: {len(input_list_items)}")
          if input_list_items:
            print(f"First input item keys: {list(input_list_items[0].keys()) if hasattr(input_list_items[0], 'keys') else 'Not dict-like'}")
        else:
          print(f"Input list is None or empty")
          email_text += f"No input list for {rpt_r['rpt_type']} report: {rpt_r['Report Description']}\n"
          continue

        print(f"Calling rpt_mgr_new_rpts with rpt_type={rpt_r['rpt_type']}")
        email_text += f"\n Processing {rpt_r['rpt_type']} Reports \n"

        try:
          result = rpt_mgr_new_rpts(rpt_r, input_list, disp_team)

          if result is None:
            print(f"ERROR: rpt_mgr_new_rpts returned None for {rpt_r['rpt_type']}")
            print(f"Report Manager: rpt_mgr_new_rpts Failed, {rpt_r['rpt_type']}, ret_val=None, report_infos=[]")
            email_text += f"Failed to process {rpt_r['rpt_type']} reports: returned None\n"
            continue

          ret_val, report_infos = result
          print(f"rpt_mgr_new_rpts returned successfully")
          #print(f"  ret_val: '{ret_val}'")
          #print(f"  report_infos count: {len(report_infos) if report_infos else 0}")

          if not ret_val and not report_infos:
            print(f"ERROR: rpt_mgr_new_rpts failed for {rpt_r['rpt_type']} - no output")
            print(f"Report Manager: rpt_mgr_new_rpts Failed, {rpt_r['rpt_type']}, ret_val={ret_val}, report_infos={report_infos}")
            email_text += f"Failed to process {rpt_r['rpt_type']} reports: no output generated\n"
          else:
            email_text += ret_val + '\n'
            processed_count += 1
            print(f"SUCCESS: Processed {rpt_r['rpt_type']} reports")

          if rpt_r['email'] and report_infos:
            user_reports[rpt_r['email']].extend(report_infos)
            #print(f"Added {len(report_infos)} reports to email queue for {rpt_r['email']}")

        except Exception as e:
          print(f"ERROR: Exception in rpt_mgr_new_rpts: {str(e)}")
          print(f"rpt_mgr_generate_background: Error processing {rpt_r['rpt_type']}: {str(e)}")
          email_text += f"Error processing {rpt_r['rpt_type']} reports: {str(e)}\n"

      elif rpt_r['rpt_type'] == 'scouting':
        pair_list = rpt_r['pair_list']
        

        print(f"Scoting Reports, Pair list type: {type(pair_list)} pair_list :  \n {pair_list}")
        if pair_list:
          #pair_list_items = list(pair_list)
          pair_list_items = [row['pair'] for row in pair_list]
          #print(f"Pair list count: {len(pair_list_items)}, Pair List Items: {pair_list_items}")
          if pair_list_items:
            print(f"First pair item keys: {list(pair_list_items[0].keys()) if hasattr(pair_list_items[0], 'keys') else 'Not dict-like'}")
        else:
          print(f"Pair list is None or empty")
          email_text += f"No pair list for scouting report: {rpt_r['Report Description']}\n"
          continue

        print(f"Calling rpt_mgr_scouting_rpts with pair_list")
        email_text += '\n Processing scouting Reports \n'

        try:
          result = rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team)

          if result is None:
            print(f"ERROR: rpt_mgr_scouting_rpts returned None")
            print(f"Report Manager: rpt_mgr_scouting_rpts Failed, ret_val=None, report_infos=[]")
            email_text += f"Failed to process scouting reports: returned None\n"
            continue

          ret_val, report_infos = result
          print(f"rpt_mgr_scouting_rpts returned successfully")
          #print(f"  ret_val: '{ret_val}'")
          #print(f"  report_infos count: {len(report_infos) if report_infos else 0}")

          if not ret_val and not report_infos:
            print(f"ERROR: rpt_mgr_scouting_rpts failed - no output")
            print(f"Report Manager: rpt_mgr_scouting_rpts Failed, ret_val={ret_val}, report_infos={report_infos}")
            email_text += f"Failed to process scouting reports: no output generated\n"
          else:
            email_text += ret_val + '\n'
            processed_count += 1
            print(f"SUCCESS: Processed scouting reports")

          if rpt_r['email'] and report_infos:
            user_reports[rpt_r['email']].extend(report_infos)
            #print(f"Added {len(report_infos)} reports to email queue for {rpt_r['email']}")

        except Exception as e:
          print(f"ERROR: Exception in rpt_mgr_scouting_rpts: {str(e)}")
          print(f"rpt_mgr_generate_background: Error processing scouting: {str(e)}")
          email_text += f"Error processing scouting reports: {str(e)}\n"

      else:
        print(f"ERROR: Invalid Report Type: {rpt_r['rpt_type']}")
        print(f"rpt_mgr_generate_background: Invalid Report Type: {rpt_r['rpt_type']}")
        email_text += f"Invalid Report Type: {rpt_r['rpt_type']}\n"

    except Exception as e:
      print(f"ERROR: Exception processing report {report_index + 1}: {str(e)}")
      print(f"rpt_mgr_generate_background: Error processing report: {str(e)}")
      email_text += f"Error processing report {rpt_r.get('Report Description', 'Unknown')}: {str(e)}\n"

  print(f"\n=== SUMMARY ===")
  print(f"Total reports found: {total_reports}")
  print(f"Reports processed successfully: {processed_count}")
  print(f"Users to receive emails: {len(user_reports)}")

  # Send batched user emails
  for user_email, reports in user_reports.items():
        try:
            print(f"Sending email to {user_email} with {len(reports)} reports")
            email_subject = "Beach Internals - Your Generated Reports"
            email_body = f"""
Dear Coach,

The following reports have been generated and are available on Google Drive:

Generated on: {today.strftime('%Y-%m-%d %H:%M:%S')}

"""
            for info in reports:
                email_body += f"""{info['combined']}
"""
            email_body +="""
Best regards,
Beach Internals
info@BeachInternals.com
"""

            email_status = anvil.email.send(
                to=user_email,
                from_address="no-reply",
                subject=email_subject,
                text=email_body
            )
            print(f"Email sent to {user_email}: {email_status}")
            
        except Exception as e:
            print(f"ERROR: Failed to send email to {user_email}: {str(e)}")

  # Last thing, empty the report_data table when we are done
  print("Clearing report_data table...")
  email_text += '\nDeleting all rows from report_data table \n\n'
  try:
    app_tables.report_data.delete_all_rows()
    print("Successfully cleared report_data table")
  except Exception as e:
    print(f"ERROR: Failed to clear report_data table: {str(e)}")

  # Send summary email
  internals_email = 'info@beachinternals.com'
  now1 = datetime.now()
  compute_time = now1 - now
  email_message = email_text + f"Report Manager Completed at: {now1}\nCompute time: {compute_time}\n"
    
  print(f"Sending summary email to {internals_email}")
  try:
    email_status = anvil.email.send(
        to=internals_email,
        from_address="no-reply",
        subject='Beach Internals - Report Manager',
        text=email_message
    )
    print(f"Summary email sent successfully: {email_status}")
  except Exception as e:
    print(f"ERROR: Failed to send summary email: {str(e)}")

  print(f"Report Manager completed. Total runtime: {compute_time}")
  return True

  

  







#-------------------------------------------------------------------------------------------------------
#  Report Manager - All Types of Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_new_rpts(rpt_r, p_list, disp_team):
  '''
    Using the new format, where we store filters in **rpt_filters and then store the data in a file,
    then call the pdf window to get data from the file.

    Args:
        rpt_r: Report configuration with rpts_inc containing DataTableRow objects
        p_list: List of players or pairs
        disp_team: Team name
    
    Returns:
        tuple: (return_text, report_infos)
    '''
  today = datetime.now()
  return_text = ''
  report_infos = []

  print(f"=== ENTERING rpt_mgr_new_rpts ===")
  print(f"rpt_type: {rpt_r['rpt_type']}")
  #print(f"disp_team: {disp_team}")
  #print(f"p_list type: {type(p_list)}")
  #print(f"p_list length: {len(p_list) if p_list else 'None'}")

  if not p_list:
    print("ERROR: No players/pairs provided")
    return return_text, report_infos

  try:
    for i, p in enumerate(p_list):
      print(f"\n--- Processing item {i+1} of {len(p_list)} ---")
      #print(f"Item type: {type(p)}")

      # Try to convert to dict to see the structure
      try:
        p_dict = dict(p)
        #print(f"Item as dict: {p_dict}")
        #print(f"Available keys: {list(p_dict.keys())}")
      except Exception as e:
        print(f"Could not convert item to dict: {str(e)}")

        # Try to access individual fields we expect
        try:
          #print(f"Trying to access expected fields:")
          if hasattr(p, '__getitem__'):
            try:
              league = p['league']
              print(f"  league: {league}")
            except:
              print(f"  league: not accessible")
            try:
              gender = p['gender']
              print(f"  gender: {gender}")
            except:
              print(f"  gender: not accessible")
            try:
              year = p['year']
              print(f"  year: {year}")
            except:
              print(f"  year: not accessible")
        except Exception as e2:
          print(f"  Could not access any fields: {str(e2)}")
        continue

        # Build the report filters
      print(f"Calling populate_filters_from_rpt_mgr_table...")
      try:
        rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, p)
        print(f"Report filters: {rpt_filters}")
      except Exception as e:
        print(f"ERROR: Failed to populate filters: {str(e)}")
        continue

        # Calculate the folder we will store these into
      try:
        pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), 
                      disp_team.strip(), today.strftime("%Y-%m-%d")]
        json_folder = pdf_folder + ['json']
        lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']
        #print(f"PDF folder: {pdf_folder}")
        #print(f"JSON folder: {json_folder}")
        #print(f"lgy: {lgy}")
      except Exception as e:
        print(f"ERROR: Failed to create folder paths: {str(e)}")
        continue

        # Determine player_pair based on report type
      try:
        if rpt_r['rpt_type'] == 'player':
          #if not all(k in p for k in ['team', 'number', 'shortname']):
          #  print(f"ERROR: Invalid player entry, missing required keys: {list(p.keys()) if hasattr(p, 'keys') else 'unknown keys'}")
          #  continue
          player_pair = p['team'] + " " + p['number'] + ' ' + p['shortname']
        elif rpt_r['rpt_type'] == 'pair':
          #if 'pair' not in p:
          #  print(f"ERROR: Invalid pair entry, missing 'pair' key")
          #  continue
          player_pair = p['pair']
        elif rpt_r['rpt_type'] in ['league', 'dashboard']:
          player_pair = lgy
        else:
          print(f"ERROR: Unknown rpt_type: {rpt_r['rpt_type']}")
          player_pair = 'Unknown'

        print(f"player_pair: {player_pair}")
      except Exception as e:
        print(f"ERROR: Failed to determine player_pair: {str(e)}")
        continue

      pdf_name = f"{player_pair} {rpt_r['Report Description']}.pdf"
      print(f"PDF name: {pdf_name}")

      # Process rpts_inc
      print(f"Processing rpts_inc...")
      #print(f"rpts_inc type: {type(rpt_r['rpts_inc'])}")
      #print(f"rpts_inc value: {rpt_r['rpts_inc']}")

      rptname_rows = []
      rpts_inc = rpt_r['rpts_inc'] or []

      if rpts_inc:
        print(f"rpts_inc has {len(list(rpts_inc))} items")
        for j, rptname1 in enumerate(rpts_inc):
          #print(f"  Processing rpts_inc item {j+1}:")
          #print(f"    Type: {type(rptname1)}")

          try:
            rptname1_dict = dict(rptname1) if rptname1 else {}
            #print(f"    As dict: {rptname1_dict}")
            #print(f"    Keys: {list(rptname1_dict.keys())}")
          except Exception as e:
            print(f"    Could not convert to dict: {str(e)}")
            continue

            # Check required fields
          required_fields = ['id', 'report_name', 'rpt_type', 'rpt_form', 'function_name']
          missing_fields = []
          for field in required_fields:
            if field not in rptname1_dict:
              missing_fields.append(field)

          if missing_fields:
            print(f"    SKIPPING: Missing fields: {missing_fields}")
            continue
          else:
            rptname_rows.append(rptname1)
            print(f"    ADDED: id={rptname1_dict['id']}, name={rptname1_dict['report_name']}")
      else:
        print(f"rpts_inc is empty or None")

      if not rptname_rows:
        print(f"ERROR: No valid reports in rpts_inc for {rpt_r['Report Description']}")
        continue

      print(f"Found {len(rptname_rows)} valid reports to process")

      # Sort reports by order
      try:
        sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)
        print(f"Sorted reports: {[r['report_name'] for r in sorted_rptnames]}")
      except Exception as e:
        print(f"ERROR: Failed to sort reports: {str(e)}")
        sorted_rptnames = rptname_rows

      full_rpt_pdf = None
      pdf_files_created = []

      # Process each report
      for k, rptname in enumerate(sorted_rptnames):
        print(f"\n  --- Processing report {k+1}/{len(sorted_rptnames)}: {rptname['report_name']} ---")

        try:
          print(f"    Function: {rptname['function_name']}")
          print(f"    Form: {rptname['rpt_form']}")

          # Call the report function and save the report ID
          print(f"    Calling generate_and_store_report...")
          report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)

          if not report_id:
            print(f"    ERROR: Failed to generate report ID")
            continue

          print(f"    Generated report_id: {report_id}")

          # Generate the PDF file
          print(f"    Calling generate_pdf_report...")
          pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)

          if isinstance(pdf_result, dict):
            if pdf_result.get('error'):
              print(f"    ERROR: PDF generation failed: {pdf_result['error']}")
              continue
            pdf1 = pdf_result.get('pdf')
          else:
            print(f"    ERROR: Unexpected return type from generate_pdf_report: {type(pdf_result)}")
            continue

          if pdf1 is None:
            print(f"    ERROR: No PDF generated")
            continue

          print(f"    SUCCESS: Generated PDF, type={type(pdf1)}")

          # Generate JSON file
          print(f"    Generating JSON...")
          try:
            json_media = generate_json_report(rptname['rpt_form'], report_id)
            if isinstance(json_media, dict) and json_media.get('error'):
              print(f"    WARNING: JSON generation failed: {json_media['error']}")
            elif json_media:
              json_name = f"{player_pair} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
              print(f"    Creating JSON file: {json_name}")
              json_result = write_to_nested_folder(json_folder, json_name, json_media)
              print(f"    JSON file created: {json_result}")
              pdf_files_created.append({'name': json_name, 'result': json_result})
            else:
              print(f"    WARNING: No JSON generated")
          except Exception as e:
            print(f"    ERROR: Exception generating JSON: {str(e)}")

            # Merge PDFs
          print(f"    Merging PDFs...")
          if full_rpt_pdf:
            try:
              print(f"    Merging with existing PDF...")
              full_rpt_pdf = merge_pdfs(full_rpt_pdf, pdf1, pdf_name=pdf_name)
              print(f"    After merging: type={type(full_rpt_pdf)}")
            except Exception as e:
              print(f"    ERROR: Failed to merge PDFs: {str(e)}")
              continue
          else:
            print(f"    First PDF, setting as full_rpt_pdf")
            full_rpt_pdf = pdf1

        except Exception as e:
          print(f"    ERROR: Exception processing report {rptname['report_name']}: {str(e)}")
          continue

          # Write combined PDF to Google Drive
      print(f"  Writing combined PDF...")
      if full_rpt_pdf:
        try:
          print(f"    Writing PDF to folder: {pdf_folder}")
          combined_result = write_to_nested_folder(pdf_folder, pdf_name, full_rpt_pdf)
          print(f"    SUCCESS: Combined PDF saved: {combined_result}")
          return_text = return_text + '\n' + combined_result
        except Exception as e:
          print(f"    ERROR: Failed to write combined PDF: {str(e)}")
          combined_result = f"Failed to write combined PDF {pdf_name}"
      else:
        print(f"    WARNING: No combined PDF generated")
        combined_result = f"No combined PDF generated for {pdf_name}"

        # Collect report info
      report_info = {
        'player_pair': player_pair,
        'description': rpt_r['Report Description'],
        'combined': combined_result,
        'individuals': pdf_files_created
      }
      report_infos.append(report_info)
      print(f"  Added report_info: {report_info}")

  except Exception as e:
    print(f"CRITICAL ERROR in rpt_mgr_new_rpts: {str(e)}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

  print(f"=== EXITING rpt_mgr_new_rpts ===")
  print(f"return_text: '{return_text}'")
  print(f"report_infos count: {len(report_infos)}")

  return return_text, report_infos
  





  


  





  


#-------------------------------------------------------------------------------------------------------
#  Report Manager - Scouting Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team):
  """
    Generate scouting reports and player reports, merging them into a single PDF per pair, and save to Google Drive.
    
    Args:
        rpt_r: Report configuration with rpts_inc containing DataTableRow objects
        pair_list: List of player pairs
        disp_team: Team name
    
    Returns:
        tuple: (return_text, report_infos)
    """
  today = datetime.now()
  return_text = ''
  report_infos = []

  print(f"rpt_mgr_scouting_rpts: pair_list={pair_list}, disp_team={disp_team}, rpt_r={dict(rpt_r)}")
  if not pair_list:
    logging.warning(f"No pairs provided for scouting report: {rpt_r.get('Report Description', 'Unknown')}")
    return return_text, report_infos

  try:
    for p in pair_list:
      print(f"Processing pair entry: {p}")
      disp_pair = p['pair']
      print(f"Display Pair: {disp_pair}")
      if not disp_pair:
        print(f"Invalid pair entry: {p}")
        continue
      try:
        player1, player2 = pair_players(disp_pair)
        print(f"Parsed pair: {disp_pair} -> player1={player1}, player2={player2}")
      except Exception as e:
        print(f"Failed to parse pair {disp_pair}: {str(e)}")
        continue

        # Calculate the folder we will store these into
      pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d")]
      json_folder = pdf_folder + ['json']
      lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']
      print(f"PDF folder: {pdf_folder}, JSON folder: {json_folder}, lgy={lgy}")

      # Use rpts_inc directly as DataTableRow objects
      rptname_rows = []
      for rptname1 in rpt_r['rpts_inc']:
        rptname_rows.append({
          'report_name': rptname1['report_name'],
          'rpt_type': rptname1['rpt_type'], 
          'order': rptname1['order'],
          'function_name': rptname1['function_name'],
          'rpt_form': rptname1['rpt_form']
        })


      #if len(rptname_rows) == 0:
      #  print(f"No valid reports in rpts_inc for {rpt_r['Report Description']}")
      #  continue
      print(f"Reports: {rptname_rows}")
      sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)
      print(f"Sorted reports: {[r['report_name'] for r in sorted_rptnames]}")

      # Segment reports into scouting and non-scouting
      scouting_rptnames = [rptname for rptname in sorted_rptnames if rptname['rpt_type'] == 'scouting']
      player_rptnames = [rptname for rptname in sorted_rptnames if rptname['rpt_type'] != 'scouting']
      print(f"Scouting reports: {[r['report_name'] for r in scouting_rptnames]}, Player reports: {[r['report_name'] for r in player_rptnames]}")

      # Generate scouting reports for the pair
      pair_pdf = None
      scouting_pdf_files = []
      attack_tendency_merged = False
      rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
      rpt_filters['pair'] = disp_pair
      print(f"Scouting report filters: {rpt_filters}")

      for rptname in scouting_rptnames:
        print(f"Processing scouting report: name={rptname['report_name']}, function={rptname['function_name']}, form={rptname['rpt_form']}")
        try:
          report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)
          if not report_id:
            logging.error(f"Failed to generate report ID for {rptname['report_name']}")
            continue
          logging.info(f"Generated report_id: {report_id}")

          pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
          if isinstance(pdf_result, anvil._serialise.StreamingMedia):
            logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']}")
            pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{disp_pair} {rptname['report_name']}.pdf")
          elif isinstance(pdf_result, dict):
            if pdf_result.get('error'):
              logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
              continue
            pdf1 = pdf_result.get('pdf')
          else:
            logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
            continue

          if pdf1 is None:
            logging.error(f"No PDF generated for report {rptname['report_name']}")
            continue

          logging.info(f"Generated scouting pdf: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

          # Generate JSON
          try:
            json_media = generate_json_report(rptname['rpt_form'], report_id)
            if isinstance(json_media, anvil._serialise.StreamingMedia):
              logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']}")
              json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"scouting_{disp_pair} {rptname['report_name']}.json")
            if isinstance(json_media, dict) and json_media.get('error'):
              logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {json_media['error']}")
            elif json_media:
              json_name = f"scouting_{disp_pair} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
              logging.info(f"Creating JSON file: {json_name}")
              json_result = write_to_nested_folder(json_folder, json_name, json_media)
              logging.info(f"JSON file created: {json_result}")
              scouting_pdf_files.append({'name': json_name, 'result': json_result})
            else:
              logging.error(f"No JSON generated for report {rptname['report_name']}")
          except Exception as e:
            logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

            # Merge to pair_pdf
          pair_pdf_name = f"{disp_pair} scouting_combined_{rpt_r['Report Description']}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
          logging.info(f"Creating pair PDF: {pair_pdf_name}")
          if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
            logging.info(f"Skipping attack tendency report for {disp_pair} as it was already merged")
            continue
          if pair_pdf:
            try:
              logging.info(f"Merging pair_pdf: type={type(pair_pdf)}, content_type={getattr(pair_pdf, 'content_type', 'Unknown')}")
              pair_pdf = merge_pdfs(pair_pdf, pdf1, pdf_name=pair_pdf_name)
              logging.info(f"After merging pair_pdf: type={type(pair_pdf)}")
              if rptname['report_name'] == 'attack_tendency':
                attack_tendency_merged = True
            except Exception as e:
              logging.error(f"Failed to merge pair_pdf for {pair_pdf_name}: {str(e)}")
              continue
          else:
            pair_pdf = pdf1

        except Exception as e:
          logging.error(f"Error processing scouting report {rptname['report_name']}: {str(e)}")
          continue

          # Save pair_pdf
      if pair_pdf:
        try:
          pair_pdf_result = write_to_nested_folder(pdf_folder, pair_pdf_name, pair_pdf)
          logging.info(f"Saved pair PDF: {pair_pdf_result}")
          scouting_pdf_files.append({'name': pair_pdf_name, 'result': pair_pdf_result})
        except Exception as e:
          logging.error(f"Error writing pair PDF {pair_pdf_name}: {str(e)}")

          # Generate player reports for player1
      player1_pdf = None
      player1_pdf_files = []
      attack_tendency_merged = False
      player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
      player_filters['player'] = player1
      logging.info(f"Player1 report filters: {player_filters}")

      for rptname in player_rptnames:
        print(f"Processing player report for {player1}: name={rptname['report_name']}, function={rptname['function_name']}, form={rptname['rpt_form']}")
        try:
          report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
          if not report_id:
            logging.error(f"Failed to generate report ID for {rptname['report_name']}")
            continue
          logging.info(f"Generated report_id: {report_id}")

          pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
          if isinstance(pdf_result, anvil._serialise.StreamingMedia):
            logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
            pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{player1} {rptname['report_name']}.pdf")
          elif isinstance(pdf_result, dict):
            if pdf_result.get('error'):
              logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
              continue
            pdf1 = pdf_result.get('pdf')
          else:
            logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
            continue

          if pdf1 is None:
            logging.error(f"No PDF generated for report {rptname['report_name']}")
            continue

          logging.info(f"Generated player pdf for {player1}: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

          # Generate JSON
          try:
            json_media = generate_json_report(rptname['rpt_form'], report_id)
            if isinstance(json_media, anvil._serialise.StreamingMedia):
              logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
              json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"player_{player1} {rptname['report_name']}.json")
            if isinstance(json_media, dict) and json_media.get('error'):
              logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {json_media['error']}")
            elif json_media:
              json_name = f"player_{player1} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
              logging.info(f"Creating JSON file: {json_name}")
              json_result = write_to_nested_folder(json_folder, json_name, json_media)
              logging.info(f"JSON file created: {json_result}")
              player1_pdf_files.append({'name': json_name, 'result': json_result})
            else:
              logging.error(f"No JSON generated for report {rptname['report_name']}")
          except Exception as e:
            logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

            # Merge to player1_pdf
          player1_pdf_name = f"{player1} player_combined_{rpt_r['Report Description']}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
          logging.info(f"Creating player1 PDF: {player1_pdf_name}")
          if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
            logging.info(f"Skipping attack tendency report for {player1} as it was already merged")
            continue
          if player1_pdf:
            try:
              logging.info(f"Merging player1_pdf: type={type(player1_pdf)}, content_type={getattr(player1_pdf, 'content_type', 'Unknown')}")
              player1_pdf = merge_pdfs(player1_pdf, pdf1, pdf_name=player1_pdf_name)
              logging.info(f"After merging player1_pdf: type={type(player1_pdf)}")
              if rptname['report_name'] == 'attack_tendency':
                attack_tendency_merged = True
            except Exception as e:
              logging.error(f"Failed to merge player1_pdf for {player1_pdf_name}: {str(e)}")
              continue
          else:
            player1_pdf = pdf1

        except Exception as e:
          logging.error(f"Error processing player1 report {rptname['report_name']}: {str(e)}")
          continue

          # Save player1_pdf
      if player1_pdf:
        try:
          player1_pdf_result = write_to_nested_folder(pdf_folder, player1_pdf_name, player1_pdf)
          logging.info(f"Saved player1 PDF: {player1_pdf_result}")
          player1_pdf_files.append({'name': player1_pdf_name, 'result': player1_pdf_result})
        except Exception as e:
          logging.error(f"Error writing player1 PDF {player1_pdf_name}: {str(e)}")

          # Generate player reports for player2
      player2_pdf = None
      player2_pdf_files = []
      attack_tendency_merged = False
      player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
      player_filters['player'] = player2
      logging.info(f"Player2 report filters: {player_filters}")

      for rptname in player_rptnames:
        logging.info(f"Processing player report for {player2}: name={rptname['report_name']}, function={rptname['function_name']}, form={rptname['rpt_form']}")
        try:
          report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
          if not report_id:
            logging.error(f"Failed to generate report ID for {rptname['report_name']}")
            continue
          logging.info(f"Generated report_id: {report_id}")

          pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
          if isinstance(pdf_result, anvil._serialise.StreamingMedia):
            logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
            pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{player2} {rptname['report_name']}.pdf")
          elif isinstance(pdf_result, dict):
            if pdf_result.get('error'):
              logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
              continue
            pdf1 = pdf_result.get('pdf')
          else:
            logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
            continue

          if pdf1 is None:
            logging.error(f"No PDF generated for report {rptname['report_name']}")
            continue

          logging.info(f"Generated player pdf for {player2}: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

          # Generate JSON
          try:
            json_media = generate_json_report(rptname['rpt_form'], report_id)
            if isinstance(json_media, anvil._serialise.StreamingMedia):
              logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
              json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"player_{player2} {rptname['report_name']}.json")
            if isinstance(json_media, dict) and json_media.get('error'):
              logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {json_media['error']}")
            elif json_media:
              json_name = f"player_{player2} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
              logging.info(f"Creating JSON file: {json_name}")
              json_result = write_to_nested_folder(json_folder, json_name, json_media)
              logging.info(f"JSON file created: {json_result}")
              player2_pdf_files.append({'name': json_name, 'result': json_result})
            else:
              logging.error(f"No JSON generated for report {rptname['report_name']}")
          except Exception as e:
            logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

            # Merge to player2_pdf
          player2_pdf_name = f"{player2} player_combined_{rpt_r['Report Description']}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
          logging.info(f"Creating player2 PDF: {player2_pdf_name}")
          if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
            logging.info(f"Skipping attack tendency report for {player2} as it was already merged")
            continue
          if player2_pdf:
            try:
              logging.info(f"Merging player2_pdf: type={type(player2_pdf)}, content_type={getattr(player2_pdf, 'content_type', 'Unknown')}")
              player2_pdf = merge_pdfs(player2_pdf, pdf1, pdf_name=player2_pdf_name)
              logging.info(f"After merging player2_pdf: type={type(player2_pdf)}")
              if rptname['report_name'] == 'attack_tendency':
                attack_tendency_merged = True
            except Exception as e:
              logging.error(f"Failed to merge player2_pdf for {player2_pdf_name}: {str(e)}")
              continue
          else:
            player2_pdf = pdf1

        except Exception as e:
          logging.error(f"Error processing player2 report {rptname['report_name']}: {str(e)}")
          continue

          # Save player2_pdf
      if player2_pdf:
        try:
          player2_pdf_result = write_to_nested_folder(pdf_folder, player2_pdf_name, player2_pdf)
          logging.info(f"Saved player2 PDF: {player2_pdf_result}")
          player2_pdf_files.append({'name': player2_pdf_name, 'result': player2_pdf_result})
        except Exception as e:
          logging.error(f"Error writing player2 PDF {player2_pdf_name}: {str(e)}")

          # Combine pair_pdf, player1_pdf, and player2_pdf into a single final PDF
      final_pdf_name = f"{disp_pair} {rpt_r['Report Description']}_combined_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
      logging.info(f"Creating final PDF: {final_pdf_name}")
      logging.info(f"Merging final PDF for pair {disp_pair}: pair_pdf type={type(pair_pdf)}, player1_pdf type={type(player1_pdf)}, player2_pdf type={type(player2_pdf)}")

      # Merge all available PDFs in sequence
      final_pdf = None
      pdf_list = [pdf for pdf in [pair_pdf, player1_pdf, player2_pdf] if pdf]
      if pdf_list:
        final_pdf = pdf_list[0]
        for pdf in pdf_list[1:]:
          try:
            final_pdf = merge_pdfs(final_pdf, pdf, pdf_name=final_pdf_name)
            logging.info(f"After merging final_pdf: type={type(final_pdf)}")
          except Exception as e:
            logging.error(f"Failed to merge final_pdf for {final_pdf_name}: {str(e)}")
            continue
      else:
        logging.warning(f"No PDFs generated for pair {disp_pair}")
        continue

        # Write combined PDF
      if final_pdf:
        try:
          combined_result = write_to_nested_folder(pdf_folder, final_pdf_name, final_pdf)
          logging.info(f"Saved final PDF: {combined_result}")
          return_text += '\n' + combined_result
        except Exception as e:
          logging.error(f"Error writing final PDF {final_pdf_name}: {str(e)}")
          combined_result = f"Failed to write final PDF {final_pdf_name}"
      else:
        logging.warning(f"No final PDF generated for {final_pdf_name}")
        combined_result = f"No final PDF generated for {final_pdf_name}"

        # Collect report info
      all_individuals = scouting_pdf_files + player1_pdf_files + player2_pdf_files
      report_infos.append({
        'player_pair': disp_pair,
        'description': rpt_r['Report Description'],
        'combined': combined_result,
        'individuals': all_individuals
      })
      logging.info(f"Added report_info: player_pair={disp_pair}, description={rpt_r['Report Description']}, combined={combined_result}, individuals={all_individuals}")

  except Exception as e:
    logging.error(f"Critical error in rpt_mgr_scouting_rpts: {str(e)}")
    return return_text, report_infos

  logging.info(f"rpt_mgr_scouting_rpts returning: return_text='{return_text}', report_infos={report_infos}")
  return return_text, report_infos






  
def populate_filters_from_rpt_mgr_table( rpt_r, p_r ):
  '''
  
  use the data in the report row, a row from rpt_mgr data table, to make the rpt_filters list that is used to filter the data

  for playe, pair, opp_pair, only set if the row is passed, otherwise passed as False
  '''

  rpt_filters = {}

  #print(f" in populate filters form rpt mgr table. rpt_r: \m{rpt_r} \n pair/player rot \n{p_r}")
  
  # first, we need to tellif this is a pair or a player table row:
  rpt_type = rpt_r['rpt_type']
  if p_r is not None:
    if rpt_type == 'player':
      # then this is a player
      rpt_filters['player'] = p_r['team'] + " "+p_r['number']+' '+p_r['shortname']
    elif rpt_type == 'pair':
      # then this is a pair table row
      rpt_filters['pair'] = p_r['pair']
    
  #pair_b list, opponent pair
  if rpt_r['pair_b_list'] is not None:
    rpt_filters['opp_pair'] = rpt_r['pair_b_list']['pair']

  if rpt_r['comp1'] is not None:
    rpt_filters['comp_l1'] = rpt_r['comp1']
  if rpt_r['comp2'] is not None:
    rpt_filters['comp_l2'] = rpt_r['comp2']
  if rpt_r['comp3'] is not None:
    rpt_filters['comp_l3'] = rpt_r['comp3']

  # Additional parameters
  if rpt_r['set'] is not None:
    rpt_filters['set'] = rpt_r['set']
  if rpt_r['att_ht_low'] is not None:
    rpt_filters['att_ht_low'] = rpt_r['att_ht_low']
  if rpt_r['att_ht_high'] is not None:
    rpt_filters['att_ht_high'] = rpt_r['att_ht_high']
  if rpt_r['pass_ht_low'] is not None:
    rpt_filters['pass_ht_low'] = rpt_r['pass_ht_low']
  if rpt_r['pass_ht_high'] is not None:
    rpt_filters['pass_ht_high'] = rpt_r['pass_ht_high']
  if rpt_r['set_ht_low'] is not None:
    rpt_filters['set_ht_low'] = rpt_r['set_ht_low']
  if rpt_r['set_ht_high'] is not None:
    rpt_filters['set_ht_high'] = rpt_r['set_ht_high']
  if rpt_r['set_touch_type'] is not None:
    rpt_filters['set_touch_type'] = rpt_r['set_touch_type']
  if rpt_r['pass_oos'] is not None:
    rpt_filters['pass_oos'] = rpt_r['pass_oos']
  if rpt_r['serve_speed_high'] is not None:
    rpt_filters['serve_speed_high'] = rpt_r['serve_speed_high']
  if rpt_r['serve_speed_low'] is not None:
    rpt_filters['serve_speed_low'] = rpt_r['serve_speed_low']

  # Handle dates
  today_date = date.today()
  if rpt_r['days_hist'] and rpt_r['days_hist'] != 0:
    rpt_filters['start_date'] = today_date - timedelta(days=rpt_r['days_hist'])
    rpt_filters['end_date'] = today_date
  else:
    if rpt_r['start_date'] is not None:
      rpt_filters['start_date'] = rpt_r['start_date']
    if rpt_r['end_date'] is not None:
      rpt_filters['end_date'] = rpt_r['end_date']

  return rpt_filters



