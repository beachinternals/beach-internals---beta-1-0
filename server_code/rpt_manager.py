import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
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

#--------------------------------------------------------------
#
#. Report Manager - Generate reports based on the rpt_mgr data file
#
#---------------------------------------------------------------
@anvil.server.background_task
def rpt_mgr_generate_background():
  # generate reports from the report mgt data file

  now = datetime.now()
  email_text = 'Report Manager Started at' + str(now) + ' \n \n'

  rpt_rows = app_tables.rpt_mgr.search(active="Yes")
  for rpt_r in rpt_rows:
    print(type(rpt_r['rpts_inc']), rpt_r['rpts_inc'])  # Check type and content
    for rptname in rpt_r['rpts_inc']:
      print(type(rptname), dict(rptname))  # Check type and columns of each rptname
      break
    break

  # collect user report infos for batched emails
  user_reports = defaultdict(list)

  # Open the data file, loop over rows
  for rpt_r in rpt_rows:

    # for this row, now look at each report:
    #print(f"Report Row: {rpt_r}")
    #print(f" number of rows returned: {len(rpt_r)}")
    #print(f"Fields:{rpt_r['email']}, {rpt_r['emailto']}, {rpt_r['dow']}, {rpt_r['rpt_type']} \n\n")

    # get and store the team of the user asking for hte report
    disp_team = rpt_r['team']

    # check if this report should be run today
    today = datetime.now()
    day_of_week = today.strftime("%A")
    #print(f"Day of the week: {day_of_week}, Report Day of Week: {rpt_r['dow']}")
    if (rpt_r['dow'] == day_of_week) | (rpt_r['dow'] == 'Everyday'):

      if rpt_r['rpt_type'] == 'player':
        email_text = email_text + '\n Processing '+rpt_r['rpt_type']+' Reports \n'
        ret_val, report_infos = rpt_mgr_new_rpts(rpt_r, rpt_r['player_list'], disp_team)
        if not ret_val:
          print(f"Report Manager : rpt_mgr_new__rpts Failed, {rpt_r['rpt_type']}")
        else:
          email_text = email_text + ret_val + '\n'
        if rpt_r['email']:
          user_reports[rpt_r['email']].extend(report_infos)

      elif rpt_r['rpt_type'] == 'pair':
        email_text = email_text + '\n Processing '+rpt_r['rpt_type']+' Reports \n'
        ret_val, report_infos = rpt_mgr_new_rpts(rpt_r, rpt_r['pair_list'], disp_team)
        if not ret_val:
          print(f"Report Manager : rpt_mgr_new_rpts Failed, {rpt_r['rpt_type']}")
        else:
          email_text = email_text + ret_val + '\n'
        if rpt_r['email']:
          user_reports[rpt_r['email']].extend(report_infos)

      elif rpt_r['rpt_type'] == 'league':
        email_text = email_text + '\n Processing '+rpt_r['rpt_type']+' Reports \n'
        ret_val, report_infos = rpt_mgr_new_rpts(rpt_r, rpt_r['player_list'], disp_team)
        if not ret_val:
          print(f"Report Manager : rpt_mgr_new_rpts Failed, {rpt_r['rpt_type']}")
        else:
          email_text = email_text + ret_val + '\n'
        if rpt_r['email']:
          user_reports[rpt_r['email']].extend(report_infos)

      elif rpt_r['rpt_type'] == 'scouting':
        email_text = email_text + '\n Processing scouting Reports \n'
        ret_val, report_infos, _ = rpt_mgr_scouting_rpts(rpt_r, rpt_r['pair_list'], disp_team)
        if not ret_val:
          print(f"Report Manager : rpt_mgr_scouting_rpts Failed, {rpt_r['rpt_type']}")
        else:
          email_text = email_text + ret_val + '\n'
        if rpt_r['email']:
          user_reports[rpt_r['email']].extend(report_infos)

      else:
        print(f"rpt_mgr_generate_background : Invalide Report Type : {rpt_r['rpt_type']}")
    else:
      print(f"rpt_mgr_generate_background : Invalide Report Type : {rpt_r['rpt_type']}")

  # Send batched user emails
  for user_email, reports in user_reports.items():
    try:
      email_subject = "Beach Internals - Your Generated Reports"
      email_body = f"""
Dear User,

The following reports have been generated and are available on Google Drive:

Generated on: {today.strftime('%Y-%m-%d %H:%M:%S')}

"""
      for info in reports:
        email_body += f"""
Player/Pair: {info['player_pair']}
Report Description: {info['description']}
Combined PDF: {info['combined']}

Individual Reports:
"""
        for pdf_info in info['individuals']:
          email_body += f"- {pdf_info['name']}: {pdf_info['result']}\n"

      email_body += f"""
JSON data files have also been created in the 'json' subdirectories for data analysis purposes.

Best regards,
Beach Internals Report Manager
"""

      email_status = anvil.email.send(
        to=user_email,
        from_address="no-reply",
        subject=email_subject,
        text=email_body
      )
      print(f"Email sent to {user_email}: {email_status}")
    except Exception as e:
      print(f"Error sending email to {user_email}: {str(e)}")

  # last thing, empy the report_data table when we are done
  email_text = email_text + '\nDeleting all rows form report_data table \n'+'\n'
  app_tables.report_data.delete_all_rows()

  #now, send an email with the updates
  internals_email = 'info@beachinternals.com'
  now1 = datetime.now()
  email_message = email_text + "Report Manager Completed at:" + str(now1) + ' \n' + ' Compute time: '+str(now1-now)+ "\n"
  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - Report Manager',text=email_message)

  return True




#-------------------------------------------------------------------------------------------------------
#  Report Manager - All Types of Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_new_rpts(rpt_r, p_list, disp_team):
  '''
    Using the new format, where we store filters in **rpt_filters and then store the data in a file,
    then call the pdf window to get data from the file.

    Only constraint is that the report function needs parameters of 
  '''
  today = datetime.now()
  return_text = ''
  report_infos = []

  for p in p_list:
    full_rpt_pdf = None
    pdf_files_created = []  # Track individual PDF files for email links

    # Build the report filters
    rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, p)

    # Calculate the folder we will store these into
    pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d")]
    json_folder = pdf_folder + ['json']  # JSON subfolder

    if rpt_r['rpt_type'] == 'player':
      # Then this is a player
      player_pair = p['team'] + " " + p['number'] + ' ' + p['shortname']
    elif rpt_r['rpt_type'] == 'pair':
      # Then this is a pair table row
      player_pair = p['pair']
    else:
      player_pair = 'Unknown'

    lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']

    if rpt_r['rpt_type'] == 'league':
      player_pair = lgy

    pdf_name = f"{player_pair} {rpt_r['Report Description']}.pdf"
    logging.info(f"Creating combined PDF: {pdf_name}")

    # Convert LiveObjectProxy objects to DataTableRow objects
    rptname_rows = []
    for rptname1 in rpt_r['rpts_inc']:
      if rptname1 and 'id' in rptname1:
        try:
          row = app_tables.report_list.get(id=rptname1['id'])
          rptname_rows.append(row)
        except anvil.tables.TableError as e:
          logging.error(f"Skipping deleted report_list row with id {rptname1['id']}: {str(e)}")
          continue
      else:
        logging.warning(f"Skipping invalid rptname1: {rptname1}")

        # Sort by the desired column (e.g., 'order')
    sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)

    for rptname in sorted_rptnames:
      logging.info(f"Report name: {rptname['report_name']}, function_name: {rptname['function_name']}")

      # Call the report function and save the report ID
      report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)

      # Generate the PDF file
      pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
      if isinstance(pdf_result, dict):
        if pdf_result.get('error'):
          logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
          continue
        pdf1 = pdf_result['pdf']
      else:
        logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
        continue

      if pdf1 is None:
        logging.error(f"No PDF generated for report {rptname['report_name']}")
        continue

        # Generate JSON file for individual report
      try:
        json_media = generate_json_report(rptname['rpt_form'], report_id)
        if json_media and (not isinstance(json_media, dict) or not json_media.get('error')):
          json_name = f"{player_pair} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          logging.info(f"Creating JSON file: {json_name}")
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          logging.info(f"JSON file created: {json_result}")
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

        # Store individual PDF info for email links
      individual_pdf_name = f"{player_pair} {rptname['report_name']}.pdf"
      logging.info(f"Creating individual PDF: {individual_pdf_name}")
      try:
        individual_pdf_result = write_to_nested_folder(pdf_folder, individual_pdf_name, pdf1)
        if individual_pdf_result:
          pdf_files_created.append({
            'name': individual_pdf_name,
            'result': individual_pdf_result
          })
      except Exception as e:
        logging.error(f"Error writing PDF for report {rptname['report_name']}: {str(e)}")
        continue

        # Merge PDFs
      if full_rpt_pdf:
        try:
          logging.info(f"Before merge_pdfs: full_rpt_pdf type={type(full_rpt_pdf)}, pdf1 type={type(pdf1)}")
          full_rpt_pdf = merge_pdfs(full_rpt_pdf, pdf1, pdf_name=pdf_name)
          logging.info(f"After merge_pdfs: full_rpt_pdf type={type(full_rpt_pdf)}")
        except Exception as e:
          logging.error(f"Failed to merge PDFs for {pdf_name}: {str(e)}")
          continue
      else:
        full_rpt_pdf = pdf1

        # Write combined PDF to Google Drive
    logging.info(f"Writing combined PDF: full_rpt_pdf type={type(full_rpt_pdf)}, pdf_name={pdf_name}")
    combined_result = write_to_nested_folder(pdf_folder, pdf_name, full_rpt_pdf)
    return_text = return_text + '\n' + combined_result

    # Collect report info
    report_infos.append({
      'player_pair': player_pair,
      'description': rpt_r['Report Description'],
      'combined': combined_result,
      'individuals': pdf_files_created
    })

  return return_text, report_infos




  


#-------------------------------------------------------------------------------------------------------
#  Report Manager - Scouting Reports
#-------------------------------------------------------------------------------------------------------
@anvil.server.callable
def rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team, return_pdfs=False):
  """
    Generate scouting reports and player reports, merging them into a single PDF per pair, and optionally return BlobMedia objects.
    
    Args:
        rpt_r: Report configuration with rpts_inc containing report definitions
        pair_list: List of player pairs
        disp_team: Team name
        return_pdfs (bool): If True, include BlobMedia objects in the return value
    
    Returns:
        tuple: (return_text, report_infos, pdfs) where pdfs is a list of BlobMedia objects if return_pdfs=True, else None
    """
  today = datetime.now()
  return_text = ''
  report_infos = []
  pdfs = [] if return_pdfs else None

  for p in pair_list:
    disp_pair = p['pair']
    player1, player2 = pair_players(disp_pair)
    logging.info(f"Processing pair: {disp_pair}, Team: {disp_team}")

    # Calculate the folder we will store these into
    pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), disp_team.strip(), today.strftime("%Y%m%d")]
    json_folder = pdf_folder + ['json']
    lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']

    # Convert LiveObjectProxy objects to DataTableRow objects
    rptname_rows = []
    for rptname1 in rpt_r['rpts_inc']:
      if rptname1 and 'id' in rptname1:
        try:
          row = app_tables.report_list.get(id=rptname1['id'])
          rptname_rows.append(row)
        except anvil.tables.TableError as e:
          logging.error(f"Skipping deleted report_list row with id {rptname1['id']}: {str(e)}")
          continue
      else:
        logging.warning(f"Skipping invalid rptname1: {rptname1}")

    sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)

    # Segment reports into scouting and non-scouting
    scouting_rptnames = [rptname for rptname in sorted_rptnames if rptname['rpt_type'] == 'scouting']
    player_rptnames = [rptname for rptname in sorted_rptnames if rptname['rpt_type'] != 'scouting']

    # Generate scouting reports for the pair
    pair_pdf = None
    scouting_pdf_files = []
    attack_tendency_merged = False
    rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
    rpt_filters['pair'] = disp_pair

    for rptname in scouting_rptnames:
      logging.info(f"Processing scouting report: name={rptname['report_name']}, function={rptname['function_name']}")
      report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)
      pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)

      # Handle StreamingMedia (fallback)
      if isinstance(pdf_result, anvil._serialise.StreamingMedia):
        logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']}")
        pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{disp_pair} {rptname['report_name']}.pdf")
      elif isinstance(pdf_result, dict):
        if pdf_result.get('error'):
          logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
          continue
        pdf1 = pdf_result['pdf']
      else:
        logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
        continue

      logging.info(f"Generated scouting pdf: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

      # Generate JSON
      try:
        json_media = generate_json_report(rptname['rpt_form'], report_id)
        if isinstance(json_media, anvil._serialise.StreamingMedia):
          logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']}")
          json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"scouting_{disp_pair} {rptname['report_name']}.json")
        if json_media and (not isinstance(json_media, dict) or not json_media.get('error')):
          json_name = f"scouting_{disp_pair} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          logging.info(f"Creating JSON file: {json_name}")
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          logging.info(f"JSON file created: {json_result}")
          scouting_pdf_files.append({'name': json_name, 'result': json_result})
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

        # Merge to pair_pdf, skipping attack tendency if already merged
      pair_pdf_name = f"{disp_pair} scouting_combined_{rpt_r['Report Description']}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
      logging.info(f"Creating pair PDF: {pair_pdf_name}")
      if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
        logging.info(f"Skipping attack tendency report for {disp_pair} as it was already merged")
        continue
      if pair_pdf:
        logging.info(f"Merging pair_pdf: type={type(pair_pdf)}, content_type={getattr(pair_pdf, 'content_type', 'Unknown')}")
        pair_pdf = merge_pdfs(pair_pdf, pdf1, pdf_name=pair_pdf_name)
        if rptname['report_name'] == 'attack_tendency':
          attack_tendency_merged = True
      else:
        pair_pdf = pdf1

        # Generate player reports for player1
    player1_pdf = None
    player1_pdf_files = []
    attack_tendency_merged = False
    player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
    player_filters['player'] = player1

    for rptname in player_rptnames:
      logging.info(f"Processing player report for {player1}: name={rptname['report_name']}, function={rptname['function_name']}")
      report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
      pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)

      # Handle StreamingMedia (fallback)
      if isinstance(pdf_result, anvil._serialise.StreamingMedia):
        logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
        pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{player1} {rptname['report_name']}.pdf")
      elif isinstance(pdf_result, dict):
        if pdf_result.get('error'):
          logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
          continue
        pdf1 = pdf_result['pdf']
      else:
        logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
        continue

      logging.info(f"Generated player pdf for {player1}: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

      # Generate JSON
      try:
        json_media = generate_json_report(rptname['rpt_form'], report_id)
        if isinstance(json_media, anvil._serialise.StreamingMedia):
          logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
          json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"player_{player1} {rptname['report_name']}.json")
        if json_media and (not isinstance(json_media, dict) or not json_media.get('error')):
          json_name = f"player_{player1} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          logging.info(f"Creating JSON file: {json_name}")
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          logging.info(f"JSON file created: {json_result}")
          player1_pdf_files.append({'name': json_name, 'result': json_result})
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

        # Merge to player1_pdf, skipping attack tendency if already merged
      player1_pdf_name = f"{player1} player_combined_{rpt_r['Report Description']}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
      logging.info(f"Creating player1 PDF: {player1_pdf_name}")
      if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
        logging.info(f"Skipping attack tendency report for {player1} as it was already merged")
        continue
      if player1_pdf:
        logging.info(f"Merging player1_pdf: type={type(player1_pdf)}, content_type={getattr(player1_pdf, 'content_type', 'Unknown')}")
        player1_pdf = merge_pdfs(player1_pdf, pdf1, pdf_name=player1_pdf_name)
        if rptname['report_name'] == 'attack_tendency':
          attack_tendency_merged = True
      else:
        player1_pdf = pdf1

        # Generate player reports for player2
    player2_pdf = None
    player2_pdf_files = []
    attack_tendency_merged = False
    player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
    player_filters['player'] = player2

    for rptname in player_rptnames:
      logging.info(f"Processing player report for {player2}: name={rptname['report_name']}, function={rptname['function_name']}")
      report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
      pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)

      # Handle StreamingMedia (fallback)
      if isinstance(pdf_result, anvil._serialise.StreamingMedia):
        logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
        pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{player2} {rptname['report_name']}.pdf")
      elif isinstance(pdf_result, dict):
        if pdf_result.get('error'):
          logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
          continue
        pdf1 = pdf_result['pdf']
      else:
        logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
        continue

      logging.info(f"Generated player pdf for {player2}: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

      # Generate JSON
      try:
        json_media = generate_json_report(rptname['rpt_form'], report_id)
        if isinstance(json_media, anvil._serialise.StreamingMedia):
          logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
          json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"player_{player2} {rptname['report_name']}.json")
        if json_media and (not isinstance(json_media, dict) or not json_media.get('error')):
          json_name = f"player_{player2} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          logging.info(f"Creating JSON file: {json_name}")
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          logging.info(f"JSON file created: {json_result}")
          player2_pdf_files.append({'name': json_name, 'result': json_result})
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

        # Merge to player2_pdf, skipping attack tendency if already merged
      player2_pdf_name = f"{player2} player_combined_{rpt_r['Report Description']}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
      logging.info(f"Creating player2 PDF: {player2_pdf_name}")
      if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
        logging.info(f"Skipping attack tendency report for {player2} as it was already merged")
        continue
      if player2_pdf:
        logging.info(f"Merging player2_pdf: type={type(player2_pdf)}, content_type={getattr(player2_pdf, 'content_type', 'Unknown')}")
        player2_pdf = merge_pdfs(player2_pdf, pdf1, pdf_name=player2_pdf_name)
        if rptname['report_name'] == 'attack_tendency':
          attack_tendency_merged = True
        else:
          player2_pdf = pdf1

        # Combine pair_pdf, player1_pdf, and player2_pdf into a single final PDF
        final_pdf_name = f"{disp_pair} {rpt_r['Report Description']}_combined_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
        logging.info(f"Creating final PDF: {final_pdf_name}")
        logging.info(f"Merging final PDF for pair {disp_pair}: pair_pdf type={type(pair_pdf)}, player1_pdf type={type(player1_pdf)}, player2_pdf type={type(player2_pdf)}")

        # Merge all available PDFs in sequence: pair_pdf, player1_pdf, player2_pdf
        final_pdf = None
        pdf_list = [pdf for pdf in [pair_pdf, player1_pdf, player2_pdf] if pdf]
        if pdf_list:
            final_pdf = pdf_list[0]
            for pdf in pdf_list[1:]:
                final_pdf = merge_pdfs(final_pdf, pdf, pdf_name=final_pdf_name)
        else:
            logging.warning(f"No PDFs generated for pair {disp_pair}")
            continue

        # Write combined PDF
        combined_result = write_to_nested_folder(pdf_folder, final_pdf_name, final_pdf)
        return_text += '\n' + combined_result

        # Collect report info
        all_individuals = scouting_pdf_files + player1_pdf_files + player2_pdf_files
        report_infos.append({
            'player_pair': disp_pair,
            'description': rpt_r['Report Description'],
            'combined': combined_result,
            'individuals': all_individuals
        })

        # Store PDF if requested
        if return_pdfs and final_pdf:
            pdfs.append({'player_pair': disp_pair, 'pdf': final_pdf})

    return return_text, report_infos, pdfs


  
     

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

@anvil.server.callable
def rpt_mgr_generate():
  # client callable functiom to call the background function to generate reports
  #logger.info(" Report Manager  - Generate Called")
  #test = rpt_mgr_generate_background()
  anvil.server.launch_background_task('rpt_mgr_generate_background')
  return

