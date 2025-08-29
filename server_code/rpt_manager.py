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
def rpt_mgr_new_rpts( rpt_r, p_list, disp_team ):

  '''
  
  Using the new format, where we store filters in **rpt_filters and then store the data in a file, then call the pdf window to get data from the file

  Only COostraint is that the report function needs parameters of 
  '''

  today = datetime.now() 
  return_text = ''
  report_infos = []

  for p in p_list:
    full_rpt_pdf = None
    pdf_files_created = []  # Track individual PDF files for email links

    # build the rpt_filers to pass
    #  we will leave it up to the 
    rpt_filters = populate_filters_from_rpt_mgr_table( rpt_r, p)

    # calculate the folder we will store thiese into
    pdf_folder = [ p['league'].strip() + p['gender'].strip() + p['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
    json_folder = pdf_folder + ['json']  # JSON subfolder

    #print(f"pdf folder: {pdf_folder}")
    #print(f"json folder: {json_folder}")

    full_rpt_pdf = None
    if rpt_r['rpt_type'] == 'player':
      # then this is a player
      player_pair = p['team'] + " "+p['number']+' '+p['shortname']
    elif rpt_r['rpt_type'] == 'pair':
      # then this is a pair table row
      player_pair = p['pair']
    else:
      player_pair = ' Unknown '

    #print(f"new player reports: player {p['league']}, {p['gender']}, {p['year']}, {p['team']},{p['number']}, {p['shortname']}")
    lgy = p['league']+' | '+p['gender']+' | '+p['year']

    if rpt_r['rpt_type'] == 'league':
      player_pair = lgy  

    pdf_name = rpt_r['Report Description'] + ' ' + player_pair + '.pdf'

    # Convert LiveObjectProxy objects to DataTableRow objects
    rptname_rows = [app_tables.report_list.get(id=rptname1['id']) for rptname1 in rpt_r['rpts_inc'] if rptname1]

    # Sort by the desired column (e.g., 'order')
    sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)

    # Debug: Print sorted_rptnames and its type
    #print(f"sorted rptnames: {[dict(row) for row in sorted_rptnames]}, \n type: {type(sorted_rptnames)}")

    # If you want to access rpt_r['rpts_inc'], do it here
    #print(f"rpts_inc from rpt_r: {rpt_r['rpts_inc']}")

    for rptname in sorted_rptnames:
      print(f" Report name: {rptname['report_name']}, {rptname['function_name']}\n\n")

      # call the report function and save the report id
      #print(f"rpt mgr: lgy: {lgy}, disp team {disp_team}, rpt filters {rpt_filters}")
      report_id = generate_and_store_report( rptname['function_name'], lgy, disp_team, **rpt_filters )

      # generate the PDF file
      pdf1 = generate_pdf_report( rptname['rpt_form'], report_id)

      # NEW: Generate JSON file for individual report
      try:
        json_media = generate_json_report(rptname['rpt_form'], report_id)
        if json_media and not isinstance(json_media, dict) or not json_media.get('error'):
          json_name = f"{rptname['report_name']}_{player_pair}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          print(f"JSON file created: {json_result}")
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          print(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        print(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

      # Store individual PDF info for email links
      individual_pdf_name = f"{rptname['report_name']}_{player_pair}.pdf"
      individual_pdf_result = write_to_nested_folder(pdf_folder, individual_pdf_name, pdf1)
      if individual_pdf_result:
        pdf_files_created.append({
          'name': individual_pdf_name,
          'result': individual_pdf_result
        })

      # now, need to merge this report with the next one
      if full_rpt_pdf:
        #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
        full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
      else:
        #print('no original pdf file, setting to pdf1')
        full_rpt_pdf = pdf1

    # now write this to the google drive (combined PDF)
    combined_result = write_to_nested_folder( pdf_folder, pdf_name, full_rpt_pdf )
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
def rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team, return_pdfs=False):
  """
    Generate scouting reports and player reports, merging them appropriately, and optionally return BlobMedia objects.
    
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
    rptname_rows = [app_tables.report_list.get(id=rptname1['id']) for rptname1 in rpt_r['rpts_inc'] if rptname1]
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
        pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{rptname['report_name']}.pdf")
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
          json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"scouting_{rptname['report_name']}_{disp_pair}.json")
        if json_media and (not isinstance(json_media, dict) or not json_media.get('error')):
          json_name = f"scouting_{rptname['report_name']}_{disp_pair}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          logging.info(f"JSON file created: {json_result}")
          scouting_pdf_files.append({'name': json_name, 'result': json_result})
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

        # Merge to pair_pdf, skipping attack tendency if already merged
      pair_pdf_name = f"scouting_combined_{disp_pair}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
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

        # Generate player reports for each player
    for disp_player in [player1, player2]:
      player_pdf = None
      player_pdf_files = []
      attack_tendency_merged = False
      player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
      player_filters['player'] = disp_player

      for rptname in player_rptnames:
        logging.info(f"Processing player report for {disp_player}: name={rptname['report_name']}, function={rptname['function_name']}")
        report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
        pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)

        # Handle StreamingMedia (fallback)
        if isinstance(pdf_result, anvil._serialise.StreamingMedia):
          logging.info(f"Converting unexpected StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
          pdf1 = anvil.BlobMedia('application/pdf', pdf_result.get_bytes(), name=f"{rptname['report_name']}.pdf")
        elif isinstance(pdf_result, dict):
          if pdf_result.get('error'):
            logging.error(f"Failed to generate PDF for report {rptname['report_name']}: {pdf_result['error']}")
            continue
          pdf1 = pdf_result['pdf']
        else:
          logging.error(f"Unexpected return type from generate_pdf_report: {type(pdf_result)}")
          continue

        logging.info(f"Generated player pdf for {disp_player}: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

        # Generate JSON
        try:
          json_media = generate_json_report(rptname['rpt_form'], report_id)
          if isinstance(json_media, anvil._serialise.StreamingMedia):
            logging.info(f"Converting JSON StreamingMedia to BlobMedia for report {rptname['report_name']} (player)")
            json_media = anvil.BlobMedia('application/json', json_media.get_bytes(), name=f"player_{rptname['report_name']}_{disp_player}.json")
          if json_media and (not isinstance(json_media, dict) or not json_media.get('error')):
            json_name = f"player_{rptname['report_name']}_{disp_player}_{today.strftime('%Y%m%d_%H%M%S')}.json"
            json_result = write_to_nested_folder(json_folder, json_name, json_media)
            logging.info(f"JSON file created: {json_result}")
            player_pdf_files.append({'name': json_name, 'result': json_result})
          else:
            error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
            logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
        except Exception as e:
          logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

          # Merge to player_pdf, skipping attack tendency if already merged
        player_pdf_name = f"player_combined_{disp_player}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
        if rptname['report_name'] == 'attack_tendency' and attack_tendency_merged:
          logging.info(f"Skipping attack tendency report for {disp_player} as it was already merged")
          continue
        if player_pdf:
          logging.info(f"Merging player_pdf: type={type(player_pdf)}, content_type={getattr(player_pdf, 'content_type', 'Unknown')}")
          player_pdf = merge_pdfs(player_pdf, pdf1, pdf_name=player_pdf_name)
          if rptname['report_name'] == 'attack_tendency':
            attack_tendency_merged = True
        else:
          player_pdf = pdf1

          # Combine pair_pdf and player_pdf into final PDF
      final_pdf_name = f"{rpt_r['Report Description']} {disp_pair} - {disp_player}.pdf"
      logging.info(f"Merging final PDF for {disp_player}: pair_pdf type={type(pair_pdf)}, player_pdf type={type(player_pdf)}")
      final_pdf = merge_pdfs(pair_pdf, player_pdf, pdf_name=final_pdf_name) if pair_pdf and player_pdf else (pair_pdf or player_pdf)

      # Write combined PDF
      combined_result = write_to_nested_folder(pdf_folder, final_pdf_name, final_pdf)
      return_text += '\n' + combined_result

      # Collect report info
      all_individuals = scouting_pdf_files + player_pdf_files
      report_infos.append({
        'player_pair': disp_pair + ' - ' + disp_player,
        'description': rpt_r['Report Description'],
        'combined': combined_result,
        'individuals': all_individuals
      })

      # Store PDF if requested
      if return_pdfs and final_pdf:
        pdfs.append({'player_pair': disp_pair + ' - ' + disp_player, 'pdf': final_pdf})

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

