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

@anvil.server.callable
def rpt_mgr_generate():
  # client callable functiom to call the background function to generate reports
  return anvil.server.launch_background_task('rpt_mgr_generate_background')



@anvil.server.background_task
def rpt_mgr_generate_background():
  # generate reports from the report mgt data file

  now = datetime.now()
  email_text = 'Report Manager Started at' + str(now) + ' \n \n'

  # items needed to limit/compatible with report function calls
  comp_l1_checked = False
  disp_comp_l1 = ''
  comp_l2_checked = False
  disp_comp_l2= ''
  comp_l3_checked = False
  disp_comp_l3 = ''
  date_checked = False
  disp_start_date = ' '
  disp_end_date = ' '
  scout = True
  explain_text = ' '

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

    # items needed to limit/compatible with report function calls
    if rpt_r['comp1']:
      comp_l1_checked = True
      disp_comp_l1 = rpt_r['comp1']
    else:
      comp_l1_checked = False
      disp_comp_l1 = ''

    if rpt_r['comp2']:
      comp_l2_checked = True
      disp_comp_l2 = rpt_r['comp2']
    else:
      comp_l2_checked = False
      disp_comp_l2 =''

    if rpt_r['comp3']:
      comp_l3_checked = True
      disp_comp_l3 = rpt_r['comp3']    
    else:
      comp_l3_checked = False 
      disp_comp_l3 = ''

    if (rpt_r['days_hist'] != 0) and (rpt_r['days_hist']):
      disp_end_date = date.today()
      disp_start_date = disp_end_date - timedelta(days = rpt_r['days_hist'])
      date_checked = True
    else:
      date_checked = False
      disp_end_date = date.today()
      disp_start_date = disp_end_date - timedelta(days = 365)
    #print(f"rpt_mgr_generate_background: report row, days_history: {rpt_r['days_hist']}, Date Checked: {date_checked}, {disp_start_date}, {disp_end_date}")
    #print(f"rpt_mgr_generate_background: From the rpt_mgr DB - Serve From: {rpt_r['srv_fr']}, Serve to: {rpt_r['srv_to']}")
    # now look for scouting report serve to and from arrays\
    srv_fr = [False, False, False ]

    # test for no srv_fr or srv_to text
    if rpt_r['srv_fr'] is None:
      rpt_r['srv_fr'] = '3'
    if rpt_r['srv_to'] is None:
      rpt_r['srv_to'] = '3D'
    if len(rpt_r['srv_fr'].strip())  == 0:
      rpt_r['srv_fr'] = '3'
    if len(rpt_r['srv_to'].strip()) == 0:
      rpt_r['srv_to'] = '3D'

    #print(f"rpt_mgr_generate_background: serve From: {rpt_r['srv_fr']}, Serve to: {rpt_r['srv_to']}")
    if (len(rpt_r['srv_fr'])) != 0:
      # split the string into 3 parts ( looking for 1,3,5)
      srv_from_txt = rpt_r['srv_fr'].split(',')
      for fr in srv_from_txt:
        #print(f"rpt_mgr_generate_background: fr: {fr}")
        match fr:
          case '1':
            srv_fr[0] = True ## serve from zone 1
          case '3':
            srv_fr[1] = True ## serve from zone 3
          case '5':
            srv_fr[2] = True ## serve from zone 5
    #print(f"rpt_mgr_generate_background: serve from: {srv_fr[0]}, {srv_fr[1]}, {srv_fr[2]}")

    # serve to zone will be for 3,e would have True at srv_to_zone.at[2,0] (3 and 1 but 0 based)
    srv_to_1 = [False, False, False] # E, D, C
    srv_to_2 = [False, False, False] # E, D, C
    srv_to_3 = [False, False, False] # E, D, C
    srv_to_4 = [False, False, False] # E, D, C
    srv_to_5 = [False, False, False] # E, D, C
    srv_to_txt = rpt_r['srv_to'].split(',')
    for stt in srv_to_txt:
      stt = stt.strip()
      # parse this into a number and a letter
      net_zone = str(stt[0]).upper()
      depth_zone = str(stt[1]).upper()
      #print(f" net zone: {net_zone}, depth_zone : {depth_zone}")
      match depth_zone:
        case 'E':
          index = 0
        case 'D':
          index = 1
        case 'C':
          index = 2

      match net_zone:
        case '1':
          srv_to_1[index] = True
        case '2':
          srv_to_2[index] = True
        case '3':
          srv_to_3[index] = True
        case '4':
          srv_to_4[index] = True
        case '5':
          srv_to_5[index] = True

    #print(f"rpt_mgr_generate_background : Serve Parameters: srv_fr : {srv_fr}, serve to 1 {srv_to_1}, serve to 2 {srv_to_2}, serve to 3 {srv_to_3}, serve to 4 {srv_to_4}, serve to 5 {srv_to_5}")
    # should now have srv_to_ and srv_fr arrays ready

    scout = True
    explain_text = ' '
    #print(f"Report Filters: {comp_l1_checked}, {disp_comp_l1},{comp_l2_checked},{disp_comp_l2},{comp_l3_checked},{comp_l3_checked},{date_checked},{disp_start_date},{disp_end_date}")

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
        ret_val, report_infos = rpt_mgr_scouting_rpts(rpt_r, rpt_r['pair_list'], disp_team)
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
def rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team):
  today = datetime.now()
  return_text = ''
  report_infos = []

  for p in pair_list:
    disp_pair = p['pair']
    player1, player2 = pair_players(disp_pair)

    # Calculate the folder we will store these into
    pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d")]
    json_folder = pdf_folder + ['json']

    lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']

    # Convert LiveObjectProxy objects to DataTableRow objects
    rptname_rows = [app_tables.report_list.get(id=rptname1['id']) for rptname1 in rpt_r['rpts_inc'] if rptname1]

    # Sort by the desired column (e.g., 'order')
    sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)

    # First, create scouting reports for the pair (scouting PDF)
    scouting_pdf = None
    scouting_pdf_files = []

    rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
    rpt_filters['pair'] = disp_pair

    for rptname in sorted_rptnames:
      report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)
      pdf1 = generate_pdf_report(rptname['rpt_form'], report_id)

      # Log the type and content type of pdf1
      logging.info(f"Generated pdf1: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

      # Generate JSON
      try:
        json_media = generate_json_report(rptname['rpt_form'], report_id)
        if json_media and not isinstance(json_media, dict) or not json_media.get('error'):
          json_name = f"scouting_{rptname['report_name']}_{disp_pair}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          logging.info(f"JSON file created: {json_result}")
        else:
          error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
          logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
      except Exception as e:
        logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

        # Store individual scouting PDF
      ind_name = f"scouting_{rptname['report_name']}_{disp_pair}.pdf"
      ind_result = write_to_nested_folder(pdf_folder, ind_name, pdf1)
      if ind_result:
        scouting_pdf_files.append({'name': ind_name, 'result': ind_result})

        # Merge to scouting_pdf with a scouting PDF name
      scouting_pdf_name = f"scouting_combined_{disp_pair}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
      if scouting_pdf:
        logging.info(f"Merging scouting_pdf: type={type(scouting_pdf)}, content_type={getattr(scouting_pdf, 'content_type', 'Unknown')}")
        scouting_pdf = merge_pdfs(scouting_pdf, pdf1, pdf_name=scouting_pdf_name)
      else:
        scouting_pdf = pdf1

        # Now, for each player in the pair
    for disp_player in [player1, player2]:
      player_pdf = None
      player_pdf_files = []

      player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
      player_filters['player'] = disp_player

      for rptname in sorted_rptnames:
        report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
        pdf1 = generate_pdf_report(rptname['rpt_form'], report_id)

        # Log the type and content type of pdf1
        logging.info(f"Generated pdf1 for player: type={type(pdf1)}, content_type={getattr(pdf1, 'content_type', 'Unknown')}")

        # Generate JSON
        try:
          json_media = generate_json_report(rptname['rpt_form'], report_id)
          if json_media and not isinstance(json_media, dict) or not json_media.get('error'):
            json_name = f"non_scouting_{rptname['report_name']}_{disp_player}_{today.strftime('%Y%m%d_%H%M%S')}.json"
            json_result = write_to_nested_folder(json_folder, json_name, json_media)
            logging.info(f"JSON file created: {json_result}")
          else:
            error_msg = json_media.get('error') if isinstance(json_media, dict) else 'Unknown error'
            logging.error(f"Failed to generate JSON for report {rptname['report_name']}: {error_msg}")
        except Exception as e:
          logging.error(f"Error generating JSON for report {rptname['report_name']}: {str(e)}")

          # Store individual player PDF
        ind_name = f"non_scouting_{rptname['report_name']}_{disp_player}.pdf"
        ind_result = write_to_nested_folder(pdf_folder, ind_name, pdf1)
        if ind_result:
          player_pdf_files.append({'name': ind_name, 'result': ind_result})

          # Merge to player_pdf
        player_pdf_name = f"non_scouting_combined_{disp_player}_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
        if player_pdf:
          logging.info(f"Merging player_pdf: type={type(player_pdf)}, content_type={getattr(player_pdf, 'content_type', 'Unknown')}")
          player_pdf = merge_pdfs(player_pdf, pdf1, pdf_name=player_pdf_name)
        else:
          player_pdf = pdf1

          # Now merge scouting_pdf with player_pdf
      pdf_name = rpt_r['Report Description'] + ' ' + disp_pair + ' - ' + disp_player + '.pdf'
      logging.info(f"Merging final PDF: scouting_pdf type={type(scouting_pdf)}, player_pdf type={type(player_pdf)}")
      full_rpt_pdf = merge_pdfs(scouting_pdf, player_pdf, pdf_name=pdf_name)

      # Write combined PDF
      combined_result = write_to_nested_folder(pdf_folder, pdf_name, full_rpt_pdf)
      return_text = return_text + '\n' + combined_result

      # Collect report info (individuals include both scouting and player-specific)
      all_individuals = scouting_pdf_files + player_pdf_files
      report_infos.append({
        'player_pair': disp_pair + ' - ' + disp_player,
        'description': rpt_r['Report Description'],
        'combined': combined_result,
        'individuals': all_individuals
      })

  return return_text, report_infos


#--------------------
#.  Function to read serve receive ingo (45 total) into a matrix
#-------------------------
def make_sr_matrix(pair_yn, disp_league, disp_gender, disp_year, disp_pair, disp_player):
  # make a dataframe to store this informationin
  sr_matrix_dict = {'sr_fr':[0],
                    'sr_to_net':[0],
                    'sr_to_depth':[' '],
                    'att':[0],
                    'fbhe':[0],
                    'pass_area':[0]
                   }
  sr_matrix = pd.DataFrame.from_dict(sr_matrix_dict)
  
  # open and read the row for this  pair
  if pair_yn:
    # for pairs
    p_df, pstat_df = get_pair_data( disp_league, disp_gender, disp_year)
    p_row = p_df.loc[ (p_df['pair'] == disp_pair) & (p_df['player'] == disp_player ) ].iloc[0]
  else:
    # for the player
    p_df, pstat_df = get_player_data ( disp_league, disp_gender, disp_year )
    p_row = p_df.loc[ p_df['player'] == disp_player ].iloc[0]

  # p_df should now be 1 row
  #print(f"make_sr_matrix: p_df size = {p_df.shape[0]}")
  #print(f"make_sr_matrix: player/pair row: row: {type(p_row)}, {p_row.shape[0]}")
  #print(f"make)_sr_matrix: p row : {p_row}")
  #print(f"player : {p_row['player']}")

  #now I need to loop thru the different 
  num_saved = 0
  for i in [1,3,5]:
    for j in [1,2,3,4,5]:
      for k in ['c','d','e']:
        var_base = 'fbhe_'+str(i)+'_'+str(j)+k
        att_var = var_base+'_n'
        #print(f"make_sr_matrix: attemtps veriable: {att_var}")
        #print(f" make_sr_matrix: attempts: {p_row[att_var]}")
        if (p_row[att_var] > 4) :
          # save this record
          sr_matrix.at[num_saved,'sr_fr'] = i
          sr_matrix.at[num_saved,'sr_to_net'] = j
          sr_matrix.at[num_saved,'sr_to_depth'] = k
          sr_matrix.at[num_saved,'att'] = p_row[att_var]
          sr_matrix.at[num_saved,'fbhe'] = p_row[var_base]
          sr_matrix.at[num_saved,'pass_area'] = p_row[var_base+'_ea']
          num_saved = num_saved + 1
          
  #print(f"make_sr_matrix : serve receive matrix: {sr_matrix}")
  return sr_matrix


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

