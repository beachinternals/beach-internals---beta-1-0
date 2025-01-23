import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from Generate_PDF import *
from datetime import datetime, timedelta, date

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


  # Open the data file, loop over rows
  for rpt_r in app_tables.rpt_mgr.search( active="Yes" ):
    
    # for this row, now look at each report:
    #print(f"Report Row: {rpt_r}")
    #print(f" number of rows returned: {len(rpt_r)}")
    #print(f"Fields:{rpt_r['email']}, {rpt_r['emailto']}, {rpt_r['dow']}, {rpt_r['tod']}, {rpt_r['rpt_type']} \n\n")

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

    print(f"rpt_mgr_generate_background: report row, days_history: {rpt_r['days_hist']}")
    if (rpt_r['days_hist'] != 0) and (rpt_r['days_hist']):
      disp_end_date = date.today()
      disp_start_date = disp_end_date - timedelta(days = rpt_r['days_hist'])
      date_checked = True
    else:
      date_checked = False
      disp_end_date = date.today()
      disp_start_date = disp_end_date - timedelta(days = 365)

    # now look for scouting report serve to and from arrays\
    srv_from = [False, False, False ]
    if (len(rpt_r['srv_fr'])) != 0:
      # split the string into 3 parts ( looking for 1,3,5)
      srv_from_txt = rpt_r['srv_fr'].split(',')
      if ( srv_from_txt[0].strip() == str(1) ) or ( srv_from_txt[1].strip() == str(1) ) or (srv_from_txt[2].strip() == str(1) ):
        srv_from[0] = True
      if ( srv_from_txt[0].strip() == str(3) ) or ( srv_from_txt[1].strip() == str(3) ) or (srv_from_txt[2].strip() == str(3) ):
        srv_from[1] = True
      if ( srv_from_txt[0].strip() == str(5) ) or ( srv_from_txt[1].strip() == str(5) ) or (srv_from_txt[2].strip() == str(5) ):
        srv_from[2] = True

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
      net_zone = str(stt[0])
      depth_zone = str(stt[1])
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
        case '2':
          srv_to_4[index] = True
        case '2':
          srv_to_5[index] = True

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
      # loop over all the players for this report listing
        for player_r in rpt_r['player_list']:
          #print("Processing Player Reports")
          #print(f"Processing report for : {player_r['league']}, {player_r['gender']}, {player_r['year']}, {player_r['team']}, {player_r['number']}, {player_r['shortname']}")
        
          # build player string
          disp_player = player_r['team']+' '+player_r['number']+' '+player_r['shortname']
        
          full_rpt_pdf = None
          pdf_name = disp_player + ' Summary.pdf'
        
          # loop over all the reports for this player
          for rpt_print in rpt_r['rpts_inc'].sort():
            #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")

            # Different create pdf reports for scouting vs. pair or player reports
            if ( rpt_r['rpt_type'] == 'player') or ( rpt_r['rpt_type'] == 'pair') :
              # call pdf report
              pdf1 = create_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    player_r['league'],
                                    player_r['gender'],
                                    player_r['year'],
                                    disp_team,
                                    disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text                                    
                    )
            elif ( rpt_r['rpt_type'] == 'scouting' ):
              pdf1 = create_scouting_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    player_r['league'],
                                    player_r['gender'],
                                    player_r['year'],
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
            else:
              print(f"rpt_mgr_generate_background : Invalid Report Type: {rpt_r['rpt_type']}, report for team: {rpt_r['team']}")
            
            # now, need to merge this report with the next one
            if full_rpt_pdf:
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
              full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
            else:
              #print('no original pdf file, setting to pdf1')
              full_rpt_pdf = pdf1
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
          
          email_status = anvil.email.send(to=rpt_r['emailto'],
                                          from_address="no-reply",
                                          subject='Beach Internals - Player Summary '+disp_player,
                                          text='Attached please find the summary report(s) for '+disp_player,
                                          attachments=[full_rpt_pdf])
          
        #print(". ")
      elif rpt_r['rpt_type'] == 'pair':
        #print("processing pair report")
        #print(f"Pair List: {rpt_r['pair_list']}")
        # loop over all the players for this report listing
        for pair_r in rpt_r['pair_list']:
          #print(f"Processing report for : {pair_r['league']}, {pair_r['gender']}, {pair_r['year']}, {pair_r['pair']}")
        
          # build pair string
          disp_pair = pair_r['pair']
        
          full_rpt_pdf = None
          pdf_name = disp_pair + ' Summary.pdf'
        
          # loop over all the reports for this player
          for rpt_print in rpt_r['rpts_inc']:
            #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")

            # call pdf report
            pdf1 = create_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    pair_r['league'],
                                    pair_r['gender'],
                                    pair_r['year'],
                                    disp_team,
                                    disp_pair,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )
            
            # now, need to merge this report with the next one
            if full_rpt_pdf:
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
              full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
            else:
              #print('no original pdf file, setting to pdf1')
              full_rpt_pdf = pdf1
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
          

          email_status = anvil.email.send(to=rpt_r['emailto'],
                                          from_address="no-reply",
                                          subject='Beach Internals - Pair Summary '+disp_pair,
                                          text='Attached please find the summary report(s) for '+disp_pair,
                                          attachments=[full_rpt_pdf])
      elif rpt_r['rpt_type'] == 'dashboard':
        email_status = anvil.email.send(to=rpt_r['emailto'],
                                          from_address="no-reply",
                                          subject='Beach Internals - Dashboard Summary '+disp_team,
                                          text='Attached please find the summary report(s) for '+disp_team)
                                          #attachments=[full_rpt_pdf])
      elif rpt_r['rpt_type'] == 'internals':
        email_status = anvil.email.send(to=rpt_r['emailto'],
                                          from_address="no-reply",
                                          subject='Beach Internals - Administrative Data ',
                                          text='Attached please find the summary report(s)')
                                          #attachments=[full_rpt_pdf])

  return True. 
  
#-------------------------------------------------------------------------------------------------------
#  Report Manager - Player Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_player_rpts(rptlist_r,p_list):
  # make the pdf of player type reports
  
  #print(f"In rpt_mgr_player_rpts {rptlist_r}")
  #print(f"Row: {rptlist_r['report_name']},{rptlist_r['function_name']}")
  #print(f"Player List : {p_list}")
  
  for p in p_list:
    print(f"player {p['league']}, {p['gender']}, {p['year']}, {p['team']},{p['number']}, {p['shortname']}")
    for rptname in rptlist_r:
      print(f" Report name: {rptname['report_name']}, {rptname['function_name']}\n\n")
    
  rpt_pdf = 'Player Reports Stub'
  return rpt_pdf

#-------------------------------------------------------------------------------------------------------
#  Report Manager - Pair Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_pair_rpts(rptlist,pair_list):
  # make the pdf of pair type reports
  print(f"In rpt_mgr_pair_rpts {rptlist}")
  print(f"Row: {rptlist['report_name']}, {rptlist['function_name']}")
  print(f"pair list : {pair_list}")
  if pair_list:
    for pair in pair_list:
      print(f"Pair {pair['league']}, {pair['gender']}, {pair['year']}, {pair['pair']}")
    
  rpt_pdf = 'Pair Reprots Stub'
  return rpt_pdf

#-------------------------------------------------------------------------------------------------------
#  Report Manager - Scout Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_scout_rpts(rpt_r):
  # make the pdf of scout type reports
  rpt_pdf = 'Scout Reports Stub'
  return rpt_pdf

#-------------------------------------------------------------------------------------------------------
#  Report Manager - Dashboard Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_dashboard_rpts(rpt_r):
  # make the pdf of player type reports
  rpt_pdf = 'Dashboard Reports'
  return rpt_pdf

#-------------------------------------------------------------------------------------------------------
#  Report Manager - Matchup Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_matchup_rpts(rpt_r):
  # make the pdf of player type reports
  rpt_pdf = 'Matchup Reports'
  return rpt_pdf
    
      