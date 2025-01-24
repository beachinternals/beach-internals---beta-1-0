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
from pair_functions import *

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

    print(f"rpt_mgr_generate_background: serve From: {rpt_r['srv_fr']}, Serve to: {rpt_r['srv_to']}")
    if (len(rpt_r['srv_fr'])) != 0:
      # split the string into 3 parts ( looking for 1,3,5)
      srv_from_txt = rpt_r['srv_fr'].split(',')
      for fr in srv_from_txt:
        print(f"rpt_mgr_generate_background: fr: {fr}")
        match fr:
          case str(1):
            srv_fr[0] = True ## serve from zone 1
          case str(3):
            srv_fr[1] = True ## serve from zone 3
          case str(5):
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
        case '2':
          srv_to_4[index] = True
        case '2':
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
                                          text='Attached please find the summary report(s) : Internals Reports')
                                          #attachments=[full_rpt_pdf])
      elif rpt_r['rpt_type'] == 'scouting':
        pdf_list = ['']*len(rpt_r['pair_list'])*2      # start a list of all pdf files to pass to email send
        pdf_num = 0
        for pair_r in rpt_r['pair_list']:
          # build pair string
          disp_pair = pair_r['pair']
          disp_player = ['','']
          disp_player[0], disp_player[1] = pair_players(disp_pair)

          for i in [0,1]: # loop over two players in the pair
            print(f"Processing scouting report for : {pair_r['league']}, {pair_r['gender']}, {pair_r['year']}, {pair_r['pair']}, {disp_player[i]}")
            # make the report for each player in the pair
            full_rpt_pdf = None
            pdf_name = disp_player[i] + ' Summary.pdf'
        
            # loop over all the reports for this player
            for rpt_print in rpt_r['rpts_inc']:
              #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")

              pdf1 = create_scouting_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    pair_r['league'],
                                    pair_r['gender'],
                                    pair_r['year'],
                                    rpt_print['team'],
                                    disp_pair,
                                    disp_player[i],
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, rpt_print['explain_text'],    
                    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                    )
              # now, need to merge this report with the next one
              if full_rpt_pdf:
                print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
                full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
              else:
                print('no original pdf file, setting to pdf1')
                full_rpt_pdf = pdf1
                print(f'merging pdf files {full_rpt_pdf}, {pdf1}')

            # put this pdf into the pdf list
            pdf_list[pdf_num] = full_rpt_pdf
            pdf_num = pdf_num + 1
          
        email_status = anvil.email.send(to=rpt_r['emailto'],
                                      from_address="no-reply",
                                      subject='Beach Internals - Scouting Reports ',
                                      text='Attached please find the summary report(s)',
                                      attachments=pdf_list)
        if not email_status:
          print("report:Manager, Scouting Reports, email send failed")
      else:
        print(f"rpt_mgr_generate_background : Invalide Report Type : {rpt_r['rpt_type']}")

  return True
  
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
    
      