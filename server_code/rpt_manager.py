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
from server_functions import *
import pandas as pd

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
    print(f"Day of the week: {day_of_week}, Report Day of Week: {rpt_r['dow']}")
    if (rpt_r['dow'] == day_of_week) | (rpt_r['dow'] == 'Everyday'):

      print(f"processing report type : {rpt_r['rpt_type']}")
      if rpt_r['rpt_type'] == 'player':
        # loop over all the players for this report listing

        pdf_list = ['']*len(rpt_r['player_list'])     # start a list of all pdf files to pass to email send
        pdf_num = 0
        #print(f"Pdf_list (empty) : {pdf_list}, {len(pdf_list)}")

        
        for player_r in rpt_r['player_list']:
          #print("Processing Player Reports")
          #print(f"Processing report for : {player_r['league']}, {player_r['gender']}, {player_r['year']}, {player_r['team']}, {player_r['number']}, {player_r['shortname']}")
          
          # calculate the folder we will store thiese into
          pdf_folder = [ player_r['league'].strip() + player_r['gender'].strip() + player_r['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
          #print(f"pdf folder: {pdf_folder}")
          
          # build player string
          disp_player = player_r['team']+' '+player_r['number']+' '+player_r['shortname']
          disp_pair = '' ## holdong a dummy argument to remain consistent across report calling functions
        
          full_rpt_pdf = None
          pdf_name = disp_player + ' Summary.pdf'
        
          # loop over all the reports for this player
          for rpt_print in rpt_r['rpts_inc']:
            #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")

            pdf1 = create_player_pdf_reports(rpt_print['function_name'],
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
                    scout, rpt_print['explain_text']                                 
                    )


            # now, need to merge this report with the next one
            if full_rpt_pdf:
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
              full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
            else:
              #print('no original pdf file, setting to pdf1')
              full_rpt_pdf = pdf1
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')

      
          # now write this to the google drive
          file_msg = write_to_nested_folder( pdf_folder, pdf_name, full_rpt_pdf)
      

      elif rpt_r['rpt_type'] == 'pair':
        #print("processing pair report")
        #print(f"Pair List: {rpt_r['pair_list']}")
        # loop over all the players for this report listing
        
        pdf_list = ['']*len(rpt_r['pair_list'])      # start a list of all pdf files to pass to email send
        pdf_num = 0
      
        for pair_r in rpt_r['pair_list']:
          #print(f"Processing report for : {pair_r['league']}, {pair_r['gender']}, {pair_r['year']}, {pair_r['pair']}")
        
          # build pair string
          disp_pair = pair_r['pair']

          # calculate the folder we will store thiese into
          pdf_folder = [ pair_r['league'].strip() + pair_r['gender'].strip() + pair_r['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
          #print(f"pdf folder: {pdf_folder}")
        
          full_rpt_pdf = None
          pdf_name = disp_pair + ' Summary.pdf'
          disp_player1, disp_player2 = pair_players(disp_pair)
        
          # loop over all the reports for this player
          for rpt_print in rpt_r['rpts_inc']:
            #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")

            # call pdf report
            pdf1 = create_pair_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    pair_r['league'],
                                    pair_r['gender'],
                                    pair_r['year'],
                                    disp_team,
                                    disp_pair,
                                    disp_player1,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, rpt_print['explain_text'] 
                    )

            
            # now, need to merge this report with the next one
            if full_rpt_pdf:
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
              full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
            else:
              #print('no original pdf file, setting to pdf1')
              full_rpt_pdf = pdf1
              #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')

          
          # now write this to the google drive
          file_msg = write_to_nested_folder( pdf_folder, pdf_name, full_rpt_pdf)
          
          # put this pdf into the pdf list
          pdf_list[pdf_num] = full_rpt_pdf
          pdf_num = pdf_num + 1
        '''
        email_status = anvil.email.send(to=rpt_r['emailto'],
                                          from_address="no-reply",
                                          subject='Beach Internals - Pair Summary '+disp_pair,
                                          cc='beachinternals@gmail.com' if rpt_r['copy_beachinternals'] else '',                                       
                                          text='Attached please find the summary report(s) for '+disp_pair,
                                          attachments=pdf_list)
        '''
      elif rpt_r['rpt_type'] == 'dashboard':
        # dashboard reports are for a whole team, so we ignore the pair and player entries, ump right o the reports

        # build team string
        disp_team = rpt_r['team']

        for pair_r in rpt_r['pair_list']:
          # calculate the folder we will store thiese into
          pdf_folder = [ pair_r['league'].strip() + pair_r['gender'].strip() + pair_r['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
          disp_pair = pair_r['pair']
        
        full_rpt_pdf = None
        pdf_name = disp_team + ' Team - Recent Comparisons.pdf'
        disp_player1, disp_player2 = pair_players(disp_pair)
        
        # loop over all the reports for this player
        for rpt_print in rpt_r['rpts_inc']:
          #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")
          # call pdf report
          pdf1 = create_dashboard_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    pair_r['league'],
                                    pair_r['gender'],
                                    pair_r['year'],
                                    disp_team,
                                    disp_pair,
                                    disp_player1,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, rpt_print['explain_text']                                 
                    )
            
          # now, need to merge this report with the next one
          if full_rpt_pdf:
            #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
            full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
          else:
            #print('no original pdf file, setting to pdf1')              
            full_rpt_pdf = pdf1
            #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')

          
        # now write this to the google drive
        file_msg = write_to_nested_folder( pdf_folder, pdf_name, full_rpt_pdf)

      elif rpt_r['rpt_type'] == 'internals':
        email_status = anvil.email.send(to=rpt_r['emailto'],
                                          from_address="no-reply",
                                          cc='beachinternals@gmail.com' if rpt_r['copy_beachinternals'] else '',
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
                  
          # calculate the folder we will store thiese into
          pdf_folder = [ pair_r['league'].strip() + pair_r['gender'].strip() + pair_r['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
          #print(f"pdf folder: {pdf_folder}")

          for i in [0,1]: # loop over two players in the pair
            #print(f"Processing scouting report for : {pair_r['league']}, {pair_r['gender']}, {pair_r['year']}, {pair_r['pair']}, {disp_player[i]}")
            # make the report for each player in the pair
            full_rpt_pdf = None
            pdf_name = disp_player[i] + ' Summary.pdf'
        
            # loop over all the reports for this player
            for rpt_print in rpt_r['rpts_inc']:
              #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}, Team: {rpt_r['team']}")
              #print(f"Process Scout Report, serve from: {srv_fr}, serve to 3:{srv_to_3}")

              pdf1 = create_scouting_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    pair_r['league'],
                                    pair_r['gender'],
                                    pair_r['year'],
                                    rpt_r['team'],
                                    disp_pair,
                                    disp_player[i],
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, rpt_print['explain_text'], rpt_print['box1_title'],    
                    srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                    )


            
              # now, need to merge this report with the next one
              if full_rpt_pdf:
                #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
                full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
              else:
                #print('no original pdf file, setting to pdf1')
                full_rpt_pdf = pdf1
                #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
                
            # now write this to the google drive
            file_msg = write_to_nested_folder( pdf_folder, pdf_name, pdf1)
            
            # put this pdf into the pdf list
            pdf_list[pdf_num] = full_rpt_pdf
            pdf_num = pdf_num + 1

        '''
        email_status = anvil.email.send(to=rpt_r['emailto'],
                                      from_address="no-reply",
                                      cc='beachinternals@gmail.com' if rpt_r['copy_beachinternals'] else '',
                                      subject='Beach Internals - Scouting Reports ',
                                      text='Attached please find the summary report(s)',
                                      attachments=pdf_list)
        if not email_status:
          print("report:Manager, Scouting Reports, email send failed")
        '''
      elif rpt_r['rpt_type'] == 'scouting - pdf only':
        # this category is for reports taht can only be generated as pdf files, not as web pages.
        #. all of these are pair based reports, first player 1, then player 2.  
        #  only one report may be speficied here
        #. Valid report types :
        #              - 'full scouting report - pair'
        #
        
        pdf_list = []   # start a list of all pdf files to pass to email send
        pdf_num = 0
        for pair_r in rpt_r['pair_list']:
          pdf_num = 0 # new pdf number because 
          # build pair string
          disp_pair = pair_r['pair']
          disp_player = ['','']
          disp_player[0], disp_player[1] = pair_players(disp_pair)

          # calculate the folder we will store thiese into
          pdf_folder = [ pair_r['league'].strip() + pair_r['gender'].strip() + pair_r['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
          #print(f"pdf folder: {pdf_folder}")

          for i in [0,1]: # loop over two players in the pair
            #print(f"Processing scouting report for : {pair_r['league']}, {pair_r['gender']}, {pair_r['year']}, {pair_r['pair']}, {disp_player[i]}")
            # make the report for each player in the pair
            full_rpt_pdf = None
            pdf_name = disp_player[i] + ' Full Scouting Report.pdf'

            #print(f"process report: rpt_r: {rpt_r}")
            # loop over all the reports for this player
            for rpt_print in rpt_r['rpts_inc']:
              #print(f"rpt_print: {rpt_print}")
              #print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}, Team: {rpt_r['team']}")
              #print(f"Process Scout Report, serve from: {srv_fr}, serve to 3:{srv_to_3}")

              if rpt_print['function_name'] == 'full_scouting_report_pair':
                # this full pair scouting report is made of the following merged to gether
                #. pair_sw_report
                #. pair_fbhe_net
                #. pair_fbhe_pass
                #. scout_attack_pass_zones
                #. scout_players_serve
                #. scout_attack_pass_zones

                # let's put these into a list, then loop thru the list
                report_list = ['pair_sw_report','pair_attacking','pair_serving_effectiveness','scout_attack_pass_zones','scout_srv_strategy']
                #report_list = ['scout_srv_strategy']
                for report in report_list: 
                  # create the report with the standard call:

                  # look this up in the report list to find the type of report this is
                  rpt_row = app_tables.report_list.get(function_name = report)
                  if ( (rpt_row['rpt_type'] == 'pair')  ):
                    #print(f" calling create pdf reports : rpt_row : type : {rpt_row['rpt_type']}, form: {rpt_row['rpt_form']}")
                    pdf1 = create_pair_pdf_reports(report,
                                      rpt_row['rpt_form'], 
                                      pair_r['league'],
                                      pair_r['gender'],
                                      pair_r['year'],
                                      rpt_r['team'],
                                      disp_pair,
                                      disp_player[i],
                                      comp_l1_checked, disp_comp_l1,
                                      comp_l2_checked, disp_comp_l2,
                                      comp_l3_checked, disp_comp_l3,
                                      date_checked, disp_start_date, disp_end_date,
                                      scout, rpt_print['explain_text']
                                                      )
                  elif ( rpt_row['rpt_type'] == 'scouting'):
                    # here, I want to look for the scout_srv_strategy report. 
                    if report == 'scout_srv_strategy' :
                      # for this one, we want to look in the pair data file, find this piar/player, then look for the top 3 and bottom 3 serve receive zones
                      sr_matrix = make_sr_matrix(True, pair_r['league'], pair_r['gender'], pair_r['year'], disp_pair, disp_player[i])

                      # sort this matrix
                      sr_matrix = sr_matrix.sort_values(by='fbhe', ascending=False) # this should be the high fbhe first, low last

                      # create the serve strategy report for each one in the matrix
                      if sr_matrix.shape[0] < 7 :
                        sr_index = sr_matrix.index.tolist()
                      else:
                        sr_index = sr_matrix.head(3).index.tolist() + sr_matrix.tail(3).index.tolist()
                        
                      #print(f" sr_index range of number ot process: {sr_index}")
                    
                      #now loop thru these serve strategies (to/fr) to create the reprots
                      # srv_to is T/F from zones [ 1 , 3, 5 ]
                      # serve_to_n is T/F from zone [ E, D, C ]
                      #print(f"serve index: {sr_index}, erve Matrix : {sr_matrix}")
                      for srv_strat in sr_index:
                        #print(f"serve strategy: {srv_strat}, service index : {sr_index}")
                        srv_fr = [False, False, False]
                        if sr_matrix.at[srv_strat,'sr_fr'] == 1:
                          srv_fr[0] = True
                        elif sr_matrix.at[srv_strat,'sr_fr'] == 3:
                          srv_fr[1] = True
                        elif sr_matrix.at[srv_strat,'sr_fr'] == 5:
                          srv_fr[2] = True
                        else:
                          print(f"invalid serve from {sr_matrix.at[srv_strat,'sr_fr']}")
                        srv_to_1 = [False,False,False]
                        srv_to_2 = [False,False,False]
                        srv_to_3 = [False,False,False]
                        srv_to_4 = [False,False,False]
                        srv_to_5 = [False,False,False]
                        if sr_matrix.at[srv_strat,'sr_to_net'] == 1:
                          if sr_matrix.at[srv_strat,'sr_to_depth'] == 'c':
                            srv_to_1[2] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'd':
                            srv_to_1[1] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'e':
                            srv_to_1[0] = True

                        if sr_matrix.at[srv_strat,'sr_to_net'] == 2:
                          if sr_matrix.at[srv_strat,'sr_to_depth'] == 'c':
                            srv_to_2[2] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'd':
                            srv_to_2[1] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'e':
                            srv_to_2[0] = True

                        if sr_matrix.at[srv_strat,'sr_to_net'] == 3:
                          if sr_matrix.at[srv_strat,'sr_to_depth'] == 'c':
                            srv_to_3[2] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'd':
                            srv_to_3[1] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'e':
                            srv_to_3[0] = True

                        if sr_matrix.at[srv_strat,'sr_to_net'] == 4:
                          if sr_matrix.at[srv_strat,'sr_to_depth'] == 'c':
                            srv_to_4[2] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'd':
                            srv_to_4[1] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'e':
                            srv_to_4[0] = True

                        if sr_matrix.at[srv_strat,'sr_to_net'] == 5:
                          if sr_matrix.at[srv_strat,'sr_to_depth'] == 'c':
                            srv_to_5[2] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'd':
                            srv_to_5[1] = True
                          elif sr_matrix.at[srv_strat,'sr_to_depth'] == 'e':
                            srv_to_5[0] = True

                        # print out result real quick to check
                        #print(f"rpt_mgr: Serve to / from arrays: Srv_fr:{srv_fr}, serve to 1,2,3,4,5:{srv_to_1},{srv_to_2},{srv_to_3},{srv_to_4},{srv_to_5}")

                        # now call the scouting report for each serve strategy:
                        print(f"reprot manager: Create scouting pdf reports, scout_srv _strategy, explain text: {rpt_print['explain_text']}, title: {rpt_print['box1_title']}")
                        pdf1 = create_scouting_pdf_reports(report,
                                      rpt_row['rpt_form'], 
                                      pair_r['league'],
                                      pair_r['gender'],
                                      pair_r['year'],
                                      rpt_r['team'],
                                      disp_pair,
                                      disp_player[i],
                                      comp_l1_checked, disp_comp_l1,
                                      comp_l2_checked, disp_comp_l2,
                                      comp_l3_checked, disp_comp_l3,
                                      date_checked, disp_start_date, disp_end_date,
                                      scout, rpt_print['explain_text'], rpt_print['box1_title'],    
                                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                                      )

                        # now write this to the google drive
                        #file_msg = write_to_nested_folder( pdf_folder, pdf_name, pdf1)
            
                        # now, need to merge this report with the next one
                        if full_rpt_pdf:
                          #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
                          full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
                        else:
                          #print('no original pdf file, setting to pdf1')
                          full_rpt_pdf = pdf1
                          #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
                    else:
                      # this should be scouting report, that are not the serve strategy report

                      # set dummy srv_fr, to items to complete the call
                      srv_fr = [False,False,False]
                      srv_to_1 = [False,False,False]
                      srv_to_2 = [False,False,False]
                      srv_to_3 = [False,False,False]
                      srv_to_4 = [False,False,False]
                      srv_to_5 = [False,False,False]
                      print(f"report manager: Create scouting pdf reports, explain text: {rpt_print['explain_text']}, title: {rpt_print['box1_title']}")
                      pdf1 = create_scouting_pdf_reports(report,
                                      rpt_row['rpt_form'], 
                                      pair_r['league'],
                                      pair_r['gender'],
                                      pair_r['year'],
                                      rpt_r['team'],
                                      disp_pair,
                                      disp_player[i],
                                      comp_l1_checked, disp_comp_l1,
                                      comp_l2_checked, disp_comp_l2,
                                      comp_l3_checked, disp_comp_l3,
                                      date_checked, disp_start_date, disp_end_date,
                                      scout, rpt_print['explain_text'], rpt_print['box1_title'],    
                                      srv_fr, srv_to_1,srv_to_2,srv_to_3,srv_to_4,srv_to_5 
                                      )
                  else:
                    pdf1 = 'Invalid Report Type'+rpt_row['rpt_type']

        
                  # now, need to merge this report with the next one
                  if full_rpt_pdf:
                    #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
                    full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
                  else:
                    #print('no original pdf file, setting to pdf1')
                    full_rpt_pdf = pdf1
                  #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
              else:
                full_rpt_pdf = 'Invalid Function name/report type'+rpt_r['function_name']+' '+rpt_r['rpt_type']

            # now write this to the google drive
            file_msg = write_to_nested_folder( pdf_folder, pdf_name, full_rpt_pdf)
            
            # put this pdf into the pdf list
            pdf_list.append(full_rpt_pdf)
            #print(f"rpt_manager: Scout Reports: added pdf report to list: list size: {pdf_num}, {len(pdf_list)}")
            pdf_num = pdf_num + 1
          
          #print(f"Preparing to send email.  PDF List Length {len(pdf_list)}, Type: {type(pdf_list)}, pdf_num: {pdf_num}")
          #for i in range(0,len(pdf_list)):
          #  print(f"PDF List to email: item number :{i}, PDF File: {pdf_list[i]}")
            
          '''
          email_status = anvil.email.send(to=rpt_r['emailto'],
                                      from_address="no-reply",
                                      cc='beachinternals@gmail.com' if rpt_r['copy_beachinternals'] else '',
                                      subject='Beach Internals - Scouting Reports ',
                                      text='Attached please find the summary report(s)',
                                      attachments=pdf_list)

          if not email_status:
            print("report:Manager, Scouting Reports, email send failed")
          '''
      elif rpt_r['rpt_type'] == 'matchup':
        print(f"Matchup Reports: {rpt_r['rpt_type']}")
        ret_val = rpt_mgr_matchup_rpts(rpt_r, disp_team)
        if not ret_val:
          print(f"Report Manager : rpt_mgt_matachup_rpts Failed, {rpt_r['rpt_type']}")
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
def rpt_mgr_matchup_rpts(rpt_r, disp_team):
  # for a matchup report, rpt_r should have just one pair and just one pair_b in the list
  today = datetime.now() 
  for pair_r in rpt_r['pair_list']:
    #print(f"Processing report for : {pair_r['league']}, {pair_r['gender']}, {pair_r['year']}, {pair_r['pair']}")
        
    # build pair string
    pair_a = pair_r['pair']
    #print(f"opponentn pair, index 5 {rpt_r['pair_b_list'][5]}")
    #print(f"opponentn pair, index 5, '1' {rpt_r['pair_b_list'][5][1]}")   
    pair_b = rpt_r['pair_b_list'][5][1]
    #print(f"Pair _b ; {pair_b}")


    # calculate the folder we will store thiese into
    pdf_folder = [ pair_r['league'].strip() + pair_r['gender'].strip() + pair_r['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d") ]
    #print(f"pdf folder: {pdf_folder}")
        
    full_rpt_pdf = None
    pdf_name = rpt_r['Report Description'] + ' Matchup Analysis.pdf'

    
    '''
    # loop over all the reports for this player
    print(f" report Row, rpts_inc {rpt_r['rpts_inc']}")
    #print(f"rpts_inc 0: {rpt_r['rpts_inc'][0]}")
    print(f"rpts_inc 0:0 {rpt_r['rpts_inc'][0][0]}")
    print(f"rpts_inc 0:1 {rpt_r['rpts_inc'][0][1]}")
    print(f"rpts_inc 0:2 {rpt_r['rpts_inc'][0][2]}")
    print(f"rpts_inc 0:3 {rpt_r['rpts_inc'][0][3]}")
    print(f"rpts_inc 0:4 {rpt_r['rpts_inc'][0][4]}")
    print(f"rpts_inc 0:5 {rpt_r['rpts_inc'][0][5]}")
    print(f"rpts_inc 0:6 {rpt_r['rpts_inc'][0][6]}")
    print(f"rpts_inc 0:7 {rpt_r['rpts_inc'][0][7]}")
    print(f"rpts_inc 0:8 {rpt_r['rpts_inc'][0][8]}")
    print(f"rpts_inc 0:9 {rpt_r['rpts_inc'][0][9]}")
    print(f"rpts_inc 0:9:0 {rpt_r['rpts_inc'][0][9][0]}")
    print(f"rpts_inc 0:9:1 {rpt_r['rpts_inc'][0][9][1]}")
    print(f"rpts_inc 0:10 {rpt_r['rpts_inc'][0][10]}")
    print(f"rpts_inc 0:11 {rpt_r['rpts_inc'][0][11]}")
    print(f"rpts_inc 0:12 {rpt_r['rpts_inc'][0][12]}")
    print(f"rpts_inc 0:13 {rpt_r['rpts_inc'][0][13]}")
    print(f"rpts_inc 0:14 {rpt_r['rpts_inc'][0][14]}")
    print(f"rpts_inc 0:15 {rpt_r['rpts_inc'][0][15]}")
    print(f"rpts_inc 0:16 {rpt_r['rpts_inc'][0][16]}")
    print(f"rpts_inc 0:17 {rpt_r['rpts_inc'][0][17]}")
    print(f"rpts_inc 0:18 {rpt_r['rpts_inc'][0][18]}")
    print(f"rpts_inc 0:19 {rpt_r['rpts_inc'][0][19]}")
    print(f"rpts_inc 0:20 {rpt_r['rpts_inc'][0][20]}")
    print(f"rpts_inc 0:21 {rpt_r['rpts_inc'][0][21]}")
    '''
    # make a little list of the function name, report name, and index,
    rpt_list_df = pd.DataFrame({'order':[0],'rpt_form':[''],'function_name':[''] })
    for r in rpt_r['rpts_inc']:
      rpt_list_df.loc[len(rpt_list_df)] = { 'order':r[9][1], 'rpt_form':r[4][1], 'function_name':r[14][1] }

    rpt_list_df = rpt_list_df.sort_values('order', ascending=True)
    rpt_list_df = rpt_list_df.iloc[1:]
    print(f"report list {rpt_list_df}")
      
    for j in [0,1]:
      if j == 1: # swap the two teams, and run it again
        tmp = pair_a
        pair_a = pair_b
        pair_b = tmp
      for index, rpt_print in rpt_list_df.iterrows():
        print(f"Process report: {rpt_print['function_name']}")
        # call pdf report
        pdf1 = create_matchup_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    pair_r['league'],
                                    pair_r['gender'],
                                    pair_r['year'],
                                    pair_a,
                                    pair_b, 
                                    disp_team
                    )
        # now, need to merge this report with the next one
        if full_rpt_pdf:
          #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
          full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
        else:
          #print('no original pdf file, setting to pdf1')
          full_rpt_pdf = pdf1
          #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
          
    # now write this to the google drive
    file_msg = write_to_nested_folder( pdf_folder, pdf_name, full_rpt_pdf)

  return True


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