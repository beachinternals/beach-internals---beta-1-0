import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from Generate_PDF import *

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
  # gnerate reports from the report mgt data file

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

    if rpt_r['rpt_type'] == 'player':
      # loop over all the players for this report listing
      for player_r in rpt_r['player_list']:
        print("Processing Player Reports")
        print(f"Processing report for : {player_r['league']}, {player_r['gender']}, {player_r['year']}, {player_r['team']}, {player_r['number']}, {player_r['shortname']}")

        # loop over all the reports for this player
        for rpt_print in rpt_r['rpts_inc']:
          print(f"Process report: {rpt_print['report_name']}, {rpt_print['function_name']}")
          # build player string
          disp_player = player_r['team']+' '+player_r['number']+' '+player_r['shortname']
          # call pdf report
          pdf1 = create_pdf_reports(rpt_print['function_name'],
                                    rpt_print['rpt_form'], 
                                    player_r['league'],
                                    player_r['gender'],
                                    player_r['year'],
                                    player_r['team'],
                                    disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )
          #pdf2 = anvil.BlobMedia('application/pdf',pdf1.getvalue(), name='player pdf')
          email_status = anvil.email.send(to=rpt_r['emailto'],from_address="no-reply",subject='Beach Internals - Report Manager',text='Testing 123',attachments=[pdf1])
          
      print(". ")
    elif rpt_r['rpt_type'] == 'pair':
      print("processing pair report")
      print(' ')

      # now, merge the new rpt_pdf into the master rpt_pdf to be returned

  email_text = "wow, we are done!"
  rpt_pdf = ""
  # now email the report to the email
  email_status = anvil.email.send(to=rpt_r['emailto'],from_address="no-reply",subject='Beach Internals - Report Manager',text=email_text,attachments=[rpt_pdf])

  return email_status
  
#-------------------------------------------------------------------------------------------------------
#  Report Manager - Player Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_player_rpts(rptlist_r,p_list):
  # make the pdf of player type reports
  
  print(f"In rpt_mgr_player_rpts {rptlist_r}")
  print(f"Row: {rptlist_r['report_name']},{rptlist_r['function_name']}")
  print(f"Player List : {p_list}")
  
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
    
      