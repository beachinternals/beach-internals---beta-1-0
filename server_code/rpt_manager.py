import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

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

  # Open the data file, loop over rows
  for rpt_r in app_tables.rpt_mgr.search(active=True):
    # for this row, now look at each report:
    print(f"Report Row: {rpt_r}")
    print(f"Fields:{rpt_r['email']}")
    print(f" {rpt_r['emailto']}")
    print(f" {rpt_r['dow']}")
    print(f"{rpt_r['tod']}")
    print(f" {rpt_r['rpt_type']}")
    print(f"Report Row, rpts inc: {type(rpt_r['rpts_inc'])}")
    for rptlink in rpt_r['rpts_inc']:
      print(f" Report Row -> rpts_inc: {rptlink}")
      print(f" Rpt Link: {rptlink['report_name']}, {rptlink['function_name']}")
      if rptlink['rpt_type'] == 'player':
        rpt_pdf = rpt_mgr_player_rpts(rptlink)
        email_text = 'Please find attached your requested report for the player(s)'
      elif rptlink['rpt_type'] == 'pair':
        rpt_pdf = rpt_mgr_pair_rpts(rptlink)
        email_text = 'Please find attached your requested report for the pair(s)' + rpt_r['pair_list'][0]
      elif rptlink['rpt_type'] == 'scout':
        rpt_pdf = rpt_mgr_scout_rpts(rptlink)
        email_text = 'Please find attached your requested report for the pair(s)' + rpt_r['pair_list']
      elif rptlink['rpt_type'] == 'dashboard':
        rpt_pdf = rpt_mgr_dashboard_rpts(rptlink)
        email_text = 'Please find attached your requested report for the player(s)' + rpt_r['player_list']
      elif rptlink['rpt_type'] == 'matchup':
        rpt_pdf = rpt_mgr_matchup_rpts(rptlink)
        email_text = 'Please find attached your requested report for the player(s)' + rpt_r['player_list']
      else:
        email_text = 'Invalid Report Type: '+rpt_r['rpt_type']
      # now, merge the new rpt_pdf into the master rpt_pdf to be returned

  # now email the report to the email
  email_status = anvil.email.send(to=rpt_r['emailto'],from_address="no-reply",subject='Beach Internals - Report Manager',text=email_text,attachments=[rpt_pdf])

  return email_status
  
#-------------------------------------------------------------------------------------------------------
#  Report Manager - Player Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_player_rpts(rpt_r):
  # make the pdf of player type reports
  print(f"In rpt_mgr_player_rpts {rpt_r}")
  print(f"Row: {rpt_r['report_name']}")
  rpt_pdf = 'Player Reports Stub'
  return rpt_pdf

#-------------------------------------------------------------------------------------------------------
#  Report Manager - Pair Reports
#-------------------------------------------------------------------------------------------------------
def rpt_mgr_pair_rpts(rpt_r):
  # make the pdf of pair type reports
  print(f"In rpt_mgr_pair_rpts {rpt_r}")
  print(f"Row: {rpt_r['report_name']}")
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
    
      