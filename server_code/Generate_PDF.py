import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import anvil.pdf as PDFRenderer

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

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
def create_pdf_reports(fnct_name, disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # call report function
  table_data1, table_data2, table_data3 = anvil.server.call(fnct_name, disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )

  # calculate the query text
  filter_text = f"""
    Data Filters:
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """
  
  # call render form
  pdf = PDFRenderer.render_form('function_rpt', table_data1, table_data2, table_data3, filter_text, explain_text, disp_player)
  return pdf