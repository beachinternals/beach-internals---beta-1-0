import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from anvil.pdf import PDFRenderer
from PyPDF2 import PdfMerger
import io
import pair_functions
import pair_reports
import datetime

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
def create_pdf_reports(fnct_name, rpt_form, disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # call report function
  print(f'Calling Function:{fnct_name}')
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
    - PDF Created : {datetime.datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {disp_comp_l1 if comp_l1_checked else ''}
    - Competition 2 : {disp_comp_l2 if comp_l2_checked else ''}
    - Competition 3 : {disp_comp_l3 if comp_l3_checked else ''}
    - Date Filtered : {str(disp_start_date)+' to '+str(disp_end_date) if date_checked else ''}
    """

  # fetch the labels from the report file
  report_row = app_tables.report_list.get(function_name=fnct_name)
  
  # call render form
  #print(f"Rendering Form for {table_data1}")
  pdf_file =disp_player + ' ' + report_row['report_name'] 
  pdf = PDFRenderer( filename=pdf_file, landscape = True).render_form(rpt_form, 
                                table_data1, 
                                table_data2, 
                                table_data3, 
                                filter_text, 
                                report_row['explain_text'], 
                                disp_player, 
                                report_row['report_name'], 
                                report_row['box1_title'], 
                                report_row['box2_title'], 
                                report_row['box3_title']
                               )
  return pdf

  
@anvil.server.callable
def send_email(email_subj, email_body, email_attachment, email_to, email_from):
  if not email_to:
    email_to = anvil.users.get_user()['email']

  if not email_from:
    email_from = 'no-reply'

  result = anvil.email.send(
    from_address=email_from,
    from_name='Beach Internals', 
    to=email_to,
    subject=email_subj,
    text=email_body,
    attachments = email_attachment
  )

  return result

@anvil.server.callable
def render_all_rpts_pdf_callable(
                    disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, player_pair, user_email
                    ):
  # just kick off the background task to do this
  return_value = anvil.server.launch_background_task('render_all_rpts_pdf_background',
                    disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, player_pair, user_email
                    )

  return return_value
  
@anvil.server.background_task
def  render_all_rpts_pdf_background(
                    disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text, player_pair, user_email
                    ):

  # get all the reports out of the table, then loop thruy them all for the disp player
  function_list = [(f_row['function_name']) for f_row in app_tables.report_list.search(tables.order_by("order"),private=False,rpt_type=player_pair)]
  text_list = [(f_row['explain_text']) for f_row in app_tables.report_list.search(private=False,rpt_type=player_pair)]
  form_list = [(f_row['rpt_form']) for f_row in app_tables.report_list.search(private=False,rpt_type=player_pair)]
  print(function_list)
  full_rpt_pdf = None
  pdf_name = disp_player + ' Summary.pdf'

  
  # now loop over the items in the functioj list
  for index, value in enumerate(function_list):
    #print(index,value)
    pdf1 = anvil.server.call('create_pdf_reports', value, form_list[index],
                          disp_league, disp_gender, disp_year, 
                          disp_team, disp_player,
                          comp_l1_checked, disp_comp_l1,
                          comp_l2_checked, disp_comp_l2,
                          comp_l3_checked, disp_comp_l3,
                          date_checked, disp_start_date, disp_end_date,
                          scout, text_list[0]
                          )
    #print(pdf1)
    if pdf1 and full_rpt_pdf:
      #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')
      full_rpt_pdf = merge_pdfs( full_rpt_pdf, pdf1, pdf_name)
    else:
      #print('no original pdf file, setting to pdf1')
      full_rpt_pdf = pdf1
      #print(f'merging pdf files {full_rpt_pdf}, {pdf1}')

  # now that we are done, send this to the user
  return_value = send_email("Beach Internals - Detailed Report", "Please find the attached full player report.", full_rpt_pdf, user_email, 'no-reply')

  # and, let's write to the Google Drive
  #return_string = save_to_google_drive(full_rpt_pdf)
  #print(return_string)
  
  return return_value


def merge_pdfs( file1, file2, pdf_name):
  # initialize PdfMerger
  merger = PdfMerger()

  # print out key elements
  #print(f' Before io.Bytes(): file1: {file1}, File2: {file2}')
  
  # merge PDFs
  pdf1 = io.BytesIO(file1.get_bytes())
  pdf2 = io.BytesIO(file2.get_bytes())
  #print(f' After io.Bytes(): file1: {file1}, File2: {file2}')
  merger.append(pdf1)
  merger.append(pdf2)
  merged_pdf = io.BytesIO()
  merger.write(merged_pdf)
  merger.close()
  
  return anvil.BlobMedia('application/pdf',merged_pdf.getvalue(), name=pdf_name)

@anvil.server.callable
def save_to_google_drive(file):
   # Ensure the input is an Anvil Media object
   if not isinstance(file, anvil.Media):
       raise ValueError("Expected an Anvil Media object.")

   # Save the file to Google Drive
   google_file = app_files.drive.create_file(file.get_name(), file.get_bytes())
   return f"File '{google_file['name']}' saved to Google Drive!"
