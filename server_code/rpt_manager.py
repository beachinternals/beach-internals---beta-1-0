import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
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

@anvil.server.callable
def rpt_mgr_generate():
  # client callable functiom to call the background function to generate reports
  #logger.info(" Report Manager  - Generate Called")
  #test = rpt_mgr_generate_background()
  anvil.server.launch_background_task('rpt_mgr_generate_background')
  return
  
#--------------------------------------------------------------
#
#. Report Manager - Generate reports based on the rpt_mgr data file
#
#---------------------------------------------------------------
@anvil.server.background_task
def rpt_mgr_generate_background():
  # Generate reports from the report mgt data file
  now = datetime.now()
  email_text = 'Report Manager Started at ' + str(now) + ' \n \n'
  print(f"Report Manager Started at {now}")

  print("Checking table access...")
  try:
    # Access app_tables through the tables module
    app_tables = tables.app_tables
    #print(f"app_tables type: {type(app_tables)}")
    #print(f"app_tables dir: {dir(app_tables)}")

    # Try to access rpt_mgr table
    rpt_rows = app_tables.rpt_mgr.search(active="Yes")
    rpt_rows_list = list(rpt_rows)
    print(f"SUCCESS: Found {len(rpt_rows_list)} active reports in rpt_mgr table")

  except AttributeError as e:
    print(f"AttributeError: {e}")
    # Try alternative access method
    try:
      print("Trying alternative access...")
      rpt_mgr_table = getattr(tables.app_tables, 'rpt_mgr', None)
      if rpt_mgr_table:
        rpt_rows = rpt_mgr_table.search(active="Yes")
        rpt_rows_list = list(rpt_rows)
        print(f"SUCCESS via getattr: Found {len(rpt_rows_list)} active reports")
      else:
        print("rpt_mgr table not found via getattr either")
        # List all available tables
        print("Available app tables:")
        for attr in dir(tables.app_tables):
          if not attr.startswith('_'):
            print(f"  - {attr}")
        return False
    except Exception as e2:
      print(f"Alternative access also failed: {e2}")
      return False
  except Exception as e:
    print(f"Other error: {e}")
    return False

  if not rpt_rows_list:
    print("No active reports found in rpt_mgr table")
    email_text += "No active reports found in rpt_mgr table\n"
    return False


    # Collect user report infos for batched emails
  user_reports = defaultdict(list)
  processed_count = 0
  total_reports = len(rpt_rows_list)

  # Open the data file, loop over rows
  for report_index, rpt_r in enumerate(rpt_rows_list):
    print(f"\n=== Processing report {report_index + 1} of {total_reports} ===")

    try:
      disp_team = rpt_r['team']
      #print(f"Report Description: {rpt_r['Report Description']}")
      #print(f"Report type: {rpt_r['rpt_type']}")
      #print(f"Team: {disp_team}")

      # Check if this report should be run today
      today = datetime.now()
      day_of_week = today.strftime("%A")
      #print(f"Today is: {day_of_week}")
      #print(f"Report scheduled for: {rpt_r['dow']}")

      if rpt_r['dow'] not in [day_of_week, 'Everyday']:
        print(f"SKIPPING: Report not scheduled for {day_of_week}")
        print(f"rpt_mgr_generate_background: Report {rpt_r['rpt_type']} not scheduled for {day_of_week}")
        continue

      #print(f"Report IS scheduled to run today")

      # Process different report types
      if rpt_r['rpt_type'] in ['player', 'pair', 'league', 'dashboard']:
        input_list = rpt_r['player_list'] if rpt_r['rpt_type'] != 'pair' else rpt_r['pair_list']

        print(f"Input list type: {type(input_list)}")
        if input_list:
          input_list_items = list(input_list)
          #print(f"Input list count: {len(input_list_items)}")
          if input_list_items:
            print(f"First input item keys: {list(input_list_items[0].keys()) if hasattr(input_list_items[0], 'keys') else 'Not dict-like'}")
        else:
          print(f"Input list is None or empty")
          email_text += f"No input list for {rpt_r['rpt_type']} report: {rpt_r['Report Description']}\n"
          continue

        print(f"Calling rpt_mgr_new_rpts with rpt_type={rpt_r['rpt_type']}")
        email_text += f"\n Processing {rpt_r['rpt_type']} Reports \n"

        try:
          result = rpt_mgr_new_rpts(rpt_r, input_list, disp_team)

          if result is None:
            print(f"ERROR: rpt_mgr_new_rpts returned None for {rpt_r['rpt_type']}")
            print(f"Report Manager: rpt_mgr_new_rpts Failed, {rpt_r['rpt_type']}, ret_val=None, report_infos=[]")
            email_text += f"Failed to process {rpt_r['rpt_type']} reports: returned None\n"
            continue

          ret_val, report_infos = result
          print(f"rpt_mgr_new_rpts returned successfully")
          #print(f"  ret_val: '{ret_val}'")
          #print(f"  report_infos count: {len(report_infos) if report_infos else 0}")

          if not ret_val and not report_infos:
            print(f"ERROR: rpt_mgr_new_rpts failed for {rpt_r['rpt_type']} - no output")
            print(f"Report Manager: rpt_mgr_new_rpts Failed, {rpt_r['rpt_type']}, ret_val={ret_val}, report_infos={report_infos}")
            email_text += f"Failed to process {rpt_r['rpt_type']} reports: no output generated\n"
          else:
            email_text += ret_val + '\n'
            processed_count += 1
            print(f"SUCCESS: Processed {rpt_r['rpt_type']} reports")

          if rpt_r['email'] and report_infos:
            user_reports[rpt_r['email']].extend(report_infos)
            #print(f"Added {len(report_infos)} reports to email queue for {rpt_r['email']}")

        except Exception as e:
          print(f"ERROR: Exception in rpt_mgr_new_rpts: {str(e)}")
          print(f"rpt_mgr_generate_background: Error processing {rpt_r['rpt_type']}: {str(e)}")
          email_text += f"Error processing {rpt_r['rpt_type']} reports: {str(e)}\n"

      elif rpt_r['rpt_type'] == 'scouting':
        pair_list = rpt_r['pair_list']
        

        print(f"Scoting Reports, Pair list type: {type(pair_list)} pair_list :  \n {pair_list}")
        if pair_list:
          #pair_list_items = list(pair_list)
          pair_list_items = [row['pair'] for row in pair_list]
          #print(f"Pair list count: {len(pair_list_items)}, Pair List Items: {pair_list_items}")
          if pair_list_items:
            print(f"First pair item keys: {list(pair_list_items[0].keys()) if hasattr(pair_list_items[0], 'keys') else 'Not dict-like'}")
        else:
          print(f"Pair list is None or empty")
          email_text += f"No pair list for scouting report: {rpt_r['Report Description']}\n"
          continue

        print(f"Calling rpt_mgr_scouting_rpts with pair_list")
        email_text += '\n Processing scouting Reports \n'

        try:
          result = rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team)

          if result is None:
            print(f"ERROR: rpt_mgr_scouting_rpts returned None")
            print(f"Report Manager: rpt_mgr_scouting_rpts Failed, ret_val=None, report_infos=[]")
            email_text += f"Failed to process scouting reports: returned None\n"
            continue

          ret_val, report_infos = result
          print(f"rpt_mgr_scouting_rpts returned successfully")
          #print(f"  ret_val: '{ret_val}'")
          #print(f"  report_infos count: {len(report_infos) if report_infos else 0}")

          if not ret_val and not report_infos:
            print(f"ERROR: rpt_mgr_scouting_rpts failed - no output")
            print(f"Report Manager: rpt_mgr_scouting_rpts Failed, ret_val={ret_val}, report_infos={report_infos}")
            email_text += f"Failed to process scouting reports: no output generated\n"
          else:
            email_text += ret_val + '\n'
            processed_count += 1
            print(f"SUCCESS: Processed scouting reports")

          if rpt_r['email'] and report_infos:
            user_reports[rpt_r['email']].extend(report_infos)
            #print(f"Added {len(report_infos)} reports to email queue for {rpt_r['email']}")

        except Exception as e:
          print(f"ERROR: Exception in rpt_mgr_scouting_rpts: {str(e)}")
          print(f"rpt_mgr_generate_background: Error processing scouting: {str(e)}")
          email_text += f"Error processing scouting reports: {str(e)}\n"

      else:
        print(f"ERROR: Invalid Report Type: {rpt_r['rpt_type']}")
        print(f"rpt_mgr_generate_background: Invalid Report Type: {rpt_r['rpt_type']}")
        email_text += f"Invalid Report Type: {rpt_r['rpt_type']}\n"

    except Exception as e:
      print(f"ERROR: Exception processing report {report_index + 1}: {str(e)}")
      print(f"rpt_mgr_generate_background: Error processing report: {str(e)}")
      email_text += f"Error processing report {rpt_r.get('Report Description', 'Unknown')}: {str(e)}\n"

  print(f"\n=== SUMMARY ===")
  print(f"Total reports found: {total_reports}")
  print(f"Reports processed successfully: {processed_count}")
  print(f"Users to receive emails: {len(user_reports)}")

  # Send batched user emails
  for user_email, reports in user_reports.items():
        try:
            print(f"Sending email to {user_email} with {len(reports)} reports")
            email_subject = "Beach Internals - Your Generated Reports"
            email_body = f"""
Dear Coach,

The following reports have been generated and are available on Google Drive:

Generated on: {today.strftime('%Y-%m-%d %H:%M:%S')}

"""
            for info in reports:
                email_body += f"""{info['combined']}
"""
            email_body +="""
Best regards,
Beach Internals
info@BeachInternals.com
"""

            email_status = anvil.email.send(
                to=user_email,
                from_address="no-reply",
                subject=email_subject,
                text=email_body
            )
            print(f"Email sent to {user_email}: {email_status}")
            
        except Exception as e:
            print(f"ERROR: Failed to send email to {user_email}: {str(e)}")

  # Last thing, empty the report_data table when we are done
  print("Clearing report_data table...")
  email_text += '\nDeleting all rows from report_data table \n\n'
  try:
    app_tables.report_data.delete_all_rows()
    print("Successfully cleared report_data table")
  except Exception as e:
    print(f"ERROR: Failed to clear report_data table: {str(e)}")

  # Send summary email
  internals_email = 'info@beachinternals.com'
  now1 = datetime.now()
  compute_time = now1 - now
  email_message = email_text + f"Report Manager Completed at: {now1}\nCompute time: {compute_time}\n"
    
  print(f"Sending summary email to {internals_email}")
  try:
    email_status = anvil.email.send(
        to=internals_email,
        from_address="no-reply",
        subject='Beach Internals - Report Manager',
        text=email_message
    )
    print(f"Summary email sent successfully: {email_status}")
  except Exception as e:
    print(f"ERROR: Failed to send summary email: {str(e)}")

  print(f"Report Manager completed. Total runtime: {compute_time}")
  return True

  

  







#-------------------------------------------------------------------------------------------------------
#  Report Manager - All Types of Reports
#-------------------------------------------------------------------------------------------------------

def rpt_mgr_new_rpts(rpt_r, p_list, disp_team):
  today = datetime.now()
  return_text = ''
  report_infos = []

  print(f"=== ENTERING rpt_mgr_new_rpts ===")
  print(f"rpt_type: {rpt_r['rpt_type']}")

  if not p_list:
    print("ERROR: No players/pairs provided")
    return return_text, report_infos

  try:
    for i, p in enumerate(p_list):
      print(f"\n--- Processing item {i+1} of {len(p_list)} ---")
      try:
        p_dict = dict(p)
      except Exception as e:
        print(f"Could not convert item to dict: {str(e)}")
        continue

        # Build report filters
      rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, p)
      print(f"Report filters: {rpt_filters}")

      # Calculate folder paths
      pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), 
                    disp_team.strip(), today.strftime("%Y-%m-%d")]
      json_folder = pdf_folder + ['json']
      lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']

      # Determine player_pair
      if rpt_r['rpt_type'] == 'player':
        player_pair = p['team'] + " " + p['number'] + ' ' + p['shortname']
      elif rpt_r['rpt_type'] == 'pair':
        player_pair = p['pair']
      elif rpt_r['rpt_type'] in ['league', 'dashboard']:
        player_pair = lgy
      else:
        print(f"ERROR: Unknown rpt_type: {rpt_r['rpt_type']}")
        continue

      pdf_name = f"{player_pair} {rpt_r['Report Description']}.pdf"
      if rpt_r['rpt_type'] == 'dashboard':
        pdf_name = f"{player_pair} {disp_team} {rpt_r['Report Description']}.pdf"
      print(f"PDF name: {pdf_name}")

      # Process rpts_inc
      rptname_rows = []
      rpts_inc = rpt_r['rpts_inc'] or []
      for rptname1 in rpts_inc:
        rptname1_dict = dict(rptname1)
        required_fields = ['id', 'report_name', 'rpt_type', 'rpt_form', 'function_name']
        if all(field in rptname1_dict for field in required_fields):
          rptname_rows.append(rptname1)

      if not rptname_rows:
        print(f"ERROR: No valid reports in rpts_inc for {rpt_r['Report Description']}")
        continue

      sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)
      print(f"Sorted reports: {[r['report_name'] for r in sorted_rptnames]}")

      full_rpt_pdf = None
      pdf_files_created = []
      individual_summaries = []

      # Process each report
      for k, rptname in enumerate(sorted_rptnames):
        print(f"\n  --- Processing report {k+1}/{len(sorted_rptnames)}: {rptname['report_name']} ---")

        # Generate report
        report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)
        if not report_id:
          print(f"    ERROR: Failed to generate report ID")
          continue

          # Generate JSON
        json_media = generate_json_report(rptname['rpt_form'], report_id, include_images=False, include_urls=False)
        if isinstance(json_media, dict) and json_media.get('error'):
          print(f"    WARNING: JSON generation failed: {json_media['error']}")
          json_result = None
        else:
          json_name = f"{player_pair} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
          json_result = write_to_nested_folder(json_folder, json_name, json_media)
          pdf_files_created.append({'name': json_name, 'result': json_result})

          # Generate AI summary
        prompt_row = app_tables.ai_prompt_templates.get(
          report_id=rptname['id'],
          hierarchy_level='0',
          coach_id=q.any_of(rpt_r['email'], '')
        )
        if prompt_row:
          summary = generate_ai_summary(json_media.get_bytes().decode('utf-8'), prompt_row['prompt_text'], rpt_r['email'])
          individual_summaries.append(summary)
        else:
          summary = "No summary generated: Prompt not found"
          individual_summaries.append(summary)

          # Generate PDF
        pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
        if isinstance(pdf_result, dict) and pdf_result.get('pdf'):
          pdf1 = pdf_result['pdf']
          # Insert AI summary
          pdf1 = insert_summary_into_pdf(pdf1, summary)
          # Anonymize PDF
          coach_prefs = app_tables.coach_preferences.get(coach_id=rpt_r['email'])
          pii_terms = coach_prefs['pii_terms'] if coach_prefs and 'pii_terms' in coach_prefs else [] # fixed in case there is no row for this coach in coach_prefs
          #pii_terms = app_tables.coach_preferences.get(coach_id=rpt_r['email'])['pii_terms'] or []
          anon_pdf_id = anonymize_pdf(pdf1, pii_terms)
          pdf_files_created.append({'name': pdf1.name + '_anon.pdf', 'result': anon_pdf_id or 'Failed to anonymize'})
        else:
          print(f"    ERROR: PDF generation failed")
          continue

          # Merge PDFs
        if full_rpt_pdf:
          full_rpt_pdf = merge_pdfs(full_rpt_pdf, pdf1, pdf_name=pdf_name)
        else:
          full_rpt_pdf = pdf1

      # Generate roll-up summary
      rollup_prompt_rows = app_tables.ai_prompt_templates.search(
            report_description=rpt_r['Report Description'],
            hierarchy_level='1',
            coach_id=q.any_of(rpt_r['email'], '')
      )
      rollup_prompt_rows = rollup_prompt_rows.order_by('version', ascending=False)
      rollup_prompt = rollup_prompt_rows[0] if rollup_prompt_rows else None

      if rollup_prompt:
        rollup_summary = generate_ai_summary(json.dumps({'summaries': individual_summaries}), rollup_prompt['prompt_text'], rpt_r['email'])
        summary_pdf = create_summary_pdf(rollup_summary, f"{player_pair}_summary_{today.strftime('%Y%m%d_%H%M%S')}.pdf")
        if summary_pdf:
          full_rpt_pdf = merge_pdfs(summary_pdf, full_rpt_pdf, pdf_name=pdf_name)
          # Anonymize summary PDF
          anon_summary_id = anonymize_pdf(summary_pdf, pii_terms)
          pdf_files_created.append({'name': summary_pdf.name + '_anon.pdf', 'result': anon_summary_id or 'Failed to anonymize'})

      # Save combined PDF
      if full_rpt_pdf:
        combined_result = write_to_nested_folder(pdf_folder, pdf_name, full_rpt_pdf)
        anon_pdf_id = anonymize_pdf(full_rpt_pdf, pii_terms)
        pdf_files_created.append({'name': pdf_name + '_anon.pdf', 'result': anon_pdf_id or 'Failed to anonymize'})
        return_text += '\n' + combined_result
      else:
        combined_result = f"No combined PDF generated for {pdf_name}"

        # Collect report info
        report_infos.append({
                'player_pair': player_pair,
                'description': rpt_r['Report Description'],
                'combined': combined_result,
                'individuals': pdf_files_created
        })

  except Exception as e:
    print(f"CRITICAL ERROR in rpt_mgr_new_rpts: {str(e)}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

  return return_text, report_infos


def rpt_mgr_scouting_rpts(rpt_r, pair_list, disp_team):
    today = datetime.now()
    return_text = ''
    report_infos = []

    if not pair_list:
        logging.warning(f"No pairs provided for scouting report: {rpt_r.get('Report Description', 'Unknown')}")
        return return_text, report_infos

    try:
        for p in pair_list:
            disp_pair = p['pair']
            if not disp_pair:
                continue
            player1, player2 = pair_players(disp_pair)

            pdf_folder = [p['league'].strip() + p['gender'].strip() + p['year'].strip(), disp_team.strip(), today.strftime("%Y-%m-%d")]
            json_folder = pdf_folder + ['json']
            lgy = p['league'] + ' | ' + p['gender'] + ' | ' + p['year']

            rptname_rows = [{'report_name': r['report_name'], 'rpt_type': r['rpt_type'], 'order': r['order'], 
                             'function_name': r['function_name'], 'rpt_form': r['rpt_form'], 'id': r['id']} 
                            for r in rpt_r['rpts_inc'] or []]
            sorted_rptnames = sorted(rptname_rows, key=lambda r: r['order'] or 0)
            scouting_rptnames = [r for r in sorted_rptnames if r['rpt_type'] == 'scouting']
            player_rptnames = [r for r in sorted_rptnames if r['rpt_type'] != 'scouting']

            pair_pdf = None
            scouting_pdf_files = []
            individual_summaries = []
            #pii_terms = app_tables.coach_preferences.get(coach_id=rpt_r['email'])['pii_terms'] or []
            coach_prefs = app_tables.coach_preferences.get(coach_id=rpt_r['email'])
            pii_terms = coach_prefs['pii_terms'] if coach_prefs and 'pii_terms' in coach_prefs else []

            # Scouting reports
            rpt_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
            rpt_filters['pair'] = disp_pair
            for rptname in scouting_rptnames:
                report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **rpt_filters)
                if not report_id:
                    continue

                json_media = generate_json_report(rptname['rpt_form'], report_id, include_images=False, include_urls=False)
                if isinstance(json_media, dict) and json_media.get('error'):
                    json_result = None
                else:
                    json_name = f"scouting_{disp_pair} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
                    json_result = write_to_nested_folder(json_folder, json_name, json_media)
                    scouting_pdf_files.append({'name': json_name, 'result': json_result})

                prompt_row = app_tables.ai_prompt_templates.get(
                    report_id=rptname['id'],
                    hierarchy_level='0',
                    coach_id=q.any_of(rpt_r['email'], '')
                )
                summary = generate_ai_summary(json_media.get_bytes().decode('utf-8'), prompt_row['prompt_text'], rpt_r['email']) if prompt_row else "No summary generated"
                individual_summaries.append(summary)

                pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
                if isinstance(pdf_result, dict) and pdf_result.get('pdf'):
                    pdf1 = pdf_result['pdf']
                    pdf1 = insert_summary_into_pdf(pdf1, summary)
                    anon_pdf_id = anonymize_pdf(pdf1, pii_terms)
                    scouting_pdf_files.append({'name': pdf1.name + '_anon.pdf', 'result': anon_pdf_id or 'Failed to anonymize'})
                    pair_pdf = merge_pdfs(pair_pdf, pdf1, pdf_name=f"{disp_pair} scouting_combined_{rpt_r['Report Description']}.pdf") if pair_pdf else pdf1

            # Player reports for player1
            player1_pdf = None
            player1_pdf_files = []
            player_filters = populate_filters_from_rpt_mgr_table(rpt_r, None)
            player_filters['player'] = player1
            for rptname in player_rptnames:
                report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
                if not report_id:
                    continue

                json_media = generate_json_report(rptname['rpt_form'], report_id, include_images=False, include_urls=False)
                if isinstance(json_media, dict) and json_media.get('error'):
                    json_result = None
                else:
                    json_name = f"player_{player1} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
                    json_result = write_to_nested_folder(json_folder, json_name, json_media)
                    player1_pdf_files.append({'name': json_name, 'result': json_result})

                prompt_row = app_tables.ai_prompt_templates.get(
                    report_id=rptname['id'],
                    hierarchy_level='0',
                    coach_id=q.any_of(rpt_r['email'], '')
                )
                summary = generate_ai_summary(json_media.get_bytes().decode('utf-8'), prompt_row['prompt_text'], rpt_r['email']) if prompt_row else "No summary generated"
                individual_summaries.append(summary)

                pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
                if isinstance(pdf_result, dict) and pdf_result.get('pdf'):
                    pdf1 = pdf_result['pdf']
                    pdf1 = insert_summary_into_pdf(pdf1, summary)
                    anon_pdf_id = anonymize_pdf(pdf1, pii_terms)
                    player1_pdf_files.append({'name': pdf1.name + '_anon.pdf', 'result': anon_pdf_id or 'Failed to anonymize'})
                    player1_pdf = merge_pdfs(player1_pdf, pdf1, pdf_name=f"{player1} player_combined_{rpt_r['Report Description']}.pdf") if player1_pdf else pdf1

            # Player reports for player2
            player2_pdf = None
            player2_pdf_files = []
            player_filters['player'] = player2
            for rptname in player_rptnames:
                report_id = generate_and_store_report(rptname['function_name'], lgy, disp_team, **player_filters)
                if not report_id:
                    continue

                json_media = generate_json_report(rptname['rpt_form'], report_id, include_images=False, include_urls=False)
                if isinstance(json_media, dict) and json_media.get('error'):
                    json_result = None
                else:
                    json_name = f"player_{player2} {rptname['report_name']}_{today.strftime('%Y%m%d_%H%M%S')}.json"
                    json_result = write_to_nested_folder(json_folder, json_name, json_media)
                    player2_pdf_files.append({'name': json_name, 'result': json_result})

                prompt_row = app_tables.ai_prompt_templates.get(
                    report_id=rptname['id'],
                    hierarchy_level='0',
                    coach_id=q.any_of(rpt_r['email'], '')
                )
                summary = generate_ai_summary(json_media.get_bytes().decode('utf-8'), prompt_row['prompt_text'], rpt_r['email']) if prompt_row else "No summary generated"
                individual_summaries.append(summary)

                pdf_result = generate_pdf_report(rptname['rpt_form'], report_id)
                if isinstance(pdf_result, dict) and pdf_result.get('pdf'):
                    pdf1 = pdf_result['pdf']
                    pdf1 = insert_summary_into_pdf(pdf1, summary)
                    anon_pdf_id = anonymize_pdf(pdf1, pii_terms)
                    player2_pdf_files.append({'name': pdf1.name + '_anon.pdf', 'result': anon_pdf_id or 'Failed to anonymize'})
                    player2_pdf = merge_pdfs(player2_pdf, pdf1, pdf_name=f"{player2} player_combined_{rpt_r['Report Description']}.pdf") if player2_pdf else pdf1

            # Combine all PDFs
            final_pdf_name = f"{disp_pair} {rpt_r['Report Description']}_combined_{today.strftime('%Y%m%d_%H%M%S')}.pdf"
            final_pdf = None
            pdf_list = [pdf for pdf in [pair_pdf, player1_pdf, player2_pdf] if pdf]
            if pdf_list:
                final_pdf = pdf_list[0]
                for pdf in pdf_list[1:]:
                    final_pdf = merge_pdfs(final_pdf, pdf, pdf_name=final_pdf_name)

                # Generate roll-up summary
                rollup_prompt_rows = app_tables.ai_prompt_templates.search(
                    report_description=rpt_r['Report Description'],
                    hierarchy_level='1',
                    coach_id=q.any_of(rpt_r['email'], '')
                )
                rollup_prompt_rows = rollup_prompt_rows.order_by('version', ascending=False)
                rollup_prompt = rollup_prompt_rows[0] if rollup_prompt_rows else None
                if rollup_prompt:
                    rollup_summary = generate_ai_summary(json.dumps({'summaries': individual_summaries}), rollup_prompt['prompt_text'], rpt_r['email'])
                    summary_pdf = create_summary_pdf(rollup_summary, f"{disp_pair}_summary_{today.strftime('%Y%m%d_%H%M%S')}.pdf")
                    if summary_pdf:
                        final_pdf = merge_pdfs(summary_pdf, final_pdf, pdf_name=final_pdf_name)
                        anon_summary_id = anonymize_pdf(summary_pdf, pii_terms)
                        scouting_pdf_files.append({'name': summary_pdf.name + '_anon.pdf', 'result': anon_summary_id or 'Failed to anonymize'})

            # Save final PDF
            if final_pdf:
                combined_result = write_to_nested_folder(pdf_folder, final_pdf_name, final_pdf)
                anon_pdf_id = anonymize_pdf(final_pdf, pii_terms)
                scouting_pdf_files.append({'name': final_pdf_name + '_anon.pdf', 'result': anon_pdf_id or 'Failed to anonymize'})
                return_text += '\n' + combined_result
            else:
                combined_result = f"No final PDF generated for {final_pdf_name}"

            # Collect report info
            all_individuals = scouting_pdf_files + player1_pdf_files + player2_pdf_files
            report_infos.append({
                'player_pair': disp_pair,
                'description': rpt_r['Report Description'],
                'combined': combined_result,
                'individuals': all_individuals
            })

    except Exception as e:
        logging.error(f"Critical error in rpt_mgr_scouting_rpts: {str(e)}")
        return return_text, report_infos

    return return_text, report_infos







  
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



