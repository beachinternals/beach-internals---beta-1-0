import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

@anvil.server.callable
def repair_video_id_column():
  return_value = anvil.server.launch_background_task('repair_video_id_column_background')
  return return_value
  
@anvil.server.background_task
def repair_video_id_column_background():
  """
  Go through all btd_files records and populate video_id 
  from the actual CSV data
  """
  import pandas as pd
  import io

  updated_count = 0
  no_video_count = 0
  error_count = 0

  # Get all btd_files records
  all_files = app_tables.btd_files.search()

  for file_row in all_files:
    try:
      # Check if csv_data exists
      if not file_row['csv_data']:
        print(f"No csv_data for {file_row['filename']}")
        continue

      # Read the BTD CSV file
      btd_df = pd.read_csv(io.BytesIO(file_row['csv_data'].get_bytes()))

      # Check if video_id column exists in the CSV
      if 'video_id' in btd_df.columns:
        # Get the first video_id value (they should all be the same in a file)
        video_id_value = btd_df['video_id'].iloc[0] if len(btd_df) > 0 else None

        # Update the btd_files record
        file_row.update(video_id=video_id_value)
        updated_count += 1
        print(f"Updated {file_row['filename']} with video_id: {video_id_value}")
      else:
        # No video_id in the CSV, set to None or "No Video Id"
        file_row.update(video_id=None)
        no_video_count += 1
        print(f"No video_id in CSV for {file_row['filename']}")

    except Exception as e:
      error_count += 1
      print(f"Error processing {file_row['filename']}: {str(e)}")

  result = f"Repair complete!\n"
  result += f"Updated with video_id: {updated_count}\n"
  result += f"No video_id found: {no_video_count}\n"
  result += f"Errors: {error_count}"

  return result
