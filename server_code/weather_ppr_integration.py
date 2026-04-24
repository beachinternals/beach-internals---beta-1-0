# ============================================================================
# Weather Integration for PPR Conversion
# Add this function to btd_ppr_conversion.py
# ============================================================================

from logger_utils import log_info, log_error, log_critical, log_debug
from weather_integration import get_or_create_weather
import traceback


def add_weather_to_ppr(ppr_df, btd_row):
  """
  Fetch weather and add weather_id and weather data to all rows in ppr_df.
  Called during BTD->PPR conversion after all PPR data is calculated.
  Skips fetch if weather already exists in the dataframe.
  year is always a string.
  """
  log_debug(f"add_weather_to_ppr called for file: {btd_row['filename']}")

  # Get required fields from btd_row
  venue_name = btd_row['venue_name']
  match_date = btd_row['date']
  match_time = btd_row['match_time']

  # If weather already in the PPR rows, skip — no need to fetch again
  already_has_weather = (
    'weather_id' in ppr_df.columns and
    ppr_df['weather_id'].notna().any() and
    (ppr_df['weather_id'] != '').any()
  )
  if already_has_weather:
    log_debug(f"Weather already present for {btd_row['filename']}, skipping fetch")
    return ppr_df

  # Helper to stamp empty weather columns and return
  def set_empty_weather():
    ppr_df['weather_id']       = None
    ppr_df['temperature_f']    = None
    ppr_df['wind_speed_mph']   = None
    ppr_df['wind_gust_mph']    = None
    ppr_df['humidity_percent'] = None
    return ppr_df

  # No weather yet — check if we have enough info to fetch
  if not venue_name or not match_date or not match_time:
    log_info(f"Skipping weather for {btd_row['filename']}: "
             f"venue={venue_name}, date={match_date}, time={match_time} "
             f"(missing fields)")
    return set_empty_weather()

  # Validate match_time format
  valid_times = ["8am-12pm", "12pm-4pm", "4pm-8pm"]
  if match_time not in valid_times:
    log_error(f"Invalid match_time '{match_time}' for {btd_row['filename']}")
    return set_empty_weather()

  # All good — fetch weather
  try:
    log_debug(f"Fetching weather: venue={venue_name}, date={match_date}, time={match_time}")
    weather_id = get_or_create_weather(venue_name, match_date, match_time)

    if not weather_id:
      log_error(f"Failed to fetch weather for {btd_row['filename']}: "
                f"venue={venue_name}, date={match_date}, time={match_time}")
      return set_empty_weather()

    # Got a weather_id — fetch the data row
    from anvil.tables import app_tables
    weather_row = app_tables.weather_data.get_by_id(weather_id)

    if not weather_row:
      log_error(f"Weather ID {weather_id} returned but row not found")
      return set_empty_weather()

    # Stamp all PPR rows with weather data
    ppr_df['weather_id']       = weather_id
    ppr_df['temperature_f']    = weather_row['temperature_f']
    ppr_df['wind_speed_mph']   = weather_row['wind_speed_mph']
    ppr_df['wind_gust_mph']    = weather_row['wind_gust_mph']
    ppr_df['humidity_percent'] = weather_row['humidity_percent']

    log_info(f"✓ Weather added to {len(ppr_df)} PPR rows for {btd_row['filename']}: "
             f"temp={weather_row['temperature_f']}F, wind={weather_row['wind_speed_mph']}mph")

  except Exception as e:
    log_error(f"Exception in add_weather_to_ppr for {btd_row['filename']}: {str(e)}")
    log_error(traceback.format_exc())
    return set_empty_weather()

  return ppr_df


# ============================================================================
# INTEGRATION INSTRUCTIONS
# ============================================================================
"""
To integrate this into btd_ppr_conversion.py:

1. Add import at top:
   from weather_ppr_integration import add_weather_to_ppr

2. In generate_ppr_files_not_background(), add after calc_tactic():
   
   if calc_ppr:
       # ... existing code ...
       ppr_df = calc_tactic(ppr_df)
       
       # NEW: Add weather to PPR
       ppr_df = add_weather_to_ppr(ppr_df, flist_r)
       
       # 4) Error check the ppr file
       ppr_df, no_errors, error_string = error_check_ppr(ppr_df)
       
       # ... rest of code ...

3. Also update flist_r.update() to store weather_id in btd_files:
   
   # Get weather_id from first row (all rows have same weather)
   weather_id = ppr_df['weather_id'].iloc[0] if 'weather_id' in ppr_df.columns else None
   
   flist_r.update(
       ppr_data=ppr_media,
       error_str=error_string,
       no_errors=no_errors,
       ppr_file_date=datetime.now(),
       weather_id=weather_id,  # NEW: Store in btd_files
       weather_fetched=True if weather_id else False  # NEW: Track if fetched
   )
"""