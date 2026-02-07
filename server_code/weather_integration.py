"""
Weather Integration Module for Beach Volleyball Analytics
Fetches historical weather data from Open-Meteo API and links to performance data

Functions:
1. fetch_weather_for_match() - Fetch weather for a single match
2. update_ppr_weather_batch() - Update all ppr records for a league/year
3. get_or_create_weather() - Helper to check existing weather or fetch new

Usage in Anvil:
- Add this as a new Server Module
- Call from admin page or scheduled tasks
"""

# ============================================================================
# ANVIL IMPORTS
# ============================================================================
import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import requests
from datetime import datetime, timedelta
import json
import traceback

# ============================================================================
# PERFORMANCE MONITORING IMPORTS
# ============================================================================
from server_functions import (
monitor_performance,
MONITORING_LEVEL_OFF,
MONITORING_LEVEL_CRITICAL,
MONITORING_LEVEL_IMPORTANT,
MONITORING_LEVEL_DETAILED,
MONITORING_LEVEL_VERBOSE
)

# ============================================================================
# LOGGING IMPORTS
# ============================================================================
from logger_utils import log_info, log_error, log_critical, log_debug


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_fetch_hour(match_time):
  """
    Convert match time range to the midpoint hour for weather fetch
    
    Args:
        match_time (str): One of "8am-12pm", "12pm-4pm", "4pm-8pm"
        
    Returns:
        int: Hour in 24-hour format (10, 14, or 18)
    """
  time_map = {
    "8am-12pm": 10,
    "12pm-4pm": 14,
    "4pm-8pm": 18
  }
  return time_map.get(match_time, 14)  # Default to 14 (2pm) if not found


def degrees_to_direction(degrees):
  """
    Convert wind direction in degrees to cardinal direction
    
    Args:
        degrees (float): Wind direction in degrees (0-360)
        
    Returns:
        str: Cardinal direction (N, NE, E, SE, S, SW, W, NW)
    """
  if degrees is None:
    return "N/A"

  directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
  index = round(degrees / 45) % 8
  return directions[index]


def celsius_to_fahrenheit(celsius):
  """Convert Celsius to Fahrenheit"""
  if celsius is None:
    return None
  return round((celsius * 9/5) + 32, 1)


def kmh_to_mph(kmh):
  """Convert km/h to mph"""
  if kmh is None:
    return None
  return round(kmh * 0.621371, 1)


def get_weather_description(weather_code):
  """
    Convert Open-Meteo weather code to human-readable description
    https://open-meteo.com/en/docs
    """
  weather_codes = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    73: "Moderate snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail"
  }
  return weather_codes.get(weather_code, "Unknown")


# ============================================================================
# MAIN FUNCTIONS
# ============================================================================

@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def fetch_weather_for_match(venue_id, match_date, match_time):

  """
    Fetch weather data for a single match and store in weather table
    
    Args:
        venue_name (str): Name of the venue (must exist in venue table)
        match_date (datetime.date): Date of the match
        match_time (str): Time range ("8am-12pm", "12pm-4pm", "4pm-8pm")
        
    Returns:
        dict: {
            'success': bool,
            'weather_id': str or None,
            'message': str,
            'data': dict (weather data if successful)
        }
    """

  log_debug(f"fetch_weather_for_match called: venue={venue_id}, date={match_date}, time={match_time}")

  try:
    # 1. Validate inputs
    if not venue_id or not match_date or not match_time:
      log_error(f"Missing required parameters: venue={venue_name}, date={match_date}, time={match_time}")
      return {
        'success': False,
        'weather_id': None,
        'message': 'Missing required parameters',
        'data': None
      }

      # 2. Get venue information
    venue = app_tables.venue.get(venue_id=venue_id)
    if not venue:
      log_error(f"Venue not found: {venue_id}")
      return {
        'success': False,
        'weather_id': None,
        'message': f'Venue not found: {venue_id}',
        'data': None
      }

    venue_name = venue['venue_name']  # Extract for logging
    lat = venue['latitude']
    lon = venue['longitude']

    log_debug(f"Found venue: {venue_name} at lat={lat}, lon={lon}")

    # 3. Check if weather already exists
    fetch_hour = get_fetch_hour(match_time)
    existing_weather = app_tables.weather_data.get(
      venue_id=venue_id,
      weather_date=match_date,
      time_range=match_time
    )

    if existing_weather:
      log_info(f"Weather data already exists for {venue_name} on {match_date} at {match_time} (cached)")
      return {
        'success': True,
        'weather_id': existing_weather.get_id(),
        'message': 'Weather data already exists (using cached)',
        'data': {
          'temperature_f': existing_weather['temperature_f'],
          'wind_speed_mph': existing_weather['wind_speed_mph'],
          'humidity_percent': existing_weather['humidity_percent']
        }
      }

      # 4. Fetch from Open-Meteo API
    # Format date for API (YYYY-MM-DD)
    date_str = match_date.strftime('%Y-%m-%d')

    log_info(f"Fetching weather from Open-Meteo API for {venue_name} on {date_str} at hour {fetch_hour}")

    # Build API URL
    url = (
      f"https://archive-api.open-meteo.com/v1/archive?"
      f"latitude={lat}&longitude={lon}"
      f"&start_date={date_str}&end_date={date_str}"
      f"&hourly=temperature_2m,relative_humidity_2m,precipitation,"
      f"weather_code,cloud_cover,wind_speed_10m,wind_direction_10m,"
      f"wind_gusts_10m"
      f"&temperature_unit=celsius"
      f"&wind_speed_unit=kmh"
      f"&timezone=auto"
    )

    log_debug(f"API URL: {url}")

    response = requests.get(url, timeout=10)

    if response.status_code != 200:
      log_error(f"API request failed with status {response.status_code} for {venue_name} on {date_str}")
      return {
        'success': False,
        'weather_id': None,
        'message': f'API request failed with status {response.status_code}',
        'data': None
      }

    data = response.json()

    # 5. Parse the response for the specific hour
    hourly_data = data.get('hourly', {})
    times = hourly_data.get('time', [])

    # Find the index for our target hour
    target_time = f"{date_str}T{fetch_hour:02d}:00"

    log_debug(f"Looking for data at target time: {target_time}")

    if target_time not in times:
      log_error(f"No weather data available for {target_time} at {venue_name}")
      return {
        'success': False,
        'weather_id': None,
        'message': f'No data available for {target_time}',
        'data': None
      }

    idx = times.index(target_time)

    # Extract weather data
    temp_c = hourly_data.get('temperature_2m', [])[idx]
    humidity = hourly_data.get('relative_humidity_2m', [])[idx]
    precipitation = hourly_data.get('precipitation', [])[idx]
    weather_code = hourly_data.get('weather_code', [])[idx]
    cloud_cover = hourly_data.get('cloud_cover', [])[idx]
    wind_speed_kmh = hourly_data.get('wind_speed_10m', [])[idx]
    wind_direction = hourly_data.get('wind_direction_10m', [])[idx]
    wind_gust_kmh = hourly_data.get('wind_gusts_10m', [])[idx]

    # Convert units
    temp_f = celsius_to_fahrenheit(temp_c)
    wind_speed_mph = kmh_to_mph(wind_speed_kmh)
    wind_gust_mph = kmh_to_mph(wind_gust_kmh)
    wind_dir_text = degrees_to_direction(wind_direction)
    weather_desc = get_weather_description(weather_code)

    log_info(f"Weather data retrieved: {temp_f}°F, {wind_speed_mph}mph wind, {humidity}% humidity, {weather_desc}")

    # 6. Store in weather table
    weather_row = app_tables.weather_data.add_row(
      venue_name=venue_name,
      weather_date=match_date,
      time_range=match_time,
      fetch_hour=fetch_hour,
      temperature_f=temp_f,
      temperature_c=temp_c,
      wind_speed_mph=wind_speed_mph,
      wind_gust_mph=wind_gust_mph,
      wind_direction_degrees=wind_direction,
      wind_direction_text=wind_dir_text,
      humidity_percent=humidity,
      cloud_cover_percent=cloud_cover,
      uv_index=None,  # Open-Meteo archive doesn't provide UV index
      participation_mm=precipitation,
      weather_code=weather_code,
      weather_description=weather_desc,
      data_source="Open-Meteo",
      fetched_at=datetime.now(),
      api_success=True
    )

    log_info(f"✓ Weather data stored successfully for {venue_name} on {date_str}, weather_id={weather_row.get_id()}")

    return {
      'success': True,
      'weather_id': weather_row.get_id(),
      'message': 'Weather data fetched and stored successfully',
      'data': {
        'temperature_f': temp_f,
        'wind_speed_mph': wind_speed_mph,
        'wind_gust_mph': wind_gust_mph,
        'humidity_percent': humidity,
        'weather_description': weather_desc
      }
    }

  except requests.exceptions.Timeout:
    log_error(f"API request timeout for {venue_name} on {match_date}")
    return {
      'success': False,
      'weather_id': None,
      'message': 'API request timeout - please try again',
      'data': None
    }
  except requests.exceptions.RequestException as e:
    log_error(f"API request error for {venue_name} on {match_date}: {str(e)}")
    return {
      'success': False,
      'weather_id': None,
      'message': f'API request failed: {str(e)}',
      'data': None
    }
  except Exception as e:
    log_error(f"Unexpected error in fetch_weather_for_match: {str(e)}")
    log_error(traceback.format_exc())
    return {
      'success': False,
      'weather_id': None,
      'message': f'Error: {str(e)}',
      'data': None
    }


@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_or_create_weather(venue_name, match_date, match_time):
  """
    Get existing weather record or create new one if needed
    
    Args:
        venue_name (str): Name of the venue
        match_date (datetime.date): Date of the match
        match_time (str): Time range
        
    Returns:
        str or None: weather_id if successful, None if failed
    """
  log_debug(f"get_or_create_weather: {venue_name}, {match_date}, {match_time}")

  # Check if weather already exists
  existing = app_tables.weather_data.get(
    venue_name=venue_name,
    weather_date=match_date,
    time_range=match_time
  )

  if existing:
    log_debug(f"Found existing weather record: {existing.get_id()}")
    return existing.get_id()

    # Fetch new weather data
  log_info(f"No existing weather found, fetching new data for {venue_name} on {match_date}")
  result = fetch_weather_for_match(venue_name, match_date, match_time)

  if result['success']:
    log_info(f"✓ Weather created successfully: {result['weather_id']}")
    return result['weather_id']
  else:
    log_error(f"✗ Weather fetch failed: {result['message']}")
    return None


@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def update_ppr_weather_batch(league, year):
  """
    Update weather_id for all ppr_csv_tables records for a given league/year
    
    This function:
    1. Finds all ppr records that need weather data
    2. Groups by unique (venue_name, date, match_time)
    3. Fetches weather if needed
    4. Updates all matching ppr records with weather_id
    
    Args:
        league (str): League identifier (e.g., "FIVB", "NCAA")
        year (int): Year (e.g., 2024)
        
    Returns:
        dict: {
            'success': bool,
            'total_records': int,
            'records_updated': int,
            'records_skipped': int,
            'unique_weather_fetched': int,
            'errors': list,
            'details': list
        }
    """
  log_info(f"Starting batch weather update for {league} {year}")

  try:
    stats = {
      'success': True,
      'total_records': 0,
      'records_updated': 0,
      'records_skipped': 0,
      'unique_weather_fetched': 0,
      'errors': [],
      'details': []
    }

    # 1. Get all ppr records for this league/year
    ppr_records = app_tables.ppr_csv_tables.search(
      league=league,
      year=year
    )

    stats['total_records'] = len(ppr_records)

    log_info(f"Found {stats['total_records']} PPR records for {league} {year}")

    if stats['total_records'] == 0:
      log_info(f"No records found for {league} {year}")
      stats['details'].append(f"No records found for {league} {year}")
      return stats

      # 2. Group records by unique (venue_name, date, match_time)
      # First, collect records that need weather
    needs_weather = []

    log_debug("Scanning records for missing weather data...")

    for ppr in ppr_records:
      # Skip if already has weather_id
      if ppr['weather_id']:
        stats['records_skipped'] += 1
        continue

        # Skip if missing required fields
      venue_name = ppr['venue_name'] if 'venue_name' in ppr else None
      match_time = ppr['match_time'] if 'match_time' in ppr else None
      match_date = ppr['date'] if 'date' in ppr else None

      if not venue_name or not match_time or not match_date:
        stats['records_skipped'] += 1
        log_debug(f"Skipped record {ppr.get_id()}: missing venue_name, match_time, or date")
        stats['details'].append(
          f"Skipped record {ppr.get_id()}: missing venue_name, match_time, or date"
        )
        continue

        # Validate match_time format
      if match_time not in ["8am-12pm", "12pm-4pm", "4pm-8pm"]:
        stats['records_skipped'] += 1
        log_error(f"Invalid match_time '{match_time}' for record {ppr.get_id()}")
        stats['details'].append(
          f"Skipped record {ppr.get_id()}: invalid match_time '{match_time}'"
        )
        continue

      needs_weather.append({
        'record': ppr,
        'venue_name': venue_name,
        'date': match_date,
        'match_time': match_time
      })

    log_info(f"Found {len(needs_weather)} records needing weather data, {stats['records_skipped']} skipped")

    # 3. Group by unique combinations
    unique_matches = {}
    for item in needs_weather:
      key = (item['venue_name'], item['date'], item['match_time'])
      if key not in unique_matches:
        unique_matches[key] = []
      unique_matches[key].append(item['record'])

      log_info(f"Found {len(unique_matches)} unique venue/date/time combinations")
      stats['details'].append(
            f"Found {len(unique_matches)} unique venue/date/time combinations"
        )
        
        # 4. Fetch weather for each unique combination
      for (venue_name, match_date, match_time), records in unique_matches.items():
            try:
                log_debug(f"Processing: {venue_name}, {match_date}, {match_time} ({len(records)} records)")
                
                # Get or create weather record
                weather_id = get_or_create_weather(venue_name, match_date, match_time)
                
                if weather_id:
                    # Update all ppr records with this weather_id
                    for record in records:
                        record['weather_id'] = weather_id
                        stats['records_updated'] += 1
                    
                    stats['unique_weather_fetched'] += 1
                    log_info(f"✓ Weather linked for {venue_name} on {match_date} ({match_time}) - updated {len(records)} records")
                    stats['details'].append(
                        f"✓ Weather fetched for {venue_name} on {match_date} "
                        f"({match_time}) - updated {len(records)} records"
                    )
                else:
                    error_msg = f"✗ Failed to fetch weather for {venue_name} on {match_date} ({match_time}) - {len(records)} records not updated"
                    log_error(error_msg)
                    stats['errors'].append(error_msg)
                    
            except Exception as e:
                error_msg = (
                    f"Error processing {venue_name} on {match_date} ({match_time}): "
                    f"{str(e)}"
                )
                log_error(error_msg)
                log_error(traceback.format_exc())
                stats['errors'].append(error_msg)
        
        # 5. Final summary
      summary_msg = (
            f"Batch update complete: Updated {stats['records_updated']} of "
            f"{stats['total_records']} records for {league} {year}"
        )
      log_info(summary_msg)
      stats['details'].append(f"\n{summary_msg}")
        
      if stats['errors']:
            stats['success'] = False
            log_error(f"Batch update completed with {len(stats['errors'])} errors")
      else:
            log_info(f"✓ Batch update completed successfully with no errors")
        
      return stats
        
  except Exception as e:
        error_msg = f"Fatal error in update_ppr_weather_batch: {str(e)}"
        log_critical(error_msg)
        log_critical(traceback.format_exc())
        return {
            'success': False,
            'total_records': 0,
            'records_updated': 0,
            'records_skipped': 0,
            'unique_weather_fetched': 0,
            'errors': [f"Fatal error: {str(e)}"],
            'details': []
        }


# ============================================================================
# ADMIN/TESTING FUNCTIONS
# ============================================================================

@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def test_weather_fetch(venue_name="Stetson, Cooper Volleyball Courts", 
                       test_date="2024-03-15", 
                       match_time="12pm-4pm"):
    """
    Test function to verify weather fetching works
    
    Returns formatted result for display in admin interface
    """
    log_info(f"Test weather fetch called: {venue_name}, {test_date}, {match_time}")
    
    from datetime import datetime
    
    # Convert string date to datetime.date
    date_obj = datetime.strptime(test_date, "%Y-%m-%d").date()
    
    result = fetch_weather_for_match(venue_name, date_obj, match_time)
    
    log_info(f"Test result: success={result['success']}, message={result['message']}")
    
    return result


@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_weather_stats():
    """
    Get statistics about weather data in the system
    
    Returns:
        dict: Statistics about weather records
    """
    try:
        log_debug("Retrieving weather statistics...")
        
        total_weather = len(list(app_tables.weather_data.search()))
        
        ppr_with_weather = len(list(
            app_tables.ppr_csv_tables.search(tables.order_by('date'))
        ))
        
        ppr_total = len(list(app_tables.ppr_csv_tables.search()))
        
        stats = {
            'total_weather_records': total_weather,
            'ppr_total_records': ppr_total,
            'ppr_with_weather': ppr_with_weather,
            'ppr_without_weather': ppr_total - ppr_with_weather
        }
        
        log_info(f"Weather stats: {total_weather} weather records, {ppr_with_weather}/{ppr_total} ppr records with weather")
        
        return stats
        
    except Exception as e:
        log_error(f"Error getting weather stats: {str(e)}")
        log_error(traceback.format_exc())
        return {
            'total_weather_records': 0,
            'ppr_total_records': 0,
            'ppr_with_weather': 0,
            'ppr_without_weather': 0
        }