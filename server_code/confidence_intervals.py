"""
Anvil Integration Module for Confidence Intervals

This module provides ready-to-use functions for adding confidence intervals
to your existing volleyball performance reports on Anvil.works.

QUICK START:
1. Copy this entire file into a new Server Module in Anvil
2. Name it 'confidence_intervals' 
3. Import into your existing reports: 
   from confidence_intervals import add_ci_to_fbhe_result

"""

import math
from scipy import stats


def wilson_score_ci(successes, attempts, confidence=0.95):
  """Wilson Score Confidence Interval - robust for all sample sizes."""
  if attempts == 0:
    return {'lower': 0, 'upper': 0, 'margin': 0}

  p = successes / attempts
  n = attempts
  z = stats.norm.ppf(1 - (1 - confidence) / 2)
  z_squared = z * z

  denominator = 1 + z_squared / n
  center = (p + z_squared / (2 * n)) / denominator
  margin = (z * math.sqrt(p * (1 - p) / n + z_squared / (4 * n * n))) / denominator

  return {
    'lower': max(0, center - margin),
    'upper': min(1, center + margin),
    'margin': margin
  }


def add_ci_to_fbhe_result(fbhe_result_obj, confidence=0.95):
  """
    Add confidence interval information to an existing fbhe_obj result.
    
    Parameters:
    -----------
    fbhe_result_obj : object
        Your existing fbhe_obj result with .fbhe, .kills, .errors, .attempts
    confidence : float
        Confidence level (0.95 = 95%)
    
    Returns:
    --------
    dict : Original results plus CI information:
        {
            'fbhe': original FBHE value,
            'kills': kills,
            'errors': errors,
            'attempts': attempts,
            'ci_lower': lower bound of FBHE,
            'ci_upper': upper bound of FBHE,
            'ci_margin': margin of error,
            'ci_percent': confidence level as percent (e.g., 95),
            'ci_display': formatted string like "0.333 (95% CI: 0.124 to 0.542)"
        }
    
    Usage in your existing code:
    ----------------------------
    # Your existing code:
    fbhe_result = fbhe_obj(ppr_df, disp_player, 'att', True)
    
    # Add CI:
    fbhe_with_ci = add_ci_to_fbhe_result(fbhe_result)
    
    # Use in your report:
    print(f"FBHE: {fbhe_with_ci['ci_display']}")
    """

  fbhe = fbhe_result_obj.fbhe
  kills = fbhe_result_obj.kills
  errors = fbhe_result_obj.errors
  attempts = fbhe_result_obj.attempts

  if attempts == 0:
    return {
      'fbhe': 0,
      'kills': kills,
      'errors': errors,
      'attempts': 0,
      'ci_lower': 0,
      'ci_upper': 0,
      'ci_margin': 0,
      'ci_percent': int(confidence * 100),
      'ci_display': '0.000 (insufficient data)'
    }

    # Transform FBHE from [-1, 1] to [0, 1] scale
  fbhe_scaled = (fbhe + 1) / 2
  successes_scaled = fbhe_scaled * attempts

  # Calculate CI
  ci = wilson_score_ci(successes_scaled, attempts, confidence)

  # Transform back to FBHE scale
  ci_lower = ci['lower'] * 2 - 1
  ci_upper = ci['upper'] * 2 - 1
  ci_margin = ci['margin'] * 2

  ci_percent = int(confidence * 100)
  ci_display = f"{fbhe:.3f} ({ci_percent}% CI: {ci_lower:.3f} to {ci_upper:.3f})"

  return {
    'fbhe': fbhe,
    'kills': kills,
    'errors': errors,
    'attempts': attempts,
    'ci_lower': ci_lower,
    'ci_upper': ci_upper,
    'ci_margin': ci_margin,
    'ci_percent': ci_percent,
    'ci_display': ci_display,
    # Keep original attributes for backward compatibility
    'video_link': fbhe_result_obj.video_link if hasattr(fbhe_result_obj, 'video_link') else None,
    'fbso': fbhe_result_obj.fbso if hasattr(fbhe_result_obj, 'fbso') else None
  }


def calculate_proportion_ci(successes, attempts, confidence=0.95):
  """
    Calculate confidence interval for any proportion (e.g., Good Pass %).
    
    Parameters:
    -----------
    successes : int
        Number of successful outcomes
    attempts : int
        Total attempts
    confidence : float
        Confidence level
    
    Returns:
    --------
    dict : {
        'proportion': observed proportion,
        'percent': proportion as percentage,
        'ci_lower': lower bound,
        'ci_upper': upper bound,
        'ci_margin': margin of error,
        'ci_display': formatted string
    }
    
    Usage Example:
    --------------
    # For Good Pass Percentage:
    good_passes = 25
    total_attempts = 30
    result = calculate_proportion_ci(good_passes, total_attempts)
    print(result['ci_display'])  # "83.3% (95% CI: 65.3% to 94.4%)"
    """
  if attempts == 0:
    return {
      'proportion': 0,
      'percent': 0,
      'ci_lower': 0,
      'ci_upper': 0,
      'ci_margin': 0,
      'ci_display': '0.0% (insufficient data)'
    }

  proportion = successes / attempts
  ci = wilson_score_ci(successes, attempts, confidence)

  ci_percent = int(confidence * 100)
  ci_display = f"{proportion*100:.1f}% ({ci_percent}% CI: {ci['lower']*100:.1f}% to {ci['upper']*100:.1f}%)"

  return {
    'proportion': proportion,
    'percent': proportion * 100,
    'ci_lower': ci['lower'],
    'ci_upper': ci['upper'],
    'ci_margin': ci['margin'],
    'ci_display': ci_display
  }


def get_reliability_indicator(attempts):
  """
    Get a simple reliability indicator based on number of attempts.
    
    Returns a string describing how reliable the metric is:
    - "Very Limited" : < 10 attempts
    - "Limited" : 10-19 attempts  
    - "Moderate" : 20-49 attempts
    - "Good" : 50-99 attempts
    - "Very Good" : 100+ attempts
    
    Usage:
    ------
    reliability = get_reliability_indicator(25)
    print(f"Data reliability: {reliability}")  # "Moderate"
    """
  if attempts < 10:
    return "Very Limited"
  elif attempts < 20:
    return "Limited"
  elif attempts < 50:
    return "Moderate"
  elif attempts < 100:
    return "Good"
  else:
    return "Very Good"


def format_fbhe_with_ci_for_table(fbhe, attempts, kills=None, errors=None):
  """
    Format FBHE with confidence interval for display in tables.
    
    Returns a formatted string suitable for table cells.
    
    Parameters:
    -----------
    fbhe : float
        FBHE value
    attempts : int
        Number of attempts
    kills : int (optional)
        If provided, will calculate more accurate CI
    errors : int (optional)
        If provided, will calculate more accurate CI
    
    Returns:
    --------
    str : Formatted string like "0.333 ± 0.123 (n=30)"
    
    Usage in your reports_player.py:
    --------------------------------
    # In your existing code where you set fbhe_table values:
    fbhe_table.at[0, 'Zone 1'] = format_fbhe_with_ci_for_table(
        fbhe_result.fbhe, 
        fbhe_result.attempts,
        fbhe_result.kills,
        fbhe_result.errors
    )
    """
  if attempts == 0:
    return "— (no data)"

    # Calculate CI if we have kills and errors
  if kills is not None and errors is not None:
    fbhe_scaled = (fbhe + 1) / 2
    successes_scaled = fbhe_scaled * attempts
    ci = wilson_score_ci(successes_scaled, attempts)
    margin = ci['margin'] * 2
  else:
    # Rough estimate based on attempts only
    # Assumes typical variance
    margin = 1.96 * math.sqrt(0.25 / attempts)  # Conservative estimate

    # Format based on attempts
  if attempts < 10:
    return f"{fbhe:.2f} ± {margin:.2f} (n={attempts}, limited)"
  else:
    return f"{fbhe:.3f} ± {margin:.3f} (n={attempts})"


def add_ci_column_to_dataframe(df, metric_col, attempts_col, output_col='ci_display'):
  """
    Add confidence interval display column to a pandas DataFrame.
    
    Useful for batch processing multiple rows.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Your dataframe
    metric_col : str
        Column name containing the metric (e.g., 'fbhe')
    attempts_col : str
        Column name containing attempts
    output_col : str
        Name for new column with CI display
    
    Returns:
    --------
    pandas.DataFrame : Original df with new CI column added
    
    Usage:
    ------
    # Assuming you have a dataframe with fbhe and attempts columns:
    df = add_ci_column_to_dataframe(df, 'fbhe', 'attempts')
    # Now df has a new 'ci_display' column
    """
  def calculate_ci_for_row(row):
    metric = row[metric_col]
    attempts = row[attempts_col]

    if attempts == 0:
      return "—"

      # Transform and calculate
    metric_scaled = (metric + 1) / 2
    successes_scaled = metric_scaled * attempts
    ci = wilson_score_ci(successes_scaled, attempts)

    ci_lower = ci['lower'] * 2 - 1
    ci_upper = ci['upper'] * 2 - 1

    return f"{metric:.3f} (CI: {ci_lower:.3f} to {ci_upper:.3f})"

  df[output_col] = df.apply(calculate_ci_for_row, axis=1)
  return df


def should_display_metric(attempts, min_attempts=10):
  """
    Determine if a metric should be displayed based on sample size.
    
    Returns True if attempts >= min_attempts, False otherwise.
    
    Usage:
    ------
    if should_display_metric(fbhe_result.attempts, min_attempts=15):
        # Display the metric
        print(f"FBHE: {fbhe_result.fbhe:.3f}")
    else:
        print("FBHE: Insufficient data")
    """
  return attempts >= min_attempts


# Example integration with your existing report code
def example_integration():
  """
    Example showing how to integrate CI into your existing reports_player.py code.
    """
  print("EXAMPLE INTEGRATION PATTERNS:")
  print("=" * 70)

  print("\n1. Simple addition to existing fbhe_obj result:")
  print("-" * 70)
    print("""
    # BEFORE (your existing code):
    fbhe_result = fbhe_obj(ppr_df, disp_player, 'att', True)
    fbhe_table.at[0, 'All'] = fbhe_result.fbhe
    
    # AFTER (with CI):
    fbhe_result = fbhe_obj(ppr_df, disp_player, 'att', True)
    fbhe_with_ci = add_ci_to_fbhe_result(fbhe_result)
    fbhe_table.at[0, 'All'] = fbhe_with_ci['ci_display']
    """)
    
    print("\n2. Add reliability indicator:")
    print("-" * 70)
    print("""
    fbhe_result = fbhe_obj(ppr_df, disp_player, 'att', True)
    reliability = get_reliability_indicator(fbhe_result.attempts)
    
    if reliability in ['Very Limited', 'Limited']:
        # Show warning or don't display
        fbhe_table.at[0, 'All'] = f"{fbhe_result.fbhe:.3f} *"
        # Add footnote: "* Based on limited data"
    else:
        fbhe_with_ci = add_ci_to_fbhe_result(fbhe_result)
        fbhe_table.at[0, 'All'] = fbhe_with_ci['ci_display']
    """)
    
    print("\n3. For Good Pass Percentage:")
    print("-" * 70)
    print("""
    # BEFORE:
    oos_vector = count_out_of_system(ppr_df, disp_player, 'srv')
    good_pass_pct = 1 - oos_vector[1]
    fbhe_table.at[6, 'All'] = str('{:.1%}').format(good_pass_pct)
    
    # AFTER (with CI):
    oos_vector = count_out_of_system(ppr_df, disp_player, 'srv')
    good_passes = int((1 - oos_vector[1]) * oos_vector[2])  # Calculate good passes
    ci_result = calculate_proportion_ci(good_passes, oos_vector[2])
    fbhe_table.at[6, 'All'] = ci_result['ci_display']
    """)


if __name__ == "__main__":
    example_integration()
    
    print("\n\nTEST WITH SAMPLE DATA:")
    print("=" * 70)
    
    # Simulate fbhe_obj result
    class FakeFbheResult:
        def __init__(self, kills, errors, attempts):
            self.kills = kills
            self.errors = errors
            self.attempts = attempts
            self.fbhe = (kills - errors) / attempts if attempts > 0 else 0
            self.fbso = kills / attempts if attempts > 0 else 0
            self.video_link = "http://example.com/video"
    
    # Test with different sample sizes
    test_cases = [
        (15, 5, 30, "Moderate sample"),
        (8, 2, 20, "Small sample"),
        (50, 20, 100, "Large sample"),
        (3, 1, 5, "Very small sample")
    ]
    
    print(f"\n{'Scenario':<20} {'Attempts':<10} {'FBHE':<10} {'Display with CI':<50}")
    print("-" * 90)
    
    for kills, errors, attempts, scenario in test_cases:
        fake_result = FakeFbheResult(kills, errors, attempts)
        result_with_ci = add_ci_to_fbhe_result(fake_result)
        reliability = get_reliability_indicator(attempts)
        
        print(f"{scenario:<20} {attempts:<10} {result_with_ci['fbhe']:.3f}      {result_with_ci['ci_display']}")
        print(f"{'':>20} {'Reliability: ' + reliability}")
        print()