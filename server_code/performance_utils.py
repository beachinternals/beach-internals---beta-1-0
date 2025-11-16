# ============================================================================
# NIGHT PROCESSING PERFORMANCE SUMMARY - SCHEDULED TASK VERSION
# ============================================================================
# 
# SETUP INSTRUCTIONS:
# 1. Paste this entire code into a Server Module (e.g., "scheduled_tasks.py")
# 2. Go to Server Code → Background Tasks in Anvil
# 3. Click "+ Add Scheduled Task"
# 4. Select function: daily_performance_summary
# 5. Set schedule: Daily at 9:00 AM (or your preferred time)
# 6. Change YOUR_EMAIL_HERE to your actual email
# 7. Save and you're done!
#
# ============================================================================

import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables
import anvil.email
from datetime import datetime, timedelta
import json


# ============================================================================
# MAIN SCHEDULED TASK - This is what the cron job calls
# ============================================================================
@anvil.server.callable
def daily_performance_summary_client():
  anvil.server.background_task(daily_performance_summary())
  return
  
@anvil.server.callable
@anvil.server.background_task
def daily_performance_summary():
  """
    Scheduled task: Runs daily to email performance summary
    
    To schedule in Anvil:
    - Go to Server Code → Background Tasks
    - Add Scheduled Task
    - Function: daily_performance_summary
    - Schedule: Daily at 9:00 AM (or whenever you prefer)
    """

  # CHANGE THIS TO YOUR EMAIL
  YOUR_EMAIL = "your-email@example.com"

  print(f"Starting daily performance summary at {datetime.now()}")

  try:
    # Generate and send the summary
    stats = generate_night_processing_summary(
      send_email=True,
      recipient_email=YOUR_EMAIL
    )

    print(f"✓ Performance summary sent successfully to {YOUR_EMAIL}")
    print(f"  Stats: {stats}")

    return {
      'success': True,
      'timestamp': datetime.now(),
      'stats': stats
    }

  except Exception as e:
    print(f"✗ Error generating performance summary: {e}")

    # Send error notification
    try:
      anvil.email.send(
        to=YOUR_EMAIL,
        subject="⚠️ ERROR: Performance Summary Failed",
        text=f"""
Performance Summary Task Failed
{'='*60}

Error occurred at: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}

Error Details:
{str(e)}

Please check your Anvil app logs for more information.
"""
      )
      print(f"  Error notification sent to {YOUR_EMAIL}")
    except:
      print(f"  Failed to send error notification")

      # Re-raise so it shows as failed in Anvil's task history
    raise


# ============================================================================
# PERFORMANCE SUMMARY LOGIC
# ============================================================================

def generate_night_processing_summary(send_email=True, recipient_email=None):
  """
    Generate and optionally email a summary of last night's processing
    
    Args:
        send_email: If True, sends summary via email
        recipient_email: Email address to send to
    
    Returns: dict with summary stats
    """

  # Get performance logs from last 24 hours
  cutoff_time = datetime.now() - timedelta(hours=24)

  try:
    recent_logs = app_tables.performance_log.search(
      tables.order_by('timestamp', ascending=False),
      timestamp=tables.greater_than_or_equal_to(cutoff_time)
    )
  except Exception as e:
    print(f"Error reading performance logs: {e}")
    recent_logs = []

    # Initialize counters
  stats = {
    'night_processing_time': 0,
    'night_processing_files': 0,
    'report_manager_time': 0,
    'report_manager_count': 0,
    'total_reports_generated': 0,
    'scouting_reports': 0,
    'player_reports': 0,
    'errors_found': 0,
    'total_operations': 0,
    'failed_operations': 0,
    'report_types': {},
    'longest_operation': {'name': '', 'time': 0},
    'slowest_report': {'name': '', 'time': 0}
  }

  # Analyze logs
  for log in recent_logs:
    stats['total_operations'] += 1

    func_name = log['function_name'] or ''
    elapsed = log['elapsed_seconds'] or 0
    success = log['success']

    # Track failures
    if success != 1:
      stats['failed_operations'] += 1

      # Track longest operation
    if elapsed > stats['longest_operation']['time']:
      stats['longest_operation'] = {'name': func_name, 'time': elapsed}

      # Night processing
    if 'night_processing' in func_name.lower():
      stats['night_processing_time'] = elapsed

      # Try to parse file count from context
      try:
        context = json.loads(log['context_json'] or '{}')
        stats['night_processing_files'] = context.get('files_processed', 0)
      except:
        pass

        # Report manager batch jobs
    elif 'rpt_mgr' in func_name:
      stats['report_manager_time'] += elapsed
      stats['report_manager_count'] += 1

      # Individual report generation
    elif 'generate_and_store_report' in func_name:
      stats['total_reports_generated'] += 1

      if elapsed > stats['slowest_report']['time']:
        stats['slowest_report'] = {'name': func_name, 'time': elapsed}

        # Specific report types
    elif 'report_' in func_name:
      # Clean up the name for display
      report_type = func_name.replace('report_', '').replace('_', ' ').title()
      stats['report_types'][report_type] = stats['report_types'].get(report_type, 0) + 1

      if 'scouting' in func_name.lower():
        stats['scouting_reports'] += 1
      elif 'player' in func_name.lower():
        stats['player_reports'] += 1

    # Send email if requested
  if send_email and recipient_email:
    send_summary_email(stats, recipient_email)

  return stats


# ============================================================================
# EMAIL FORMATTING
# ============================================================================

def send_summary_email(stats, recipient_email):
  """Send formatted email with night processing summary"""

  # Format times
  night_proc_time = format_duration(stats['night_processing_time'])
  report_mgr_time = format_duration(stats['report_manager_time'])
  longest_op_time = format_duration(stats['longest_operation']['time'])
  slowest_rpt_time = format_duration(stats['slowest_report']['time'])

  # Calculate success rate
  if stats['total_operations'] > 0:
        success_rate = ((stats['total_operations'] - stats['failed_operations']) / 
                       stats['total_operations'] * 100)
  else:
        success_rate = 100
    
  # Build report types breakdown
  report_breakdown = ""
  if stats['report_types']:
        report_breakdown = "\n" + "\n".join([f"  • {rtype}: {count}" 
                                      for rtype, count in sorted(stats['report_types'].items())])
    
    # Determine status
  has_issues = stats['failed_operations'] > 0 or stats['errors_found'] > 0
  status_line = "⚠️ ATTENTION NEEDED" if has_issues else "✅ All systems normal"
    
    # Build email body
  email_body = f"""
Night Processing Summary - {datetime.now().strftime('%B %d, %Y')}
{'='*60}

OVERNIGHT JOBS COMPLETED:
{'─'*60}
✓ Login Report: Completed
✓ Night Processing: {night_proc_time} ({stats['night_processing_files']} new files)
✓ Data Quality Check: {stats['errors_found']} errors found
✓ Report Manager: {report_mgr_time} ({stats['report_manager_count']} batch runs)

REPORTS GENERATED:
{'─'*60}
Total Reports: {stats['total_reports_generated']}
  • Scouting Reports: {stats['scouting_reports']}
  • Player Reports: {stats['player_reports']}{report_breakdown}

PERFORMANCE DETAILS:
{'─'*60}
Longest Operation: {stats['longest_operation']['name']} ({longest_op_time})
Slowest Report: {stats['slowest_report']['name']} ({slowest_rpt_time})

SYSTEM HEALTH:
{'─'*60}
Total Operations: {stats['total_operations']}
Failed Operations: {stats['failed_operations']}
Success Rate: {success_rate:.1f}%

{status_line}

{'─'*60}
Generated: {datetime.now().strftime('%I:%M %p on %B %d, %Y')}
This is an automated summary from your Beach Volleyball Analytics system.
"""
    
    # Determine subject line based on status
  if has_issues:
        subject = f"⚠️ Night Processing Summary - ATTENTION NEEDED - {datetime.now().strftime('%m/%d/%Y')}"
  else:
        subject = f"✅ Night Processing Summary - {datetime.now().strftime('%m/%d/%Y')}"
    
  try:
        anvil.email.send(
            to=recipient_email,
            subject=subject,
            text=email_body
        )
        print(f"Summary email sent to {recipient_email}")
  except Exception as e:
        print(f"Error sending summary email: {e}")
        raise


def format_duration(seconds):
    """Format seconds into human-readable duration"""
    if seconds == 0:
        return "0 sec"
    elif seconds < 60:
        return f"{seconds:.1f} sec"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} min"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


# ============================================================================
# OPTIONAL: Enhanced performance logging helper
# Use this in your other functions to add context to logs
# ============================================================================

def log_performance_with_context(function_name, elapsed_seconds, success=True, 
                                  error_type="", context_dict=None):
    """
    Enhanced performance logging that includes context information
    
    Example usage in your night processing:
    
        start_time = time.time()
        files_processed = do_processing()
        elapsed = time.time() - start_time
        
        log_performance_with_context(
            function_name='night_processing_backgound',
            elapsed_seconds=elapsed,
            success=True,
            context_dict={'files_processed': files_processed}
        )
    
    Args:
        function_name: Name of the function being logged
        elapsed_seconds: Time taken to execute
        success: Whether the operation succeeded
        error_type: Type of error if failed
        context_dict: Additional context (e.g., {'files_processed': 5})
    """
    
    context_json = json.dumps(context_dict) if context_dict else ""
    
    app_tables.performance_log.add_row(
        timestamp=datetime.now(),
        function_name=function_name,
        elapsed_seconds=elapsed_seconds,
        success=1 if success else 0,
        error_type=error_type or "",
        context_json=context_json
    )


# ============================================================================
# TESTING FUNCTION - Call this manually to test before scheduling
# ============================================================================

@anvil.server.callable
def test_performance_summary():
    """
    Test the performance summary without scheduling
    Call this from the Anvil console to test
    """
    print("Testing performance summary...")
    
    stats = generate_night_processing_summary(
        send_email=False  # Set to True to test email
    )
    
    print("\nGenerated stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    return stats