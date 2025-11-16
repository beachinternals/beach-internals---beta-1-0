# ============================================================================
# NIGHT PROCESSING PERFORMANCE SUMMARY - WITH ERROR LOG ANALYSIS
# Module: performance_utils
# ============================================================================

import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables
import anvil.email
from datetime import datetime, timedelta, timezone
import json
from logger_utils import log_info, log_error, log_critical, log_debug

# ============================================================================
# MAIN SCHEDULED TASK - This is what the cron job calls
# ============================================================================

@anvil.server.callable
def daily_performance_summary_client():
  """Client-callable function that launches the background task"""
  log_info("Launching background task: daily performance summary")

  # Launch the background task
  task = anvil.server.launch_background_task('daily_performance_summary')

  log_info(f"Background task launched: {task.get_id()}")
  return {'status': 'launched', 'task_id': task.get_id()}


@anvil.server.background_task
def daily_performance_summary():
  """
    Background task: Runs daily to email performance summary
    
    To schedule in Anvil:
    - Go to Server Code → Background Tasks
    - Add Scheduled Task
    - Function: daily_performance_summary
    - Schedule: Daily at 9:00 AM (or whenever you prefer)
    """
  log_info('running Daily Performance Summary')
  print(f"Starting daily performance summary at {datetime.now()}")

  # CHANGE THIS TO YOUR EMAIL
  YOUR_EMAIL = "info@beachinternals.com"

  try:
    # Generate and send the summary
    stats = generate_night_processing_summary(
      send_email=True,
      recipient_email=YOUR_EMAIL
    )

    log_info(f"Performance summary sent successfully to {YOUR_EMAIL}")
    print(f"✓ Performance summary sent successfully to {YOUR_EMAIL}")
    print(f"  Stats: {stats}")

    return {
      'success': True,
      'timestamp': datetime.now(),
      'stats': stats
    }

  except Exception as e:
    log_error(f"Error generating performance summary: {e}")
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
      log_info(f"Error notification sent to {YOUR_EMAIL}")
    except Exception as email_error:
      log_error(f"Failed to send error notification: {email_error}")

    raise


# ============================================================================
# ERROR LOG ANALYSIS
# ============================================================================

def analyze_error_logs(cutoff_time):
  """
    Analyze error logs from the last 24 hours
    
    Args:
        cutoff_time: Datetime threshold for filtering recent errors
    
    Returns: dict with error statistics
    """
  try:
    # Get all error logs
    all_errors = list(app_tables.error_log.search(
      tables.order_by('timestamp', ascending=False)
    ))[:200]  # Limit to 200 most recent

    # Filter for recent errors
    recent_errors = [err for err in all_errors 
                     if err['timestamp'] and err['timestamp'] >= cutoff_time]

    log_info(f"Found {len(recent_errors)} errors from last 24 hours (out of {len(all_errors)} total)")

    # Categorize errors
    error_stats = {
      'total_errors': len(recent_errors),
      'critical_errors': 0,
      'regular_errors': 0,
      'errors_by_source': {},
      'errors_by_type': {},
      'recent_errors_list': []
    }

    for error in recent_errors:
      severity = (error['severity'] or 'error').lower()
      source = error['source'] or 'Unknown'
      error_type = error['error_type'] or 'Unknown'

      # Count by severity
      if severity == 'critical':
        error_stats['critical_errors'] += 1
      else:
        error_stats['regular_errors'] += 1

        # Count by source (function/module)
      error_stats['errors_by_source'][source] = \
      error_stats['errors_by_source'].get(source, 0) + 1

      # Count by type
      error_stats['errors_by_type'][error_type] = \
      error_stats['errors_by_type'].get(error_type, 0) + 1

      # Get the 10 most recent for detailed display
    for error in recent_errors[:10]:
      error_stats['recent_errors_list'].append({
        'timestamp': error['timestamp'],
        'source': error['source'] or 'Unknown',
        'message': (error['error_message'] or '')[:100],  # Truncate
        'severity': (error['severity'] or 'error').upper(),
        'error_type': error['error_type'] or 'Unknown'
      })

    return error_stats

  except Exception as e:
    log_error(f"Error analyzing error logs: {e}")
    return {
      'total_errors': 0,
      'critical_errors': 0,
      'regular_errors': 0,
      'errors_by_source': {},
      'errors_by_type': {},
      'recent_errors_list': []
    }


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
  # Make cutoff_time timezone-aware to match database timestamps
  cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)

  try:
    # Get all recent logs - Anvil's query syntax
    all_logs = list(app_tables.performance_log.search(
      tables.order_by('timestamp', ascending=False)
    ))[:500]  # Limit to 500 most recent logs for performance

    # Filter in Python for last 24 hours
    recent_logs = [log for log in all_logs 
                   if log['timestamp'] and log['timestamp'] >= cutoff_time]

    log_info(f"Found {len(recent_logs)} performance logs from last 24 hours (out of {len(all_logs)} total)")

  except Exception as e:
    log_error(f"Error reading performance logs: {e}")
    import traceback
    log_error(traceback.format_exc())
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

  # Analyze performance logs
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
    
    # Analyze error logs
    error_stats = analyze_error_logs(cutoff_time)
    stats['error_analysis'] = error_stats
    
    log_info(f"Performance summary stats: {stats}")
    
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
    
    # Build error analysis section
    error_analysis = stats.get('error_analysis', {})
    total_errors = error_analysis.get('total_errors', 0)
    critical_errors = error_analysis.get('critical_errors', 0)
    
    error_section = f"""
ERROR ANALYSIS:
{'─'*60}
Total Errors: {total_errors}
  • Critical Errors: {critical_errors}
  • Regular Errors: {error_analysis.get('regular_errors', 0)}
"""
    
    # Add errors by source if any
    if error_analysis.get('errors_by_source'):
        error_section += "\nErrors by Source:\n"
        sorted_sources = sorted(error_analysis['errors_by_source'].items(), 
                               key=lambda x: x[1], reverse=True)[:5]
        for source, count in sorted_sources:
            error_section += f"  • {source}: {count}\n"
    
    # Add errors by type if any
    if error_analysis.get('errors_by_type'):
        error_section += "\nErrors by Type:\n"
        sorted_types = sorted(error_analysis['errors_by_type'].items(), 
                             key=lambda x: x[1], reverse=True)[:5]
        for error_type, count in sorted_types:
            error_section += f"  • {error_type}: {count}\n"
    
    # Add recent error details if any
    if error_analysis.get('recent_errors_list'):
        error_section += "\nMost Recent Errors:\n"
        for err in error_analysis['recent_errors_list'][:5]:
            timestamp_str = err['timestamp'].strftime('%H:%M:%S') if err['timestamp'] else 'Unknown'
            error_section += f"  [{timestamp_str}] {err['severity']}: {err['source']}\n"
            error_section += f"    {err['message']}\n"
    
    # Determine status
    has_issues = stats['failed_operations'] > 0 or stats['errors_found'] > 0 or total_errors > 0
    
    if critical_errors > 0:
        status_line = f"🚨 CRITICAL: {critical_errors} critical errors require immediate attention"
    elif has_issues:
        status_line = "⚠️ ATTENTION NEEDED: Issues detected"
    else:
        status_line = "✅ All systems normal"
    
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

{error_section}

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
    if critical_errors > 0:
        subject = f"🚨 Night Processing - {critical_errors} CRITICAL ERRORS - {datetime.now().strftime('%m/%d/%Y')}"
    elif has_issues:
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
        log_error(f"Error sending summary email: {e}")
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