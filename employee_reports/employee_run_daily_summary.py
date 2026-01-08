import argparse
import os
import sys

# ✅ Ensure project root is on sys.path (works in Railway + local)
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(THIS_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from employee_reports.employee_daily_summary import employee_send_daily_summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Do not send emails, just log actions.")
    args = parser.parse_args()

    employee_send_daily_summaries(dry_run=args.dry_run)
