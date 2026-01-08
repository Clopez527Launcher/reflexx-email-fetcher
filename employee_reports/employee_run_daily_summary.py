import argparse

from employee_reports.employee_daily_summary import employee_send_daily_summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Do not send emails, just log actions.")
    args = parser.parse_args()

    employee_send_daily_summaries(dry_run=args.dry_run)
