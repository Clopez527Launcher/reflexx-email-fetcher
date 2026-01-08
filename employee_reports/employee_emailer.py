import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def employee_send_html_email(to_email: str, subject: str, html_body: str):
    """
    Send an HTML email using SMTP.

    Required env vars:
      SMTP_HOST
      SMTP_PORT (default 587)
      SMTP_USER
      SMTP_PASS
      SMTP_FROM (optional, defaults to SMTP_USER)
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    smtp_from = os.getenv("SMTP_FROM") or smtp_user

    if not smtp_host or not smtp_user or not smtp_pass:
        raise RuntimeError("Missing SMTP env vars: SMTP_HOST, SMTP_USER, SMTP_PASS (SMTP_FROM optional).")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email

    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_from, [to_email], msg.as_string())
