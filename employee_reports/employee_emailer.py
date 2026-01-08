import os
import requests


def employee_send_html_email(to_email: str, subject: str, html_body: str):
    """
    Sends an HTML email.

    ✅ Preferred: Postmark (matches your other jobs)
    Fallback: SMTP (if you ever add SMTP_* vars later)
    """

    # ---------------------------
    # ✅ Option A: Postmark
    # ---------------------------
    postmark_token = (os.getenv("POSTMARK_API_TOKEN") or "").strip()
    postmark_from  = (os.getenv("POSTMARK_FROM_EMAIL") or "").strip()

    if postmark_token and postmark_from:
        url = "https://api.postmarkapp.com/email"
        headers = {
            "X-Postmark-Server-Token": postmark_token,
            "Content-Type": "application/json"
        }
        payload = {
            "From": postmark_from,
            "To": to_email,
            "Subject": subject,
            "HtmlBody": html_body,
            "MessageStream": "outbound"  # fine for most setups
        }

        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code >= 300:
            raise Exception(f"Postmark send failed ({r.status_code}): {r.text}")
        return True

    # ---------------------------
    # ❌ Option B: SMTP fallback
    # ---------------------------
    smtp_host = os.getenv("SMTP_HOST")
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    smtp_from = os.getenv("SMTP_FROM") or smtp_user

    if not (smtp_host and smtp_user and smtp_pass):
        raise Exception("Missing SMTP env vars: SMTP_HOST, SMTP_USER, SMTP_PASS (SMTP_FROM optional).")

    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(html_body, "html")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email

    with smtplib.SMTP(smtp_host, 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_from, [to_email], msg.as_string())

    return True

