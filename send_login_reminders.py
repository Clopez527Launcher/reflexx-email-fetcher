import os
from datetime import datetime
import pytz
import mysql.connector
from postmarker.core import PostmarkClient

PACIFIC = pytz.timezone("America/Los_Angeles")

POSTMARK_API_TOKEN = os.getenv("POSTMARK_API_TOKEN")
FROM_EMAIL = os.getenv("POSTMARK_FROM_EMAIL", "chris@reflexxapp.com")
LOGIN_URL = os.getenv("REFLEXX_LOGIN_URL", "https://app.reflexxapp.com/login")


def get_db():
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQLDATABASE") or os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQLPORT", "3306")),
    )


def main():
    now_pt = datetime.now(PACIFIC)
    print(f"📨 Login reminders job running @ {now_pt.strftime('%Y-%m-%d %I:%M %p %Z')}")

    if not POSTMARK_API_TOKEN:
        raise RuntimeError("Missing POSTMARK_API_TOKEN env var")

    db = get_db()
    cur = db.cursor(dictionary=True)

    cur.execute("""
        SELECT id, email, nickname
        FROM users
        WHERE role = 'user'
          AND email_login_reminder_enabled = 1
          AND email IS NOT NULL
          AND email <> ''
    """)
    users = cur.fetchall()

    if not users:
        print("⚠️ No users with reminders enabled. Done.")
        cur.close()
        db.close()
        return

    postmark = PostmarkClient(server_token=POSTMARK_API_TOKEN)

    sent = 0
    for u in users:
        name = (u.get("nickname") or "there").strip()
        to_email = u["email"].strip()

        try:
            postmark.emails.send(
                From=FROM_EMAIL,
                To=to_email,
                Subject="Reflexx Daily Login Reminder",
                HtmlBody=f"""
                <div style="font-family:Arial,sans-serif; line-height:1.5;">
                  <p style="margin:0 0 14px 0; font-size:16px;">
                    Good morning {name},
                  </p>

                  <p style="margin:0 0 14px 0;">
                    Quick reminder to log into <b>Reflexx</b>.
                  </p>

                  <p style="margin:0 0 14px 0;">
                    Thank you for your partnership.
                  </p>

                  <p style="margin:0 0 18px 0;">
                    <b>Happy Selling!</b>
                  </p>

                  <p style="margin:0;">
                    👉 <a href="{LOGIN_URL}">Log into Reflexx</a>
                  </p>
                </div>
                """
            )
            sent += 1
            print(f"✅ Sent to {to_email}")

        except Exception as e:
            print(f"❌ Failed for {to_email}: {e}")

    cur.close()
    db.close()
    print(f"✅ Done. Sent {sent}/{len(users)} emails.")


if __name__ == "__main__":
    main()
