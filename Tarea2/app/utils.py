import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(smtp_server, smtp_port, smtp_username, smtp_password, recipient, subject, body):
    msg = MIMEMultipart()
    msg['From'] = smtp_username
    msg['To'] = recipient
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        text = msg.as_string()
        server.sendmail(smtp_username, recipient, text)
        server.quit()
        print(f"Email sent to {recipient}")
    except Exception as e:
        print(f"Failed to send email: {e}")

