import json
import os

from googleapiclient.discovery import build
from httplib2 import Http
from email.mime.text import MIMEText
import base64
from google.oauth2 import service_account

GMAIL_EMAIL_FROM = os.getenv('GMAIL_EMAIL_FROM')
GMAIL_EMAIL_TO = os.getenv('GMAIL_EMAIL_TO')
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly',
          'https://www.googleapis.com/auth/gmail.send']


def service_account_login():
    service_account_info_str = os.getenv('GOOGLE_SERVICE_ACCOUNT_INFO')
    if not service_account_info_str:
        return
    service_account_info = json.loads(service_account_info_str)
    credentials = service_account.Credentials.from_service_account_file(
        service_account_info, scopes=SCOPES)
    delegated_credentials = credentials.with_subject(GMAIL_EMAIL_FROM)
    service = build('gmail', 'v1', credentials=delegated_credentials)
    return service


gmail_service = service_account_login()


def create_message(sender, to, subject, message_text):
    """Create a message for an email.

    Args:
      sender: Email address of the sender.
      to: Email address of the receiver.
      subject: The subject of the email message.
      message_text: The text of the email message.

    Returns:
      An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}


def send_message(service, user_id, message):
    """Send an email message.
    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.
    Returns:
      Sent Message.
    """
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
                   .execute())
        print('Message Id: %s' % message['id'])
        return message
    except Exception as error:
        print('An error occurred: %s' % error)


def send_email(subject, message_text=''):
    message = create_message(GMAIL_EMAIL_FROM, GMAIL_EMAIL_TO,
                             subject, message_text)
    send_message(gmail_service, 'me', message)


if __name__ == '__main__':
    subject = 'Hello  from Forbin!'
    message_text = 'Hello, this is a test\nForbin\nhttps://darchimbaud.com'
    send_email(subject, message_text)
