import os
from twilio.rest import Client


def send_sms(text):
    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    client = Client(account_sid, auth_token)
    message = client.messages \
                    .create(
                        body=text,
                        from_='+18165791003',
                        to='+33651244977'
                    )
    print(message.sid)
