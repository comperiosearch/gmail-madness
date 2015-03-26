import mailbox
import json
import email
from email.utils import parseaddr
from bs4 import BeautifulSoup
import sys
import html5lib
import dateutil.parser as parser

MBOX = 'All mail Including Spam and Trash-2.mbox'

def jsonify_message(msg):
    
    my_dict = {}
    my_dict['To'] = parseaddr(msg['To'])[1]
    my_dict['From'] = parseaddr(msg['From'])[1]

    my_dict['Subject'] = msg['Subject']
    
    try:
        if msg.is_multipart():
            content = ''.join(part.get_payload(decode=True) for part in msg.get_payload())
        else:
            content = msg.get_payload(decode=True)
    except TypeError as te:
        pass
    else:
        parsed = BeautifulSoup(content, "html5lib").get_text().replace('\n', '').replace('\t', '')
        my_dict['Message'] = ' '.join(parsed.split())

    try:
        date = parser.parse(msg['Date']).isoformat()
    except:
        date = ''

    my_dict['Date'] = date

    return my_dict


mbox = mailbox.UnixMailbox(open(MBOX, 'rb'), email.message_from_file)

messages = []

while True:
    my_mail = mbox.next()
    if my_mail is None: break
    my_json = jsonify_message(my_mail)
    if my_json['Date'] != '':
        print(json.dumps(my_json))
    