import mailbox
import json
import email
from email.utils import parseaddr
from bs4 import BeautifulSoup
import os
import sys
import html5lib
import dateutil.parser as parser
import argparse
import elasticsearch

def jsonify_message(msg):
    
    
    msg['To'] = parseaddr(msg['To'])[1]
    msg['From'] = parseaddr(msg['From'])[1]

    
    try:
        if msg.is_multipart():
            content = ''.join(part.get_payload(decode=True) for part in msg.get_payload())
        else:
            content = msg.get_payload(decode=True)
    except TypeError as te:
        pass
    else:
        parsed = BeautifulSoup(content, "html5lib").get_text().replace('\n', '').replace('\t', '')
        msg['contents'] = ' '.join(parsed.split())

    try:
        date = parser.parse(msg['Date']).isoformat()
    except:
        date = ''

    msg['Date'] = date

    return {k: v for k, v in msg.items()}


def main(path):
    es = elasticsearch.Elasticsearch('http://localhost:9200')

    with open(path, 'rb') as mb_file:
        mbox = mailbox.UnixMailbox(mb_file, email.message_from_file)
        while True:
            my_mail = mbox.next()
            if my_mail is None: break
            my_json = jsonify_message(my_mail)
            if my_json['Date'] != '':
                # print(json.dumps(my_json))
                es.index(index='mail', doc_type='message', body=my_json)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("path_to_file", 
                        help="Specify the path of your .mbox file")

    args = argparser.parse_args()

    if os.path.isfile(args.path_to_file):
        main(args.path_to_file)
    else:
        print('Not a file.')



    