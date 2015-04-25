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


fields = {'labels', 'contents', 'Subject', 'flags'}

def jsonify(mail):

    msg = {}
    msg['to'] = parseaddr(mail['To'])[1]
    msg['from'] = parseaddr(mail['From'])[1]

    try:
        if mail.is_multipart():
            content = ''.join(part.get_payload(decode=True)
                              for part in mail.get_payload()
                              if part.get_content_type() == 'text/plain')
        else:
            content = mail.get_payload(decode=True) if mail.get_content_type() == 'text/plain' else ''
    except TypeError:
        # if the above parsing fails, we do nothing.
        pass
    else:
        try:
            parsed = BeautifulSoup(content, "html.parser").get_text().replace('\n', '').replace('\t', '')
        except:
            parsed = ''
        finally:
            msg['contents'] = ' '.join(parsed.split())

    try:
        date = parser.parse(mail['Date']).isoformat()
    except:
        date = ''

    msg['date'] = date
    msg['labels'] = mail['labels']
    msg['flags'] = mail['flags']
    msg['subject'] = mail['subject']

    return msg

def main(path):
    es = elasticsearch.Elasticsearch('http://localhost:9200',
                                     retry_on_timeout=True)
    es_index = 'mail'
    es_type = 'message'
    if es.indices.exists(index=es_index):
        es.indices.delete(es_index)
    es.indices.create(es_index)
    mapping = json.loads(open("mapping.json", "r").read())
    es.indices.put_mapping(body=mapping, index=es_index, doc_type=es_type)

    with open(path, 'rb') as mb_file:
        mbox = mailbox.UnixMailbox(mb_file, email.message_from_file)
        batch = []
        while True:
            my_mail = mbox.next()
            if my_mail is None: 
                break
            my_json = jsonify(my_mail)
            if my_json['date'] != '':
                # batch.append('{"index": {}}\n')
                batch.append(''.join(['{"index": {}}\n', json.dumps(my_json), '\n']))
                print 'appended'
            if len(batch) > 40:
                es.bulk(body=batch, index=es_index, doc_type=es_type)
                print 'batch finished'
                batch = []

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument("path_to_file", 
                        help="Specify the path of your .mbox file")

    args = argparser.parse_args()

    if os.path.isfile(args.path_to_file):
        main(args.path_to_file)
    else:
        print('Not a file.')

