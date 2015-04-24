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

def jsonify(msg):

    msg['to'] = parseaddr(msg['To'])[1]
    msg['from'] = parseaddr(msg['From'])[1]

    try:
        if msg.is_multipart():
            content = ''.join(part.get_payload(decode=True)
                              for part in msg.get_payload())
        else:
            content = msg.get_payload(decode=True)
    except TypeError:
        # if the above parsing fails, we do nothing.
        pass
    else:

        parsed = BeautifulSoup(content, "html5lib").get_text().replace('\n', '').replace('\t', '')
        msg['contents'] = ' '.join(parsed.split())

    try:
        date = parser.parse(msg['Date']).isoformat()
    except:
        date = ''

    msg['date'] = date

    return {k: v for k, v in msg.items() if k in fields.union({'date', 'from', 'to'})}


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
                batch.append({'index': {}})
                batch.append(my_json)
            if len(batch) > 500:
                es.bulk(body=batch, index=es_index, doc_type=es_type)
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

