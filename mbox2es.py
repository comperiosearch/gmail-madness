import mailbox
import json
import email
from email.utils import parseaddr
from bs4 import BeautifulSoup
import os
import sys
import dateutil.parser as parser
import argparse
import elasticsearch


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
            if mail.get_content_type() == 'text/plain':
                content = mail.get_payload(decode=True)
            else:
                content = ''
    except TypeError:
        content = ''
    else:
        try:
            parsed = BeautifulSoup(content, "html.parser") \
                        .get_text() \
                        .replace('\n', '') \
                        .replace('\t', '') \
                        .replace('\r', '')

        except:
            # This is bad practice, but the bs4 module is a bit unknown to me
            # So I'm just wildcarding this exception here.
            parsed = ''
        finally:
            msg['contents'] = ' '.join(parsed.split())

    try:
        date = parser.parse(mail['Date']).isoformat()
    except:
        date = ''

    # print mail.keys()
    msg['date'] = date
    msg['labels'] = mail['X-Gmail-Labels']
    msg['flags'] = mail['flags']
    msg['subject'] = mail['subject']

    return msg

def main(path):
    es = elasticsearch.Elasticsearch('http://localhost:9200',
                                     retry_on_timeout=True)
    es_index = 'mail'
    es_type = 'message'
    batch_size = 20

    if es.indices.exists(index=es_index):
        es.indices.delete(es_index)
    es.indices.create(es_index)

    with open('mapping.json', 'r') as mapping_file:
        mapping = json.loads(mapping_file.read())
        es.indices.put_mapping(body=mapping, index=es_index, doc_type=es_type)

    with open(path, 'rb') as mb_file:
        mbox = mailbox.UnixMailbox(mb_file, email.message_from_file)
        batch = []
        num_indexed = 0
        num_failed = 0
        while True:
            my_mail = mbox.next()
            if my_mail is None:
                break
            my_json = jsonify(my_mail)
            if my_json['date'] != '':
                batch.append(''.join(['{"index": {}}\n',
                                      json.dumps(my_json), '\n']))
                num_indexed += 1
            else:
                num_failed +=1

            if num_indexed != 0 and num_indexed % (batch_size) == 0 and batch:
                es.bulk(body=batch, index=es_index, doc_type=es_type)
                print 'I have indexed {} documents in total'.format(num_indexed)
                print 'I have thrown away {} documents'.format(num_failed)
                print '------------------------------------'
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
