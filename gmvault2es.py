import email
from email.utils import parseaddr
import json
import gzip
import io
import os
from os import path
import dateutil.parser as parser
import click
import elasticsearch
from bs4 import BeautifulSoup


es_index = 'mail'
es_type = 'message'
batch_size = 20
batch = []
num_indexed = 0
num_failed = 0



def unicodish(s):
    return s.decode('latin-1', errors='replace')


def parse_date(raw_date):
    try:
        return parser.parse(raw_date).isoformat()
    except:
        return ''


def parse_and_store(es, root, email_path):
    # blargh sorry for this
    global batch
    global num_indexed
    global num_failed

    gm_id = path.split(email_path)[-1]

    with gzip.open(email_path + '.eml.gz', 'r') as fp:
        message = email.message_from_file(fp)
    meta = {unicodish(k).lower(): unicodish(v) for k, v in message.items()}
    with open(email_path + '.meta', 'r') as fp:
        meta.update(json.load(fp))

    content = io.StringIO()
    if message.is_multipart():
        for part in message.get_payload():
            if part.get_content_type() == 'text/plain':
                content.write(unicodish(part.get_payload()))
    else:
        content.write(unicodish(message.get_payload()))

    body = {}
    body['contents'] = content.getvalue()
    body['date'] = parse_date(meta['date'])
    try:
        parsed = BeautifulSoup(body['contents'], "html.parser") \
                    .get_text() \
                    .replace('\n', '') \
                    .replace('\t', '') \
                    .replace('\r', '')
    except Exception as e:
        # This is bad practice, but the bs4 module is a bit unknown to me
        # So I'm just wildcarding this exception here.
        parsed = ''
    finally:
        body['contents'] = ' '.join(parsed.split())

    body['labels'] = [x.replace('\\', '') for x in meta['labels']]
    body['flags'] = [x.replace('\\', '') for x in meta['flags']]


    if 'from' in meta:
        body['from'] = parseaddr(meta['from'])[1]
    if 'to' in meta:
        body['to'] = parseaddr(meta['to'])[1]

    if body['date'] != '':
        batch.append(''.join(['{"index": {}}\n',
                              json.dumps(body), '\n']))
        num_indexed += 1
    else:
        num_failed += 1

    if num_indexed != 0 and num_indexed % (batch_size) == 0 and batch:
        es.bulk(body=batch, index=es_index, doc_type=es_type)
        print 'I have indexed {} documents in total'.format(num_indexed)
        print 'I have thrown away {} documents'.format(num_failed)
        print '------------------------------------'
        batch = []


@click.command()
@click.argument('root', required=True, type=click.Path(exists=True))
def index(root):
    """imports all gmvault emails at ROOT into INDEX"""
    es = elasticsearch.Elasticsearch()
    if es.indices.exists(index=es_index):
        es.indices.delete(es_index)
    es.indices.create(es_index)
    with open('mapping.json', 'r') as mapping_file:
        mapping = json.loads(mapping_file.read())
        es.indices.put_mapping(body=mapping, index=es_index, doc_type=es_type)

    root = path.abspath(root)
    for base, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith('.meta'):
                parse_and_store(es, root, path.join(base, name.split('.')[0]))


if __name__ == '__main__':
    index()
