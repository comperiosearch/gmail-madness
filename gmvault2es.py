__author__ = 'sebastienmuller'

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
es_index = 'mail'
es_type = 'message'


def unicodish(s):
    return s.decode('latin-1', errors='replace')


def parse_date(raw_date):
    try:
        return parser.parse(raw_date).isoformat()
    except:
        return ''


def parse_and_store(es, root, email_path):
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

    meta['account'] = path.split(root)[-1]
    meta['path'] = email_path

    body = meta.copy()
    body['contents'] = content.getvalue()
    body['date'] = parse_date(body['date'])

    if 'from' in body:
        body['from'] = parseaddr(body['from'])[1]
    if 'to' in body:
        body['to'] = parseaddr(body['to'])[1]

    if body['date'] != '':
        es.index(index=es_index, doc_type=es_type, id=gm_id, body=body)


@click.command()
@click.argument('root', required=True, type=click.Path(exists=True))
def index(root):
    """imports all gmvault emails at ROOT into INDEX"""
    es = elasticsearch.Elasticsearch()
    if es.indices.exists(index=es_index):
        es.indices.delete(es_index)
    es.indices.create(es_index)
    mapping = json.loads(open("mapping.json", "r").read())
    es.indices.put_mapping(body=mapping, index=es_index, doc_type=es_type)
    root = path.abspath(root)
    for base, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith('.meta'):
                parse_and_store(es, root, path.join(base, name.split('.')[0]))


if __name__ == '__main__':
    index()
