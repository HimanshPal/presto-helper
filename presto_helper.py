#!/usr/bin/env python
import gzip
import json
import os
import pprint
import random
import re
import sys
import tempfile
import boto3

pp = pprint.PrettyPrinter()

SAMPLE_SIZE = 3

RX_S3_URL = re.compile('^s3://(?P<bucket>[^/]+)/(?P<prefix>.*)$')

KEY_RESPONSE_METADATA = 'ResponseMetadata'
KEY_HTTP_STATUS_CODE = 'HTTPStatusCode'
KEY_CONTENTS = 'Contents'
KEY_KEY = 'Key'

TYPE_INT = 'int'
TYPE_BIGINT = 'bigint'
TYPE_DOUBLE = 'double'
TYPE_VARCHAR = 'string'

SQL_CREATE_TABLE_FMT = """
CREATE EXTERNAL TAB LE IF NOT EXISTS {table_name} (
{columns}
) PARTITIONED BY (
{partitions}
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION '{s3_url}'
TBLPROPERTIES ('has_encrypted_data'='false');
"""


class Helper:
    def __init__(self, s3_url):
        self.s3_url = s3_url
        self.bucket, self.prefix = self.get_bucket_and_prefix_from_s3_url(s3_url)
        self.client = boto3.client('s3')

    def get_contents_from_response(self, response):
        if KEY_RESPONSE_METADATA not in response:
            raise Exception('no {} in S3 response: {}'.format(KEY_RESPONSE_METADATA, response))
        response_metadata = response[KEY_RESPONSE_METADATA]
        if KEY_HTTP_STATUS_CODE not in response_metadata:
            raise Exception('no {} in S3 response: {}'.format(KEY_HTTP_STATUS_CODE, response))
        http_status_code = response_metadata[KEY_HTTP_STATUS_CODE]
        if http_status_code != 200:
            raise Exception('unexpected {}: {}'.format(KEY_HTTP_STATUS_CODE, http_status_code))
        if KEY_CONTENTS not in response:
            # raise Exception('no {} in S3 response: {}'.format(KEY_CONTENTS, response))
            return []

        return [item[KEY_KEY] for item in response[KEY_CONTENTS]]

    def get_bucket_and_prefix_from_s3_url(self, s3_url):
        match = RX_S3_URL.search(s3_url)
        if match:
            return match.groupdict()['bucket'], match.groupdict()['prefix']

        raise Exception('not an S3 url: {}'.format(s3_url))

    def get_files(self):
        response = self.client.list_objects(Bucket=self.bucket,
                                            Prefix=self.prefix)
        return self.get_contents_from_response(response)

    def get_file(self, key):
        try:
            f1, path = tempfile.mkstemp()
            self.client.download_file(Bucket=self.bucket,
                                      Key=key,
                                      Filename=path)
            if key.endswith('.gz'):
                with gzip.open(path) as f2:
                    return f2.read()
            else:
                with open(path) as f2:
                    return f2.read()
        finally:
            os.unlink(path)

    def get_fields(self, raw_record):
        fields = {}
        try:
            data = json.loads(raw_record)
        except:
            sys.stderr.write('could not process: {}'.format(raw_record[:100]))
            return fields
        for key, value in data.items():
            if isinstance(value, int):
                fields[key] = TYPE_BIGINT
            elif isinstance(value, float):
                fields[key] = TYPE_DOUBLE
            else:
                fields[key] = TYPE_VARCHAR

        return fields

    def merge_fields(self, fields1, fields2):
        merged = dict(fields1)
        for key, type2 in fields2.items():
            if key in merged:
                type1 = merged[key]
                if type1 == TYPE_VARCHAR or type2 == TYPE_VARCHAR:
                    merged[key] = TYPE_VARCHAR
                elif type1 == TYPE_DOUBLE or type2 == TYPE_DOUBLE:
                    merged[key] = TYPE_DOUBLE
                else:
                    merged[key] = TYPE_BIGINT
            else:
                merged[key] = type2

        return merged

    def get_fields_for_files(self, files):
        fields = {}
        for _file in files:
            content = helper.get_file(_file)
            records = content.decode('utf-8').split('\n')
            for record in records:
                fields = helper.merge_fields(fields, helper.get_fields(record))

        return fields

    def create_table_for_fields(self, fields):
        columns = ',\n'.join(['  ' + k + ' ' + v for k, v in fields.items()])
        partitions = {'year': TYPE_INT, 'month': TYPE_INT, 'day': TYPE_INT}
        partition_columns = ',\n'.join(['  ' + k + ' ' + v for k, v in partitions.items()])
        return SQL_CREATE_TABLE_FMT.format(table_name='FIXME',
                                           columns=columns,
                                           partitions=partition_columns,
                                           s3_url=self.s3_url)

helper = Helper(sys.argv[1])
files = helper.get_files()
sample = random.sample(files, SAMPLE_SIZE)
fields = helper.get_fields_for_files(sample)
print(helper.create_table_for_fields(fields))
