import os
from apiclient.http import HttpMockSequence


def content(filename):
    if not os.path.exists(filename):
        raise Exception("File not found: {0}".format(filename))
    with open(filename, 'r') as f:
        return f.read()


def create_bq_generic_mock():
    return HttpMockSequence([
        ({'status': '200'},
         content('tests/json_samples/bigquery_v2_test_schema.json'))
    ])
