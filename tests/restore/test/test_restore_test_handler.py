import json
import unittest

from apiclient.http import HttpMockSequence
from google.appengine.ext import testbed

import webtest
from mock import patch
from src.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.google_cloud_storage_client import GoogleCloudStorageClient
from src.restore.test import restore_test_handler
from src.restore.test.table_randomizer import TableRandomizer
from src.restore.test.table_restore_invoker import TableRestoreInvoker

example_table = {
    'tableReference': {
        'projectId': 'myproject123',
        'datasetId': 'd1',
        'tableId': 't1',
    },
    'numBytes': 6565,
    'numRows': 99
}


# nopep8 pylint: disable=W0613
class TestRestoreTestHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(restore_test_handler.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

        self.testbed.init_app_identity_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_returns_ok_status_if_restore_is_success(
            self, get_random_table_metadata, _create_http):

        # given
        status_success = {
            "status": "success",
            "size_in_bytes": 6565,
            "rows_count": 99
        }
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'}, json.dumps(status_success))
        ])

        # when
        response = self.under_test.get('/restore/test')

        # then
        self.assertEquals(200, response.status_int)

    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_returns_500_status_if_restore_failed(
            self, get_random_table_metadata, _create_http):

        # given
        status_failed = {
            "status": "failed",
            "data": {
                "size_in_bytes": 6565,
                "rows_count": 99
            }
        }
        _create_http.return_value = HttpMockSequence([
            ({'status': '500'}, json.dumps(status_failed))
        ])

        # when
        response = self.under_test.get('/restore/test', expect_errors=True)

        # then
        self.assertEquals(500, response.status_int)

    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_outputs_success_status_file_to_GCS_if_restore_is_success(
            self,
            get_random_table_metadata,
            put_gcs_file_content,
            _create_http):

        # given
        status_success = {
            "status": "success",
            "size_in_bytes": 6565,
            "rows_count": 99
        }
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'}, json.dumps(status_success))
        ])

        self.under_test.get('/restore/test')
        put_gcs_file_content.assert_called_once_with(
            'a-gcs-bucket',
            'restore-test-status.json',
            '{"status": "success", "restore_response": {"status": "success", "size_in_bytes": 6565, "rows_count": 99}, "tableReference": "myproject123:d1.t1"}',      # nopep8 pylint: disable=C0301
            'application/json')

    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_outputs_failed_status_file_to_GCS_if_restore_is_failed(
            self,
            get_random_table_metadata,
            put_gcs_file_content,
            _create_http):

        # given
        status_failed = {
            "status": "failed",
            "data": {
                "size_in_bytes": 6565,
                "rows_count": 99
            }
        }
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'}, json.dumps(status_failed))
        ])

        # when
        self.under_test.get('/restore/test', expect_errors=True)

        # then
        put_gcs_file_content.assert_called_once_with(
            'a-gcs-bucket',
            'restore-test-status.json',
            '{"status": "failed", "restore_response": {"status": "failed", "data": {"size_in_bytes": 6565, "rows_count": 99}}, "tableReference": "myproject123:d1.t1"}',      # nopep8 pylint: disable=C0301
            'application/json')

    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_outputs_failed_status_if_table_was_restored_but_rows_doesnt_match(                 # nopep8 pylint: disable=C0301
            self,
            get_random_table_metadata,
            put_gcs_file_content,
            _create_http):

        # given
        status_success_but_rows_mismatch = {
            "status": "success",
            "size_in_bytes": 6565,
            "rows_count": 1000
        }
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'}, json.dumps(status_success_but_rows_mismatch))
        ])

        # when
        response = self.under_test.get('/restore/test', expect_errors=True)

        # then
        self.assertEquals(500, response.status_int)
        put_gcs_file_content.assert_called_once_with(
            'a-gcs-bucket',
            'restore-test-status.json',
            '{"status": "failed", "restore_response": {"status": "success", "size_in_bytes": 6565, "rows_count": 1000}, "tableReference": "myproject123:d1.t1"}',      # nopep8 pylint: disable=C0301
            'application/json')
