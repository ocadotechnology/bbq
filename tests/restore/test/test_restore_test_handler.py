import json
import unittest

import webtest
from apiclient.http import HttpMockSequence
from google.appengine.ext import testbed
from mock import patch

from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.google_cloud_storage_client import GoogleCloudStorageClient
from src.commons.table_reference import TableReference
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

restoration_status_successful = {
    "status": {"state": "Done", "result": "Success"},
    "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84",
    "itemResults": {"done": 1},
    "itemsCount": 1, "restorationItems": [{
        "status": "Done", "customParameters": "null",
        "completed": "2018-07-04T10:14:33.118120",
        "statusMessage": "null",
        "targetTable": "target-project:target_dataset.target_table",
        "sourceTable": "source-project:source_dataset.source_table"
    }]
}

restoration_status_failed = {
    "status": {"state": "Done", "result": "Failed"},
    "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"
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

    @patch.object(TableRestoreInvoker, 'wait_till_done')
    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQuery, 'delete_table')
    def test_handler_returns_ok_status_if_restore_is_success(
            self, _, _1, _2, _3, _create_http, wait_till_done):
        # given
        endpoint_invoke_result = HttpMockSequence([(
            {'status': '200'}, json.dumps({
                "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))])
        _create_http.return_value = endpoint_invoke_result
        wait_till_done.return_value = restoration_status_successful

        # when
        response = self.under_test.get('/restore/test')

        # then
        self.assertEquals(200, response.status_int)

    @patch.object(TableRestoreInvoker, 'wait_till_done')
    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_returns_500_status_if_restore_failed(
            self, _, _1, _2, _create_http, wait_till_done):
        # given
        endpoint_invoke_result = HttpMockSequence([(
            {'status': '200'}, json.dumps({
                "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))])
        _create_http.return_value = endpoint_invoke_result
        wait_till_done.return_value = restoration_status_failed

        # when
        response = self.under_test.get('/restore/test', expect_errors=True)

        # then
        self.assertEquals(500, response.status_int)

    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(TableRestoreInvoker, 'wait_till_done',
                  return_value=restoration_status_successful)
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQuery, 'delete_table')
    def test_handler_outputs_success_status_file_to_GCS_if_restore_is_success(
            self, _, _1, _2, _3, _create_http, put_gcs_file_content):
        # given
        endpoint_invoke_result = HttpMockSequence([(
            {'status': '200'}, json.dumps({
                "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))])
        _create_http.return_value = endpoint_invoke_result

        # when
        self.under_test.get('/restore/test')

        # then
        expected_restore_response = {
            "status": "success",
            "restore_response": restoration_status_successful,
            "tableReference": "myproject123:d1.t1"
        }
        put_gcs_file_content.assert_called_once_with(
            'a-gcs-bucket',
            'restore-test-status.json',
            json.dumps(expected_restore_response),
            'application/json'
        )

    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(TableRestoreInvoker, 'wait_till_done',
                  return_value=restoration_status_failed)
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_outputs_failed_status_file_to_GCS_if_restore_is_failed(
            self, _, _1, _2, _create_http, put_gcs_file_content):
        # given
        endpoint_invoke_result = HttpMockSequence([(
            {'status': '200'}, json.dumps({
                "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))])
        _create_http.return_value = endpoint_invoke_result

        # when
        self.under_test.get('/restore/test', expect_errors=True)

        # then
        expected_restore_response = {
            "status": "failed",
            "restore_response": restoration_status_failed,
            "tableReference": "myproject123:d1.t1"
        }
        put_gcs_file_content.assert_called_once_with(
            'a-gcs-bucket',
            'restore-test-status.json',
            json.dumps(expected_restore_response),
            'application/json'
        )

    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference')
    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(TableRestoreInvoker, 'wait_till_done',
                  return_value=restoration_status_successful)
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_outputs_failed_status_if_table_was_restored_but_rows_doesnt_match(
            self, _, _1, _create_http, get_table_by_reference,
            put_gcs_file_content):
        # given
        endpoint_invoke_result = HttpMockSequence([(
            {'status': '200'}, json.dumps({
                "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))])
        _create_http.return_value = endpoint_invoke_result

        different_table = {
            'tableReference': {
                'projectId': 'myproject123',
                'datasetId': 'd1',
                'tableId': 't1',
            },
            'numBytes': 6566,
            'numRows': 99
        }
        get_table_by_reference.return_value = \
            BigQueryTableMetadata(different_table)

        # when
        response = self.under_test.get('/restore/test', expect_errors=True)

        # then
        self.assertEquals(500, response.status_int)
        expected_restore_response = {
            "status": "failed",
            "restore_response": restoration_status_successful,
            "tableReference": "myproject123:d1.t1"
        }
        put_gcs_file_content.assert_called_once_with(
            'a-gcs-bucket',
            'restore-test-status.json',
            json.dumps(expected_restore_response),
            'application/json'
        )

    @patch.object(BigQuery, 'delete_table')
    @patch.object(TableRestoreInvoker, 'wait_till_done')
    @patch.object(TableRestoreInvoker, '_create_http')
    @patch.object(GoogleCloudStorageClient, 'put_gcs_file_content')
    @patch.object(TableRandomizer, 'get_random_table_metadata',
                  return_value=BigQueryTableMetadata(example_table))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  return_value=BigQueryTableMetadata(example_table))
    def test_handler_delete_restored_table_after_success(
            self, _, _1, _2, _create_http, wait_till_done, delete_table):
        # given
        endpoint_invoke_result = HttpMockSequence([(
            {'status': '200'}, json.dumps({
                "restorationJobId": "64c6e50c-b511-43eb-ba75-f44f3d131f84"}))])
        _create_http.return_value = endpoint_invoke_result
        wait_till_done.return_value = restoration_status_successful

        # when
        response = self.under_test.get('/restore/test')

        # then
        self.assertEquals(200, response.status_int)
        delete_table.assert_called_with(TableReference('target-project',
                                                       'target_dataset',
                                                       'target_table'))
