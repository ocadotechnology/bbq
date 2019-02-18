import json
import os
from datetime import datetime

from mock.mock import Mock

from src.backup.datastore.Table import Table
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.big_query.copy_job_async.copy_job_result import CopyJobResult
from src.commons.encoders.request_encoder import RequestEncoder
from src.commons.table_reference import TableReference
from tests import test_utils
from tests.commons.big_query.copy_job_async.result_check.job_result_example \
    import JobResultExample
from tests.test_utils import content

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest
from mock import patch
import webtest
from google.appengine.ext import testbed
from apiclient.http import HttpMockSequence
from src.commons.test_utils import utils
from src.backup import after_backup_action_handler
from src.commons.big_query.big_query_table import BigQueryTable


class TestAfterBackupActionHandler(unittest.TestCase):
    def setUp(self):
        self.under_test = webtest.TestApp(after_backup_action_handler.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_app_identity_stub()
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(BigQuery, '_create_credentials', return_value=None)
    @patch.object(BigQuery, '_create_http')
    def test_should_create_datastore_backup_entity(self, _create_http, _):
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/table_get/'
                     'bigquery_partitioned_table_get.json'))
        ])

        table_entity = Table(
            project_id="source_project_id",
            dataset_id="source_dataset_id",
            table_id="source_table_id",
            partition_id="123"
        )
        table_entity.put()

        source_bq_table = TableReference.from_table_entity(
            table_entity).create_big_query_table()
        destination_bq_table = BigQueryTable("target_project_id",
                                             "target_dataset_id",
                                             "target_table_id")
        data = {"sourceBqTable": source_bq_table,
                "targetBqTable": destination_bq_table}
        payload = json.dumps({
            "data": data,
            "jobJson": JobResultExample.DONE
        }, cls=RequestEncoder)
        copy_job_result = CopyJobResult(json.loads(payload).get('jobJson'))

        # when
        response = self.under_test.post(
            '/callback/backup-created/project/dataset/table',
            params=payload)
        backup = table_entity.last_backup

        # then
        self.assertEqual(response.status_int, 200)
        self.assertEqual(backup.dataset_id, "target_dataset_id")
        self.assertEqual(backup.table_id, "target_table_id")
        self.assertTrue(isinstance(backup.created, datetime))
        self.assertEqual(backup.created, copy_job_result.end_time)

        self.assertTrue(isinstance(backup.last_modified, datetime))
        self.assertEqual(backup.last_modified, copy_job_result.start_time)

    @patch.object(BigQuery, '_create_credentials', return_value=None)
    @patch('src.backup.after_backup_action_handler.ErrorReporting')
    @patch.object(BigQuery, '_create_http')
    def test_should_not_create_backups_entity_if_copy_job_failed(
            self, _create_http, error_reporting, _):
        # given
        _create_http.return_value = HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
        ])
        table_entity = Table(
            project_id="source_project_id",
            dataset_id="source_dataset_id",
            table_id="source_table_id",
            partition_id="123"
        )
        table_entity.put()

        source_bq_table = TableReference.from_table_entity(
            table_entity).create_big_query_table()
        destination_bq_table = BigQueryTable("target_project_id",
                                             "target_dataset_id",
                                             "target_table_id")
        data = {"sourceBqTable": source_bq_table,
                "targetBqTable": destination_bq_table}
        payload = json.dumps({
            "data": data,
            "jobJson": JobResultExample.DONE_WITH_NOT_REPETITIVE_ERRORS
        }, cls=RequestEncoder)

        # when
        response = self.under_test.post(
            '/callback/backup-created/project/dataset/table',
            params=payload)
        backup = table_entity.last_backup

        # then
        self.assertEqual(response.status_int, 200)
        self.assertIsNone(backup)
        error_reporting.assert_called_once()

    @patch.object(BigQuery, '_create_credentials', return_value=None)
    @patch('src.backup.after_backup_action_handler.ErrorReporting')
    @patch.object(BigQuery, '_create_http')
    def test_should_not_create_backups_entity_if_backup_table_doesnt_exist(
            self, _create_http, error_reporting, _):
        # given
        _create_http.return_value = HttpMockSequence([
          ({'status': '200'},
           content('tests/json_samples/bigquery_v2_test_schema.json')),
          ({'status': '404'}, # Table not found
           content('tests/json_samples/table_get/'
                   'bigquery_partitioned_table_get.json'))
        ])

        table_entity = Table(
            project_id="source_project_id",
            dataset_id="source_dataset_id",
            table_id="source_table_id",
            partition_id="123"
        )
        table_entity.put()

        source_bq_table = TableReference.from_table_entity(
            table_entity).create_big_query_table()
        destination_bq_table = BigQueryTable("target_project_id",
                                             "target_dataset_id",
                                             "target_table_id")
        data = {"sourceBqTable": source_bq_table,
                "targetBqTable": destination_bq_table}
        payload = json.dumps({
            "data": data,
            "jobJson": JobResultExample.DONE
        }, cls=RequestEncoder)

        # when
        response = self.under_test.post(
            '/callback/backup-created/project/dataset/table',
            params=payload)
        backup = table_entity.last_backup

        # then
        self.assertEqual(response.status_int, 200)
        self.assertIsNone(backup)
        error_reporting.assert_called_once()

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BigQueryTableMetadata, 'get_table_by_big_query_table', return_value=BigQueryTableMetadata(None))
    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime', return_value=datetime.utcnow())
    @patch.object(BigQueryTableMetadata, 'table_size_in_bytes', return_value=123)
    @patch.object(BigQueryTableMetadata, 'has_partition_expiration', return_value=True)
    @patch.object(BigQuery, '_create_credentials', return_value=None)
    @patch.object(BigQuery, 'disable_partition_expiration')
    def test_should_disable_partition_expiration_if_backup_table_has_it(
            self, disable_partition_expiration, _, _1, _2, _3, _4, _5):
        # given
        table_entity = Table(
            project_id="source_project_id",
            dataset_id="source_dataset_id",
            table_id="source_table_id",
            partition_id="123"
        )
        table_entity.put()

        source_bq_table = TableReference.from_table_entity(
            table_entity).create_big_query_table()
        destination_bq_table = BigQueryTable("target_project_id",
                                             "target_dataset_id",
                                             "target_table_id")
        data = {"sourceBqTable": source_bq_table,
                "targetBqTable": destination_bq_table}
        payload = json.dumps({
            "data": data,
            "jobJson": JobResultExample.DONE
        }, cls=RequestEncoder)

        # when
        response = self.under_test.post(
            '/callback/backup-created/project/dataset/table',
            params=payload
        )

        # then
        self.assertEqual(response.status_int, 200)
        disable_partition_expiration.assert_called_once()

    @patch.object(BigQuery, '_create_credentials', return_value=None)
    @patch.object(BigQuery, '_create_http')
    def test_should_return_400_for_wrong_data(self, _create_http, _):
        # given
        _create_http.return_value = test_utils.create_bq_generic_mock()

        payload = '{"data": <ILikeXML/>, "jobJson": {"state": "DONE"}}'
        expected_error = "{\"status\": \"failed\", \"message\": \"No JSON " \
                         "object could be decoded\", \"httpStatus\": 400}"

        # when
        response = self.under_test.post(
            '/callback/backup-created/project/dataset/table',
            params=payload, expect_errors=True)

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @patch.object(BigQuery, '_create_credentials', return_value=None)
    @patch.object(BigQuery, '_create_http')
    def test_should_return_400_for_incomplete_data_json(self, _create_http, _):
        # given
        _create_http.return_value = test_utils.create_bq_generic_mock()
        payload = '{"data": {}, "jobJson": {"state": "DONE"}}'
        expected_error = \
            "{\"status\": \"failed\", \"message\": " \
            "\"JSON has no sourceBqTable or targetBqTable parameters\", " \
            "\"httpStatus\": 400}"

        # when
        response = self.under_test.post(
            '/callback/backup-created/project/dataset/table',
            params=payload, expect_errors=True)

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)
