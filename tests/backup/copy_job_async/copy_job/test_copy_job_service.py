import unittest

from apiclient.errors import HttpError
from google.appengine.ext import testbed, ndb
from apiclient.http import HttpMockSequence
from mock import patch, Mock

from src.backup.copy_job_async.copy_job.copy_job_request import CopyJobRequest
from src.backup.copy_job_async.copy_job.copy_job_service import CopyJobService
from src.backup.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest
from src.backup.copy_job_async.result_check.result_check_request import \
    ResultCheckRequest
from src.backup.copy_job_async.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table import BigQueryTable
from tests.test_utils import content


class TestCopyJobService(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        ndb.get_context().clear_cache()
        patch(
            'oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self._create_http = patch.object(BigQuery, '_create_http').start()

        self.example_source_bq_table = BigQueryTable("source_project_id_1",
                                                     "source_dataset_id_1",
                                                     "source_table_id_1")
        self.example_target_bq_table = BigQueryTable("target_project_id_1",
                                                     "target_dataset_id_1",
                                                     "target_table_id_1")

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    @patch.object(BigQuery, 'insert_job', return_value="job_id_123")
    @patch.object(TaskCreator, 'create_copy_job_result_check')
    def test_that_post_copy_action_request_is_passed(
            self, create_copy_job_result_check, _):
        # given
        patch('googleapiclient.discovery.build').start()
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        # when
        CopyJobService().run_copy_job_request(
            CopyJobRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id="test-process",
                source_big_query_table=self.example_source_bq_table,
                target_big_query_table=self.example_target_bq_table,
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

        # then
        create_copy_job_result_check.assert_called_once_with(
            ResultCheckRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id="test-process",
                project_id="target_project_id_1",
                job_id="job_id_123",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

    @patch.object(BigQuery, 'insert_job')
    @patch('time.sleep', side_effect=lambda _: None)
    def test_that_copy_table_should_throw_error_after_exception_not_being_http_error_thrown_on_copy_job_creation(
            self, _, insert_job):
        # given
        patch('googleapiclient.discovery.build').start()
        error_message = "test exception"
        insert_job.side_effect = Exception(error_message)
        request = CopyJobRequest(
            task_name_suffix=None,
            copy_job_type_id=None,
            source_big_query_table=self.example_source_bq_table,
            target_big_query_table=self.example_target_bq_table
        )

        # when
        with self.assertRaises(Exception) as context:
            CopyJobService().run_copy_job_request(request)

        # then
        self.assertTrue(error_message in context.exception)

    @patch.object(BigQuery, 'insert_job')
    @patch('time.sleep', side_effect=lambda _: None)
    def test_that_copy_table_should_throw_error_after_http_error_different_than_404_thrown_on_copy_job_creation(
            self, _, insert_job):
        # given
        patch('googleapiclient.discovery.build').start()
        exception = HttpError(Mock(status=500), "internal error")
        insert_job.side_effect = exception
        request = CopyJobRequest(
            task_name_suffix=None,
            copy_job_type_id=None,
            source_big_query_table=self.example_source_bq_table,
            target_big_query_table=self.example_target_bq_table
        )

        # when
        with self.assertRaises(HttpError) as context:
            CopyJobService().run_copy_job_request(request)

        # then
        self.assertEqual(context.exception, exception)

    @patch.object(BigQuery, 'insert_job')
    @patch.object(TaskCreator, 'create_post_copy_action')
    def test_that_copy_table_should_create_correct_post_copy_action_if_404_http_error_thrown_on_copy_job_creation(
            self, create_post_copy_action, insert_job):
        # given
        patch('googleapiclient.discovery.build').start()
        insert_job.side_effect = HttpError(Mock(status=404), "not found")
        post_copy_action_request = PostCopyActionRequest(url="/my/url", data={"key1": "value1"})
        request = CopyJobRequest(
            task_name_suffix='task_name_suffix',
            copy_job_type_id="test-process",
            source_big_query_table=self.example_source_bq_table,
            target_big_query_table=self.example_target_bq_table,
            retry_count=0,
            post_copy_action_request=post_copy_action_request
        )

        # when
        CopyJobService().run_copy_job_request(request)

        # then
        create_post_copy_action.assert_called_once_with(
            copy_job_type_id="test-process",
            post_copy_action_request=post_copy_action_request,
            job_json={
                "status": {
                    "state": "DONE",
                    "errors": [
                        {
                            "reason": "invalid",
                            "message": "Job not scheduled"
                        }
                    ]
                },
                "configuration": {
                    "copy": {
                        "sourceTable": {
                            "projectId": self.example_source_bq_table.get_project_id(),
                            "tableId": self.example_source_bq_table.get_table_id(),
                            "datasetId": self.example_source_bq_table.get_dataset_id()
                        },
                        "destinationTable": {
                            "projectId": self.example_target_bq_table.get_project_id(),
                            "tableId": self.example_target_bq_table.get_table_id(),
                            "datasetId": self.example_target_bq_table.get_dataset_id()
                        }
                    }
                }
            }
        )

    @patch.object(TaskCreator, 'create_copy_job_result_check')
    @patch.object(CopyJobService, '_create_random_job_id',
                  return_value="random_job_123")
    @patch('time.sleep', side_effect=lambda _: None)
    def test_should_handle_job_already_exist_error(self, _,
                                                   _create_random_job_id,
                                                   create_copy_job_result_check):
        # given
        self._create_http.return_value = HttpMockSequence([
            ({'status': '503'},
             content('tests/json_samples/bigquery_503_error.json')),
            ({'status': '409'},
             content('tests/json_samples/bigquery_409_error.json'))
        ])
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        # when
        CopyJobService().run_copy_job_request(
            CopyJobRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id="test-process",
                source_big_query_table=self.example_source_bq_table,
                target_big_query_table=self.example_target_bq_table,
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

        # then
        create_copy_job_result_check.assert_called_once_with(
            ResultCheckRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id="test-process",
                project_id="target_project_id_1",
                job_id="random_job_123",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )
