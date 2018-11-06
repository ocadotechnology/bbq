import unittest

from apiclient.errors import HttpError
from google.appengine.ext import testbed, ndb
from mock import patch, Mock

from src.commons.copy_job_async.copy_job.copy_job_request import CopyJobRequest
from src.commons.copy_job_async.copy_job.copy_job_service import CopyJobService
from src.commons.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest
from src.commons.copy_job_async.result_check.result_check_request import \
    ResultCheckRequest
from src.commons.copy_job_async.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from src.commons.big_query.big_query_table import BigQueryTable


class TestCopyJobService(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        ndb.get_context().clear_cache()
        patch('googleapiclient.discovery.build').start()
        patch(
            'oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self._create_http = patch.object(BigQuery, '_create_http').start()

        self.example_source_bq_table = BigQueryTable('source_project_id_1',
                                                     'source_dataset_id_1',
                                                     'source_table_id_1')
        self.example_target_bq_table = BigQueryTable('target_project_id_1',
                                                     'target_dataset_id_1',
                                                     'target_table_id_1')

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    @patch.object(BigQuery, 'insert_job',
                  return_value=BigQueryJobReference(
                      project_id='test_project',
                      job_id='job123',
                      location='EU'))
    @patch.object(TaskCreator, 'create_copy_job_result_check')
    def test_that_post_copy_action_request_is_passed(
        self, create_copy_job_result_check, _):
        # given
        post_copy_action_request = \
            PostCopyActionRequest(url='/my/url', data={'key1': 'value1'})

        # when
        CopyJobService().run_copy_job_request(
            CopyJobRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id='test-process',
                source_big_query_table=self.example_source_bq_table,
                target_big_query_table=self.example_target_bq_table,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

        # then
        create_copy_job_result_check.assert_called_once_with(
            ResultCheckRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id='test-process',
                job_reference=BigQueryJobReference(
                    project_id='test_project',
                    job_id='job123',
                    location='EU'),
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )
        @patch.object(BigQuery, 'insert_job',
                      return_value=BigQueryJobReference(
                          project_id='test_project',
                          job_id='job123',
                          location='EU'))
        @patch.object(TaskCreator, 'create_copy_job_result_check')
        def test_that_create_and_write_disposition_are_passed_to_result_check(
            self, create_copy_job_result_check, _):
            # given
            create_disposition = "SOME_CREATE_DISPOSITION"
            write_disposition = "SOME_WRITE_DISPOSITION"

            # when
            CopyJobService().run_copy_job_request(
                CopyJobRequest(
                    task_name_suffix='task_name_suffix',
                    copy_job_type_id='test-process',
                    source_big_query_table=self.example_source_bq_table,
                    target_big_query_table=self.example_target_bq_table,
                    create_disposition=create_disposition,
                    write_disposition=write_disposition,
                    retry_count=0,
                    post_copy_action_request=None
                )
            )

            # then
            create_copy_job_result_check.assert_called_once_with(
                ResultCheckRequest(
                    task_name_suffix='task_name_suffix',
                    copy_job_type_id='test-process',
                    job_reference=BigQueryJobReference(
                        project_id='test_project',
                        job_id='job123',
                        location='EU'),
                    create_disposition=create_disposition,
                    write_disposition=write_disposition,
                    retry_count=0,
                    post_copy_action_request=None
                )
            )


    @patch.object(BigQuery, 'insert_job')
    @patch('time.sleep', side_effect=lambda _: None)
    def test_that_copy_table_should_throw_error_after_exception_not_being_http_error_thrown_on_copy_job_creation(
        self, _, insert_job):
        # given
        error_message = 'test exception'
        insert_job.side_effect = Exception(error_message)
        request = CopyJobRequest(
            task_name_suffix=None,
            copy_job_type_id=None,
            source_big_query_table=self.example_source_bq_table,
            target_big_query_table=self.example_target_bq_table,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY"
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
        exception = HttpError(Mock(status=500), 'internal error')
        insert_job.side_effect = exception
        request = CopyJobRequest(
            task_name_suffix=None,
            copy_job_type_id=None,
            source_big_query_table=self.example_source_bq_table,
            target_big_query_table=self.example_target_bq_table,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY"
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
        insert_job.side_effect = HttpError(Mock(status=404), 'not found')
        post_copy_action_request = PostCopyActionRequest(url='/my/url', data={'key1': 'value1'})
        request = CopyJobRequest(
            task_name_suffix='task_name_suffix',
            copy_job_type_id='test-process',
            source_big_query_table=self.example_source_bq_table,
            target_big_query_table=self.example_target_bq_table,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY",
            retry_count=0,
            post_copy_action_request=post_copy_action_request
        )

        # when
        CopyJobService().run_copy_job_request(request)

        # then
        create_post_copy_action.assert_called_once_with(
            copy_job_type_id='test-process',
            post_copy_action_request=post_copy_action_request,
            job_json={
                'status': {
                    'state': 'DONE',
                    'errors': [
                        {
                            'reason': 'invalid',
                            'message': 'Job not scheduled'
                        }
                    ]
                },
                'configuration': {
                    'copy': {
                        'sourceTable': {
                            'projectId': self.example_source_bq_table.get_project_id(),
                            'tableId': self.example_source_bq_table.get_table_id(),
                            'datasetId': self.example_source_bq_table.get_dataset_id()
                        },
                        'destinationTable': {
                            'projectId': self.example_target_bq_table.get_project_id(),
                            'tableId': self.example_target_bq_table.get_table_id(),
                            'datasetId': self.example_target_bq_table.get_dataset_id()
                        }
                    }
                }
            }
        )

    @patch('src.commons.big_query.big_query_table_metadata.BigQueryTableMetadata')
    @patch.object(TaskCreator, 'create_copy_job_result_check')
    @patch.object(CopyJobService, '_create_random_job_id',
                  return_value='random_job_123')
    @patch.object(BigQuery, 'insert_job',
                  side_effect=[HttpError(Mock(status=503), 'internal error'),
                               HttpError(Mock(status=409), 'job exists')])
    @patch('time.sleep', side_effect=lambda _: None)
    def test_bug_regression_job_already_exists_after_internal_error(
        self, _, insert_job, _create_random_job_id,
        create_copy_job_result_check, table_metadata
    ):
        # given
        post_copy_action_request = \
            PostCopyActionRequest(url='/my/url', data={'key1': 'value1'})
        table_metadata._BigQueryTableMetadata__get_table_or_partition.return_value.get_location.return_value='EU'

        # when
        CopyJobService().run_copy_job_request(
            CopyJobRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id='test-process',
                source_big_query_table=self.example_source_bq_table,
                target_big_query_table=self.example_target_bq_table,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

        # then
        self.assertEqual(insert_job.call_count, 2)
        create_copy_job_result_check.assert_called_once_with(
            ResultCheckRequest(
                task_name_suffix='task_name_suffix',
                copy_job_type_id='test-process',
                job_reference=BigQueryJobReference(
                    project_id='target_project_id_1',
                    job_id='random_job_123',
                    location='EU'),
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

