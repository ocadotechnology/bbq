import unittest

from google.appengine.ext import testbed, ndb
from mock import patch

from src.commons.big_query.copy_job_async.copy_job.copy_job_request \
    import CopyJobRequest
from src.commons.big_query.copy_job_async.copy_job_service_async \
    import CopyJobServiceAsync
from src.commons.big_query.copy_job_async.post_copy_action_request \
    import PostCopyActionRequest
from src.commons.big_query.copy_job_async.task_creator \
    import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table import BigQueryTable


class TestCopyJobServiceAsync(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        ndb.get_context().clear_cache()
        patch('googleapiclient.discovery.build').start()
        patch(
            'oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    def create_example_target_bq_table(self):
        return BigQueryTable("target_project_id_1",
                             "target_dataset_id_1",
                             "target_table_id_1")

    def create_example_source_bq_table(self):
        return BigQueryTable("source_project_id_1",
                             "source_dataset_id_1",
                             "source_table_id_1")

    @patch.object(BigQuery, 'insert_job', return_value="job_id_123")
    @patch.object(TaskCreator, 'create_copy_job')
    def test_that_queue_task_was_invoked_with_default_retry_count_value(
            self, create_copy_job, _):
        # given
        # when
        CopyJobServiceAsync(
            copy_job_type_id="test-process",
            task_name_suffix="example_sufix"
        ).copy_table(
            self.create_example_source_bq_table(),
            self.create_example_target_bq_table()
        )

        # then
        expected_retry_count = 0
        create_copy_job.assert_called_once_with(
            CopyJobRequest(
                task_name_suffix="example_sufix",
                copy_job_type_id="test-process",
                source_big_query_table=(self.create_example_source_bq_table()),
                target_big_query_table=(self.create_example_target_bq_table()),
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=expected_retry_count
            )
        )

    @patch.object(TaskCreator, 'create_copy_job')
    def test_that_post_copy_action_request_is_passed(
            self, create_copy_job):
        # given
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        # when
        CopyJobServiceAsync(
            copy_job_type_id="test-process",
            task_name_suffix="example_sufix"
        ).with_post_action(
            post_copy_action_request
        ).copy_table(
            self.create_example_source_bq_table(),
            self.create_example_target_bq_table()
        )


        # then
        create_copy_job.assert_called_once_with(
            CopyJobRequest(
                task_name_suffix="example_sufix",
                copy_job_type_id="test-process",
                source_big_query_table=(self.create_example_source_bq_table()),
                target_big_query_table=(self.create_example_target_bq_table()),
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

    @patch.object(TaskCreator, 'create_copy_job')
    def test_that_create_and_write_disposition_are_passed_if_specified(
        self, create_copy_job):
        # given
        create_dispositon = "SOME_CREATE_DISPOSITON"
        write_dispostion = "SOME_WRITE_DISPOSTION"

        # when
        CopyJobServiceAsync(
            copy_job_type_id="test-process",
            task_name_suffix="example_sufix"
        )\
        .with_create_disposition(create_dispositon)\
        .with_write_disposition(write_dispostion)\
        .copy_table(
            self.create_example_source_bq_table(),
            self.create_example_target_bq_table()
        )

        # then
        create_copy_job.assert_called_once_with(
            CopyJobRequest(
                task_name_suffix="example_sufix",
                copy_job_type_id="test-process",
                source_big_query_table=(self.create_example_source_bq_table()),
                target_big_query_table=(self.create_example_target_bq_table()),
                create_disposition=create_dispositon,
                write_disposition=write_dispostion,
                retry_count=0,
                post_copy_action_request=None
            )
        )

    def test_that_assertion_erro_if_no_type_provided(self):
        with self.assertRaises(AssertionError) as error:
            CopyJobServiceAsync(
                copy_job_type_id=None,
                task_name_suffix="example_sufix"
            ).copy_table(None, None)

        self.assertEqual(error.exception.message, "copy_job_type_id needs to be assigned in constructor")

    def test_that_assertion_error_if_no_task_name_suffix_provided(self):
        with self.assertRaises(AssertionError) as error:
            CopyJobServiceAsync(
                copy_job_type_id="test-process",
                task_name_suffix=None
            ).copy_table(None, None)

        self.assertEqual(error.exception.message, "task_name_suffix needs to be assigned in constructor")
