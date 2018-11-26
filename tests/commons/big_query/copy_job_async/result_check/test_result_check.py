import unittest

from google.appengine.ext import testbed, ndb
from mock import patch

from src.commons.big_query.copy_job_async.copy_job.copy_job_request \
    import CopyJobRequest
from src.commons.big_query.copy_job_async.copy_job_result \
    import CopyJobResult
from src.commons.big_query.copy_job_async.result_check.result_check \
    import ResultCheck
from src.commons.big_query.copy_job_async.task_creator \
    import TaskCreator
from src.commons.big_query.copy_job_async.post_copy_action_request \
    import PostCopyActionRequest
from src.commons.big_query.copy_job_async.result_check.result_check_request \
    import ResultCheckRequest
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from tests.commons.big_query.copy_job_async.result_check.job_result_example \
    import JobResultExample


class TestResultCheck(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()
        patch('googleapiclient.discovery.build').start()
        patch(
            'oauth2client.client.GoogleCredentials.get_application_default') \
            .start()

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    def create_example_result_check_request(self):
        retry_count = 0
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})
        # when
        result_check_request = ResultCheckRequest(
            task_name_suffix="task_name_suffix",
            copy_job_type_id="backups",
            job_reference=BigQueryJobReference(project_id="target_project_id",
                                               job_id="job_id",
                                               location='EU'),
            retry_count=retry_count,
            post_copy_action_request=post_copy_action_request)
        return result_check_request

    @patch.object(BigQuery, 'get_job', return_value=JobResultExample.DONE)
    @patch.object(TaskCreator, 'create_post_copy_action')
    def test_that_after_successful_job_post_copy_action_request_is_created(
            self, create_post_copy_action, _):
        # given
        # when
        ResultCheck().check(self.create_example_result_check_request())

        # then
        create_post_copy_action.assert_called_once_with(
            copy_job_type_id="backups",
            post_copy_action_request=PostCopyActionRequest(url="/my/url", data={
                "key1": "value1"}),
            job_json=JobResultExample.DONE
        )

    @patch.object(BigQuery, 'get_job', return_value=JobResultExample.DONE)
    @patch.object(TaskCreator, 'create_post_copy_action')
    def test_that_after_successful_job_no_post_action_is_created(
            self, create_post_copy_action, _):
        # given
        post_copy_action_request = None

        # when
        ResultCheck().check(ResultCheckRequest(
            task_name_suffix='task_name_suffix',
            copy_job_type_id="backups",
            job_reference=BigQueryJobReference(project_id="target_project_id",
                                               job_id="job_id",
                                               location='EU'),
            retry_count=0,
            post_copy_action_request=post_copy_action_request)
        )

        # then
        create_post_copy_action.assert_not_called()

    @patch.object(BigQuery, 'get_job',
                  return_value=JobResultExample.IN_PROGRESS)
    @patch.object(TaskCreator, 'create_copy_job_result_check')
    def test_that_reschedule_itself_if_job_still_in_progress(
            self, create_copy_job_result_check, _):
        # given
        # when
        result_check_request = self.create_example_result_check_request()
        ResultCheck().check(result_check_request)

        # then
        create_copy_job_result_check.assert_called_once_with(
            result_check_request
        )

    @patch.object(BigQuery, 'get_job',
                  return_value=JobResultExample.DONE_WITH_NOT_REPETITIVE_ERRORS)
    @patch.object(TaskCreator, 'create_copy_job')
    @patch.object(TaskCreator, 'create_post_copy_action')
    def test_that_should_execute_post_copy_action_request_if_not_repetitive_error_occurs(
            self, create_post_copy_action, create_copy_job, _):
        # given
        # when
        ResultCheck().check(self.create_example_result_check_request())

        # then
        create_post_copy_action.assert_called_once_with(
            copy_job_type_id="backups",
            post_copy_action_request=PostCopyActionRequest(url="/my/url", data={
                "key1": "value1"}),
            job_json=JobResultExample.DONE_WITH_NOT_REPETITIVE_ERRORS
        )
        create_copy_job.assert_not_called()

    @patch.object(BigQuery, 'get_job',
                  return_value=JobResultExample.DONE_WITH_RETRY_ERRORS)
    @patch.object(TaskCreator, 'create_copy_job')
    def test_that_should_re_trigger_copy_job_task_if_retry_error_occurs(
            self, create_copy_job, _):
        # given
        retry_count = 0
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        # when
        ResultCheck().check(ResultCheckRequest(
            task_name_suffix="task_name_suffix",
            copy_job_type_id="backups",
            job_reference=BigQueryJobReference(project_id="target_project_id",
                                               job_id="job_id",
                                               location='EU'),
            retry_count=retry_count,
            post_copy_action_request=post_copy_action_request))

        # then
        copy_job_result = CopyJobResult(JobResultExample.DONE_WITH_RETRY_ERRORS)

        copy_job_request = CopyJobRequest(
            task_name_suffix=None,
            copy_job_type_id="backups",
            source_big_query_table=copy_job_result.source_bq_table,
            target_big_query_table=copy_job_result.target_bq_table,
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_TRUNCATE",
            retry_count=retry_count + 1,
            post_copy_action_request=post_copy_action_request
        )
        create_copy_job.assert_called_once_with(copy_job_request)

    @patch.object(BigQuery, 'get_job',
                  return_value=JobResultExample.DONE_WITH_RETRY_ERRORS)
    @patch.object(TaskCreator, 'create_copy_job')
    def test_that_should_re_trigger_copy_job_task_with_proper_create_and_write_dispositions_if_retry_error_occurs(
            self, create_copy_job, _):
        # given
        retry_count = 0
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})
        create_disposition = "CREATE_NEVER"
        write_disposition = "WRITE_TRUNCATE"

        # when
        ResultCheck().check(ResultCheckRequest(
            task_name_suffix="task_name_suffix",
            copy_job_type_id="backups",
            job_reference=BigQueryJobReference(project_id="target_project_id",
                                               job_id="job_id",
                                               location='EU'),
            retry_count=retry_count,
            post_copy_action_request=post_copy_action_request))

        # then
        copy_job_result = CopyJobResult(JobResultExample.DONE_WITH_RETRY_ERRORS)

        copy_job_request = CopyJobRequest(
            task_name_suffix=None,
            copy_job_type_id="backups",
            source_big_query_table=copy_job_result.source_bq_table,
            target_big_query_table=copy_job_result.target_bq_table,
            create_disposition=create_disposition,
            write_disposition=write_disposition,
            retry_count=retry_count + 1,
            post_copy_action_request=post_copy_action_request
        )
        create_copy_job.assert_called_once_with(copy_job_request)

    @patch.object(BigQuery, 'get_job',
                  return_value=JobResultExample.DONE_WITH_RETRY_ERRORS)
    @patch.object(TaskCreator, 'create_copy_job')
    def test_that_should_stop_if_max_retry_exceeded(
            self, create_copy_job, _):
        # given
        retry_count = 5

        # when
        ResultCheck().check(ResultCheckRequest(
            task_name_suffix="task_name_suffix",
            copy_job_type_id="backups",
            job_reference=BigQueryJobReference(project_id="target_project_id",
                                               job_id="job_id",
                                               location='EU'),
            retry_count=retry_count,
            post_copy_action_request=None
        ))

        retry_count += 1

        ResultCheck().check(ResultCheckRequest(
            task_name_suffix="task_name_suffix",
            copy_job_type_id="backups",
            job_reference=BigQueryJobReference(project_id="target_project_id",
                                               job_id="job_id",
                                               location='EU'),
            retry_count=retry_count,
            post_copy_action_request=None
        ))

        # then
        create_copy_job.assert_called_once()
