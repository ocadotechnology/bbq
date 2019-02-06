import json
import os
import unittest

from google.appengine.api.taskqueue import UnknownQueueError
from google.appengine.ext import testbed
from mock import patch

from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.big_query.copy_job_async.copy_job.copy_job_request import \
    CopyJobRequest
from src.commons.big_query.copy_job_async.copy_job.copy_job_task_name import \
    CopyJobTaskName
from src.commons.big_query.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest
from src.commons.big_query.copy_job_async.result_check.result_check_request \
    import ResultCheckRequest
from src.commons.big_query.copy_job_async.task_creator import TaskCreator
from src.commons.encoders.request_encoder import RequestEncoder
from src.commons.test_utils import utils


class TestTaskCreator(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(
            root_path=os.path.join(os.path.dirname(__file__), 'resources'))
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(CopyJobTaskName, 'create', return_value='task_name')
    def test_copy_job_creation(self, _):
        # given
        copy_job_request = CopyJobRequest(
            task_name_suffix='task-name-suffix',
            copy_job_type_id="backups",
            source_big_query_table=BigQueryTable('source_project',
                                                 'source_dataset',
                                                 'source_table'),
            target_big_query_table=BigQueryTable('target_project',
                                                 'target_dataset',
                                                 'target_table'),
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY",
            post_copy_action_request=PostCopyActionRequest(url="/my/url", data={
                "key1": "value1"})
        )
        # when
        TaskCreator.create_copy_job(
            copy_job_request=copy_job_request
        )

        # then
        expected_queue_name = 'backups-copy-job'
        executed_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names=expected_queue_name
        )

        self.assertEqual(len(executed_tasks), 1,
                         "Should create one task in queue")
        executed_task = executed_tasks[0]
        self.assertEqual(json.dumps(copy_job_request, cls=RequestEncoder),
                         executed_task.extract_params()['copyJobRequest'])
        self.assertEqual('POST', executed_task.method)
        self.assertEqual('task_name', executed_task.name)
        self.assertEqual(executed_task.url, '/tasks/copy_job_async/copy_job')

    @patch.object(CopyJobTaskName, 'create', return_value='task_name')
    def test_copy_job_creation_throws_error_on_unknown_queue(self, _):
        # when
        with self.assertRaises(UnknownQueueError) as error:
            TaskCreator.create_copy_job(
                copy_job_request=CopyJobRequest(
                    task_name_suffix=None,
                    copy_job_type_id="unknown-copying",
                    source_big_query_table=BigQueryTable('source_project',
                                                         'source_dataset',
                                                         'source_table'),
                    target_big_query_table=BigQueryTable('target_project',
                                                         'target_dataset',
                                                         'target_table'),
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_EMPTY"
                )
            )

        self.assertEqual(
            error.exception.message, "There is no queue "
                                     "'unknown-copying-copy-job'. Please add "
                                     "it to your queue.yaml definition.")

    def test_copy_job_result_check_task_should_not_be_created_when_retry_smaller_than_0(
            self):
        with self.assertRaises(AssertionError):
            TaskCreator.create_copy_job_result_check(
                ResultCheckRequest(
                    task_name_suffix=None,
                    copy_job_type_id='backups',
                    job_reference=BigQueryJobReference(project_id="project_abc",
                                                       job_id="job123",
                                                       location='EU'),
                    retry_count=-1
                )
            )

    def test_copy_job_result_check_creation(self):
        # given
        result_check_request = ResultCheckRequest(
            task_name_suffix='task-name-suffix',
            copy_job_type_id='backups',
            job_reference=BigQueryJobReference(project_id="project_abc",
                                               job_id="job123",
                                               location='EU'),
            retry_count=2,
            post_copy_action_request=PostCopyActionRequest(
                url="/my/url",
                data={"key1": "value1"})
        )
        TaskCreator.create_copy_job_result_check(result_check_request)

        # then
        expected_queue_name = 'backups-result-check'
        executed_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names=expected_queue_name
        )

        self.assertEqual(len(executed_tasks), 1,
                         "Should create one task in queue")
        executed_task = executed_tasks[0]
        self.assertEqual(json.dumps(result_check_request, cls=RequestEncoder),
                         executed_task.extract_params()['resultCheckRequest'])
        self.assertEqual('POST', executed_task.method)
        self.assertEqual(executed_task.url,
                         '/tasks/copy_job_async/result_check')

    def test_create_copy_job_result_check_throws_error_on_unknown_queue(self):
        # when
        with self.assertRaises(UnknownQueueError) as error:
            TaskCreator.create_copy_job_result_check(ResultCheckRequest(
                task_name_suffix=None,
                copy_job_type_id="unknown-copying",
                job_reference=BigQueryJobReference(project_id="project_abc",
                                                   job_id="job123",
                                                   location='EU'),
                retry_count=0,
                post_copy_action_request=PostCopyActionRequest(
                    '/my/post/copy/url', {'mypayload': 'mypayload_value'}))
            )
        self.assertEqual(error.exception.message,
                         "There is no queue 'unknown-copying-result-check'. "
                         "Please add it to your queue.yaml definition.")

    def test_create_post_copy_action(self):
        # when
        TaskCreator.create_post_copy_action(
            copy_job_type_id="backups",
            post_copy_action_request=PostCopyActionRequest('/my/post/copy/url',
                                                           {
                                                               'mypayload': 'mypayload_value'}),
            job_json={"state": "DONE"}
        )

        # then
        expected_queue_name = 'backups-post-copy-action'
        executed_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names=expected_queue_name
        )

        self.assertEqual(len(executed_tasks), 1,
                         "Should create one task in queue")
        executed_task = executed_tasks[0]
        self.assertEqual('POST', executed_task.method)
        self.assertEqual('/my/post/copy/url', executed_task.url)
        self.assertEqual(
            '{"jobJson": {"state": "DONE"}, "data": {"mypayload": "mypayload_value"}}',
            executed_task.payload)

    def test_create_post_copy_action_throws_error_on_unknown_queue(self):
        # when
        with self.assertRaises(UnknownQueueError) as error:
            TaskCreator.create_post_copy_action(
                copy_job_type_id="unknown-copying",
                post_copy_action_request=PostCopyActionRequest(
                    '/my/post/copy/url', {'mypayload': 'mypayload_value'}),
                job_json={"state": "DONE"})
        self.assertEqual(
            error.exception.message, "There is no queue "
                                     "'unknown-copying-post-copy-action'. "
                                     "Please add it to your queue.yaml "
                                     "definition.")
