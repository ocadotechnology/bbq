import unittest

import os
from google.appengine.api.taskqueue import Task
from google.appengine.ext import testbed
from mock import patch

from src.commons.test_utils import utils
from src.commons import request_correlation_id
from src.commons.tasks import Tasks


class TestTasks(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(
            root_path=os.path.join(os.path.dirname(__file__), 'resources'))
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    def test_schedule_can_work_with_array_of_tasks(self):
        # given
        task1 = Task(
            url='/example/task1',
        )
        task2 = Task(
            url='/example/task2',
        )
        # when
        Tasks.schedule("default", [task1, task2])
        # then
        executed_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names="default"
        )

        self.assertEqual(len(executed_tasks), 2, "Should create two tasks in queue")
        self.assertEqual(executed_tasks[0].url, '/example/task1')
        self.assertEqual(executed_tasks[1].url, '/example/task2')

    def test_schedule_can_work_with_single_task(self):
        # given
        task1 = Task(
            url='/example/task1',
        )
        # when
        Tasks.schedule("default", task1)
        # then
        executed_tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names="default"
        )

        self.assertEqual(len(executed_tasks), 1, "Should create one task in queue")
        self.assertEqual(executed_tasks[0].url, '/example/task1')

    @patch.object(request_correlation_id, 'get',
                  return_value='correlation-id')
    def test_should_create_task_with_correlation_id_header(self, _):
        # when
        task = Tasks.create(url='/example/task1')
        # then
        header_value = task.headers[request_correlation_id.HEADER_NAME]
        self.assertEqual(header_value, 'correlation-id')

    @patch.object(request_correlation_id, 'get',
                  return_value='correlation-id')
    def test_should_create_task_with_additional_correlation_id_header(self, _):
        # when
        task = Tasks.create(url='/example/task1',
                            headers={'key': 'value'})
        # then
        header_value = task.headers[request_correlation_id.HEADER_NAME]
        self.assertEqual(header_value, 'correlation-id')
        self.assertEqual(task.headers['key'], 'value')

    @patch.object(request_correlation_id, 'get',
                  return_value=None)
    def test_should_create_task_without_correlation_id_header(self, _):
        # when
        task = Tasks.create(url='/example/task1',
                            headers={'key': 'value'})
        # then
        self.assertEqual(task.headers['key'], 'value')

