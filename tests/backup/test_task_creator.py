import os
import unittest

from google.appengine.ext import testbed
from mock import patch

from src.commons.test_utils import utils
from src import request_correlation_id
from src.backup.task_creator import TaskCreator


class TestTaskCreator(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(
            root_path=os.path.join(os.path.dirname(__file__), 'resources'))
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(request_correlation_id, 'get',
                  return_value='correlation-id')
    def test_creating_task_in_taskqueue(self, _):
        # when
        TaskCreator.create_task_for_partition_backup("test-project",
                                                     "test-dataset",
                                                     "test-table",
                                                     "20170330")
        # then
        tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names='backup-worker')
        self.assertEqual(len(tasks), 1, "Should create one task in queue")
        header_value = tasks[0].headers[
            request_correlation_id.HEADER_NAME]
        self.assertEqual(header_value, 'correlation-id')
