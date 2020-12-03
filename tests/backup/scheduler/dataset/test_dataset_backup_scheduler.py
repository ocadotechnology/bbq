import os
import unittest

from google.appengine.ext import testbed, ndb
from mock import patch

from src.backup.scheduler.dataset.dataset_backup_scheduler import \
    DatasetBackupScheduler
from src.commons import request_correlation_id
from src.commons.test_utils import utils


class TestDatasetBackupScheduler(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.task_queue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_table_ids')
    def test_dataset_backup_scheduler_should_schedule_table_backup_tasks(
        self, list_table_ids, _1):
        # given
        list_table_ids.return_value = (['table1', 'table2'], None)
        # when
        DatasetBackupScheduler().schedule_backup(project_id='test-project-id',
                                                 dataset_id='dataset_1')

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-worker')

        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].url,
                         '/tasks/backups/table/test-project-id/dataset_1/table1')
        self.assertEqual(tasks[1].url,
                         '/tasks/backups/table/test-project-id/dataset_1/table2')

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_table_ids')
    def test_dataset_backup_scheduler_should_schedule_next_dataset_scheduler_task_if_next_token_returned(
        self, list_table_ids, _1):
        # given
        list_table_ids.return_value = (['table1', 'table2'], 'next_token')
        # when
        DatasetBackupScheduler().schedule_backup(project_id='test-project-id',
                                                 dataset_id='dataset_1')

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-worker')
        self.assertEqual(len(tasks), 2)

        scheduler_tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')
        self.assertEqual(len(scheduler_tasks), 1)
        self.assertEqual(scheduler_tasks[0].payload,
                         'projectId=test-project-id&datasetId=dataset_1&pageToken=next_token')

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_table_ids')
    def test_dataset_backup_scheduler_should_schedule_from_next_token_if_passed(
        self, list_table_ids, _1):
        # given
        list_table_ids.return_value = (['table1', 'table2'], None)
        # when
        DatasetBackupScheduler().schedule_backup(project_id='test-project-id',
                                                 dataset_id='dataset_1',
                                                 page_token='next_token')

        # then
        list_table_ids.assert_called_once_with(project_id='test-project-id',
                                               dataset_id='dataset_1',
                                               page_token='next_token')

        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-worker')
        self.assertEqual(len(tasks), 2)
