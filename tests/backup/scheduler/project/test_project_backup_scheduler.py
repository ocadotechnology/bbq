import os
import unittest

from google.appengine.ext import testbed, ndb
from mock import patch

from src.backup.scheduler.project.project_backup_scheduler import \
    ProjectBackupScheduler
from src.commons import request_correlation_id
from src.commons.test_utils import utils


class TestProjectBackupScheduler(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.task_queue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    def test_project_backup_scheduler_should_schedule_dataset_scheduler_tasks(
        self, list_dataset_ids, _1):
        # given
        list_dataset_ids.return_value = (['dataset_id1', 'dataset_id2'], None)
        # when
        ProjectBackupScheduler().schedule_backup(project_id='test-project-id')

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')

        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].url, '/tasks/schedulebackup/dataset')
        self.assertEqual(tasks[0].payload,
                         'projectId=test-project-id&datasetId=dataset_id1&pageToken=None')
        self.assertEqual(tasks[1].url, '/tasks/schedulebackup/dataset')
        self.assertEqual(tasks[1].payload,
                         'projectId=test-project-id&datasetId=dataset_id2&pageToken=None')

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    def test_project_backup_scheduler_should_schedule_next_project_scheduler_task_if_next_token_returned(
        self, list_dataset_ids, _1):
        # given
        list_dataset_ids.return_value = (
        ['dataset_id1', 'dataset_id2'], "next_token")
        # when
        ProjectBackupScheduler().schedule_backup(project_id='test-project-id')

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')

        self.assertEqual(len(tasks), 3)
        self.assertEqual(tasks[2].url, '/tasks/schedulebackup/project')
        self.assertEqual(tasks[2].payload,
                         'projectId=test-project-id&pageToken=next_token')


    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    def test_project_backup_scheduler_should_schedule_from_next_token_if_passed(
        self, list_dataset_ids, _1):
        # given
        list_dataset_ids.return_value = (
            ['dataset_id1', 'dataset_id2'], None)
        # when
        ProjectBackupScheduler().schedule_backup(project_id='test-project-id', page_token='next_token')

        # then
        list_dataset_ids.assert_called_once_with(project_id='test-project-id', page_token='next_token')

        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')
        self.assertEqual(len(tasks), 2)
