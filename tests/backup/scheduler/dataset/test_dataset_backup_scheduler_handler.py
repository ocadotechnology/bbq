import os

from src.backup.scheduler.dataset import dataset_backup_scheduler_handler
from src.backup.scheduler.dataset.dataset_backup_scheduler import \
    DatasetBackupScheduler
from src.commons.big_query.big_query import BigQuery

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch


class TestDatasetBackupSchedulerHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self._create_http = patch.object(BigQuery, '_create_http').start()
        app = dataset_backup_scheduler_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(DatasetBackupScheduler, "schedule_backup")
    def test_that_dataset_backup_scheduler_parse_arguments_correctly(self,
        scheduler):
        # when
        self.under_test.post(url='/tasks/schedulebackup/dataset',
                             params={"projectId": "project-id",
                                     "datasetId": "dataset-id"})

        # then
        scheduler.called_only_once_with(project_id="project-id",
                                        dataset_id="dataset-id")

    @patch.object(DatasetBackupScheduler, "schedule_backup")
    def test_that_dataset_backup_scheduler_parse_arguments_with_page_token_correctly(
        self, scheduler):
        # when
        self.under_test.post(url='/tasks/schedulebackup/dataset',
                             params={"projectId": "project-id",
                                     "datasetId": "dataset-id",
                                     "pageToken": "next_page_token"})

        # then
        scheduler.called_only_once_with(project_id="project-id",
                                        dataset_id="dataset-id",
                                        page_token="next_page_token")
