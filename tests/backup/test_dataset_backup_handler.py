import os

import mock
from apiclient.errors import HttpError

from src.backup.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch

from src.backup import dataset_backup_handler


class TestDatasetBackupHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self._create_http = patch.object(BigQuery, '_create_http').start()
        app = dataset_backup_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch('time.sleep', return_value=None)
    @patch.object(TaskCreator, "schedule_tasks_for_tables_backup")
    def test_that_dataset_backup_endpoint_retries_if_503(self, schedule, _):
        # given
        http_error_mock = HttpError(mock.Mock(status=503), 'Internal Error')
        schedule.side_effect = [http_error_mock, None]

        url = '/tasks/backups/dataset'
        # when
        self.under_test.post(url=url,
                             params={"projectId": "example-proj-name",
                                     "datasetId": "example-dataset-name"})

        # then
        self.assertEquals(2, schedule.call_count)

