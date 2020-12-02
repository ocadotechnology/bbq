import os

from src.backup.scheduler.partitioned_table import \
    partitioned_table_backup_scheduler_handler
from src.backup.scheduler.partitioned_table.partitioned_table_backup_scheduler import \
    PartitionedTableBackupScheduler
from src.commons.big_query.big_query import BigQuery

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch


class TestPartitionedTableBackupSchedulerHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self._create_http = patch.object(BigQuery, '_create_http').start()
        app = partitioned_table_backup_scheduler_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(PartitionedTableBackupScheduler, "schedule_backup")
    def test_that_dataset_backup_scheduler_parse_arguments_correctly(self,
        scheduler):
        # when
        self.under_test.post(url='/tasks/schedulebackup/partitionedtable',
                             params={"projectId": "project-id",
                                     "datasetId": "dataset_id",
                                     "tableId": "table_id"})

        # then
        scheduler.called_only_once_with(project_id="project-id",
                                        dataset_id="dataset_id",
                                        table_id="table_id")
