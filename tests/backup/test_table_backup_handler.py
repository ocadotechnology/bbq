import os

from src.commons.table_reference import TableReference

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch

from src.backup import table_backup_handler
from src.backup.table_backup import TableBackup


class TestTableBackupHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        app = table_backup_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(TableBackup, 'start')
    def test_that_backup_endpoint_call_for_partition_is_properly_parsed(
            self, table_backup_start
    ):
        # given
        table_reference = TableReference('example-proj-name',
                                         'example-dataset-name',
                                         'example-table-name',
                                         '20171201')
        url = '/tasks/backups/table/{}/{}/{}/{}' \
            .format(table_reference.get_project_id(),
                    table_reference.get_dataset_id(),
                    table_reference.get_table_id(),
                    table_reference.get_partition_id())

        # when
        self.under_test.get(url)

        # then
        table_backup_start.assert_called_with(table_reference)

    @patch.object(TableBackup, 'start')
    def test_that_backup_endpoint_call_for_non_partition_is_properly_parsed(
        self, table_backup_start
    ):
        # given
        table_reference = TableReference('example-proj-name',
                                         'example-dataset-name',
                                         'example-table-name'
                                         )
        url = '/tasks/backups/table/{}/{}/{}' \
          .format(table_reference.get_project_id(),
                  table_reference.get_dataset_id(),
                  table_reference.get_table_id())

        # when
        self.under_test.get(url)

        # then
        table_backup_start.assert_called_with(table_reference)