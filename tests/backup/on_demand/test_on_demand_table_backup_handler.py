import os

from src.backup.on_demand import on_demand_table_backup_handler
from src.backup.on_demand.on_demand_table_backup import OnDemandTableBackup
from src.commons.exceptions import ParameterValidationException
from src.commons.table_reference import TableReference

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch


class TestOnDemandTableBackupHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        app = on_demand_table_backup_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(OnDemandTableBackup, 'start')
    def test_on_demand_request_for_partitioned_table_is_properly_parsed(
            self, on_demand_table_backup_start):
        # given
        table_reference = TableReference('example-proj-name',
                                         'example-dataset-name',
                                         'example-table-name',
                                         '20171201')
        url = '/tasks/backups/on_demand/table/{}/{}/{}/{}' \
            .format(table_reference.get_project_id(),
                    table_reference.get_dataset_id(),
                    table_reference.get_table_id(),
                    table_reference.get_partition_id())

        # when
        self.under_test.get(url)

        # then
        on_demand_table_backup_start.assert_called_with(table_reference)

    @patch.object(OnDemandTableBackup, 'start')
    def test_on_demand_request_for_non_partitioned_table_is_properly_parsed(
            self, on_demand_table_backup_start):
        # given
        table_reference = TableReference('example-proj-name',
                                         'example-dataset-name',
                                         'example-table-name')
        url = '/tasks/backups/on_demand/table/{}/{}/{}'.format(
            table_reference.get_project_id(),
            table_reference.get_dataset_id(),
            table_reference.get_table_id())

        # when
        self.under_test.get(url)

        # then
        on_demand_table_backup_start.assert_called_with(table_reference)

    @patch.object(OnDemandTableBackup, 'start', side_effect=ParameterValidationException("error msg"))
    def test_should_return_400_if_parameter_validation_exception(
            self, on_demand_table_backup_start):
        # given
        table_reference = TableReference('example-proj-name',
                                         'example-dataset-name',
                                         'example-table-name')
        url = '/tasks/backups/on_demand/table/{}/{}/{}'.format(
            table_reference.get_project_id(),
            table_reference.get_dataset_id(),
            table_reference.get_table_id())

        # when
        response = self.under_test.get(url, expect_errors=True)

        # then
        self.assertEquals(400, response.status_int)
