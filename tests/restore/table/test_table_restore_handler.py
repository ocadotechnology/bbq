import os

from datetime import datetime

from commons.exceptions import NotFoundException
from src.restore.table import table_restore_handler
from src.restore.table.table_restore_service import TableRestoreService
from src.table_reference import TableReference

os.environ['SERVER_SOFTWARE'] = 'Development/'

import unittest

import webtest
from mock import patch


class TestTableRestoreHandler(unittest.TestCase):

    def setUp(self):
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(table_restore_handler.app)

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_handle_partition_id(self, restore):
        # given & when
        self.under_test.post(
            '/restore/table/project-id/dataset_id/table_id/20180101',
        )

        # then
        expected_table_reference = \
            TableReference('project-id', 'dataset_id', 'table_id', '20180101')
        restore.assert_called_once_with(expected_table_reference, None, None)

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_all_proper_parameters_provided_for_table_restoration(self,
                                                                  restore):
        # given & when
        self.under_test.post(
            '/restore/table/project-id/dataset_id/table_id',
            params={'targetDatasetId': 'target_dataset_id',
                    'restorationDate': '2017-07-25'}
        )

        # then
        restore.assert_called_once_with(
            TableReference('project-id', 'dataset_id', 'table_id'),
            'target_dataset_id',
            datetime(2017, 07, 25, 23, 59, 59)
        )

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_default_parameters_for_table_restoration(self, restore):
        # given & when
        self.under_test.post('/restore/table/project-id/dataset_id/table_id')

        # then
        expected_table_reference = \
            TableReference('project-id', 'dataset_id', 'table_id')
        restore.assert_called_once_with(expected_table_reference, None, None)

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_should_fail_on_wrong_date_format(self, _):
        # given & when
        response = self.under_test.post(
            '/restore/table/project-id/dataset_id/table_id',
            params={'restorationDate': '20170725'},
            expect_errors=True
        )

        # then
        expected_error = '{"status": "failed", "message": ' \
                         '"Wrong date value format for parameter ' \
                         '\'restoration_date\'. Should be \'YYYY-mm-dd\'", ' \
                         '"httpStatus": 400}'
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @patch.object(TableRestoreService, 'restore',
                  side_effect=NotFoundException("Error message"))
    def test_should_forward_backup_not_found_error(self, _):
        # given & when
        response = self.under_test.post(
            '/restore/table/project-id/dataset_id/table_id',
            params={'restorationDate': '2017-07-25'},
            expect_errors=True
        )

        # then
        expected_error = '{"status": "failed", "message": "Error message", ' \
                         '"httpStatus": 404}'
        self.assertEquals(404, response.status_int)
        self.assertEquals(response.body, expected_error)
