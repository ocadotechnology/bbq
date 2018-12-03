import os

from datetime import datetime

from src.commons.exceptions import NotFoundException
from src.commons.table_reference import TableReference
from src.restore.table import table_restore_handler
from src.restore.table.table_restore_service import TableRestoreService

os.environ['SERVER_SOFTWARE'] = 'Development/'

import unittest

import webtest
from mock import patch


RESTORE_TABLE_URL = \
    '/restore/project/project-id/dataset/dataset_id/table/table_id'


class TestTableRestoreHandler(unittest.TestCase):

    def setUp(self):
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(table_restore_handler.app)

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_all_proper_parameters_provided_for_table_restoration(self,
                                                                  restore):
        # given & when
        self.under_test.get(
            RESTORE_TABLE_URL,
            params={'partitionId': '20170725',
                    'isRestoreToSourceProject': True,
                    'targetDatasetId': 'target_dataset_id',
                    'createDisposition': 'CREATE_IF_NEEDED',
                    'writeDisposition': 'WRITE_EMPTY',
                    'restorationDate': '2017-07-25'}
        )

        # then
        restore.assert_called_once_with(
            TableReference('project-id', 'dataset_id', 'table_id', '20170725'),
            None,
            "target_dataset_id",
            "CREATE_IF_NEEDED",
            "WRITE_EMPTY",
            datetime(2017, 07, 25, 23, 59, 59)
        )

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_default_parameters_for_table_restoration(self, restore):
        # given & when
        self.under_test.get(RESTORE_TABLE_URL + '?')

        # then
        expected_table_reference = \
            TableReference('project-id', 'dataset_id', 'table_id')
        restore.assert_called_once_with(expected_table_reference,
                                        '', None, None, None, None)

    @patch.object(TableRestoreService, 'restore', return_value={})
    def test_should_fail_on_wrong_date_format(self, _):
        # given & when
        response = self.under_test.get(
            RESTORE_TABLE_URL,
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
        response = self.under_test.get(
            RESTORE_TABLE_URL,
            params={'restorationDate': '2017-07-25'},
            expect_errors=True
        )

        # then
        expected_error = '{"status": "failed", "message": "Error message", ' \
                         '"httpStatus": 404}'
        self.assertEquals(404, response.status_int)
        self.assertEquals(response.body, expected_error)
