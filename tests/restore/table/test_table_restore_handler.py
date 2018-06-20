import os

from datetime import datetime

from src.restore.table import table_restore_handler

os.environ['SERVER_SOFTWARE'] = 'Development/'

import unittest

import webtest
from mock import patch


class TestTableRestoreHandler(unittest.TestCase):

    def setUp(self):
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(table_restore_handler.app)

    @patch.object(RestoreService, 'try_to_restore')
    def test_all_proper_parameters_provided_for_table_restoration(self, try_to_restore):
        try_to_restore.return_value = {'size_in_bytes': 120, 'rows_count': 12}

        self.under_test.get(
            '/restore/table/project-id/new_dataset/ttt_sss',
            params={'target_dataset_id': 'smoke_test_US',
                    'restoration_date': '2017-07-25'})

        try_to_restore.assert_called_once_with(
            Restoration(source_project_id='project-id',
                        source_dataset_id='new_dataset', source_table_id='ttt_sss',
                        target_dataset_id='smoke_test_US', restoration_date='2017-07-25',
                        source_partition_id=None))

    @patch.object(RestoreService, 'try_to_restore')
    def test_default_parameters_for_table_restoration(self, try_to_restore):
        try_to_restore.return_value = {'size_in_bytes': 120, 'rows_count': 12}

        self.under_test.get('/restore/table/project-id/new_dataset/ttt_sss')

        try_to_restore.assert_called_once_with(
            Restoration(source_project_id='project-id',
                        source_dataset_id='new_dataset', source_table_id='ttt_sss',
                        target_dataset_id='project_id___new_dataset',
                        restoration_date=datetime.utcnow().strftime('%Y-%m-%d'),
                        source_partition_id=None))

    @patch.object(RestoreService, 'try_to_restore')
    def test_should_fail_on_wrong_date_format(self, try_to_restore):
        try_to_restore.return_value = {'size_in_bytes': 120, 'rows_count': 12}
        expected_error = '{"status": "failed", "message": "Wrong request ' \
                         'params provided:Wrong date value format for ' \
                         'parameter \'restoration_date\'. Should be ' \
                         '\'YYYY-mm-dd\'", "httpStatus": 400}'

        response = self.under_test.get(
            '/restore/table/project-id/new_dataset/ttt_sss',
            params={'restoration_date': '20170725'}, expect_errors=True)

        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @patch.object(RestoreService, 'try_to_restore')
    def test_all_proper_parameters_provided_for_partition_restoration(self, try_to_restore):
        try_to_restore.return_value = {'size_in_bytes': 70, 'rows_count': 4}

        self.under_test.get(
            '/restore/table/project-id/new_dataset/ttt_sss',
            params={
                'target_dataset_id': 'smoke_test_US',
                'restoration_date': '2017-07-25',
                'partition_id': '20170726'
            })

        try_to_restore.assert_called_once_with(
            Restoration(source_project_id='project-id',
                        source_dataset_id='new_dataset', source_table_id='ttt_sss',
                        target_dataset_id='smoke_test_US', restoration_date='2017-07-25',
                        source_partition_id='20170726'))

    @patch.object(RestoreService, 'try_to_restore',
                  side_effect=BackupNotFoundException())
    def test_should_forward_backup_not_found_error(self, try_to_restore):
        expected_error = '{"status": "failed", "message": "Backup for table ' \
                         'project-id:new_dataset.ttt_sss made  does ' \
                         'not exist.", "httpStatus": 404}'

        response = self.under_test.get(
            '/restore/table/project-id/new_dataset/ttt_sss',
            expect_errors=True)

        self.assertEquals(404, response.status_int)
        self.assertEquals(response.body, expected_error)

