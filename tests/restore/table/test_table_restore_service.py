import unittest
import uuid

from mock import Mock, MagicMock, patch

from src.restore.list.backup_list_restore_service import \
    BackupListRestoreService, BackupListRestoreRequest, BackupItem
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService
from src.restore.table.backup_finder import BackupFinder
from src.restore.table.table_restore_service import TableRestoreService


class TestTableRestoreService(unittest.TestCase):

    @patch.object(BackupListRestoreService, 'restore')
    @patch.object(BackupFinder, 'for_table')
    @patch.object(uuid, 'uuid4', return_value=123)
    def test_happy_path(self, _, for_table, restore):
        # given

        table_reference = Mock()
        target_dataset_id = "targetDatasetId"
        restoration_datetime = Mock()
        backup = Mock()
        backup.key = MagicMock()

        for_table.return_value = backup

        # when
        restore_data = TableRestoreService.restore(
            table_reference, target_dataset_id, restoration_datetime)

        # then
        expected_restoration_job_id = "123"
        expected_restore_request = BackupListRestoreRequest(
            [BackupItem(backup.key)], target_dataset_id)
        expected_endpoint = RestorationJobStatusService\
            .get_status_endpoint(expected_restoration_job_id)
        expected_warnings_only_endpoint = RestorationJobStatusService \
            .get_warnings_only_status_endpoint(expected_restoration_job_id)

        restore.assert_called_once_with(
            expected_restoration_job_id, expected_restore_request)

        self.assertEquals(expected_restoration_job_id,
                          restore_data['restorationJobId'])
        self.assertEquals(expected_endpoint,
                          restore_data['restorationStatusEndpoint'])
        self.assertEquals(expected_warnings_only_endpoint,
                          restore_data['restorationWarningsOnlyStatusEndpoint'])
