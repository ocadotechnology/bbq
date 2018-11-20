import unittest
import uuid

from mock import Mock, MagicMock, patch

from src.backup.datastore.backup_finder import BackupFinder
from src.restore.list.backup_list_restore_service import \
    BackupListRestoreService, BackupListRestoreRequest, BackupItem
from src.restore.table.table_restore_service import TableRestoreService


class TestTableRestoreService(unittest.TestCase):

    @patch.object(BackupListRestoreService, 'restore')
    @patch.object(BackupFinder, 'for_table')
    @patch.object(uuid, 'uuid4', return_value=123)
    def test_service_call_backup_list_restore_with_one_item_only(
            self, _, for_table, restore):
        # given
        table_reference = Mock()
        target_dataset_id = "targetDatasetId"
        target_project_id = "targetProjectId"
        create_disposition = "CREATE_IF_NEEDED"
        write_disposition = "WRITE_EMPTY"

        restoration_datetime = Mock()
        backup = Mock()
        backup.key = MagicMock()

        for_table.return_value = backup

        # when
        TableRestoreService.restore(table_reference,
                                    target_project_id,
                                    target_dataset_id,
                                    create_disposition,
                                    write_disposition,
                                    restoration_datetime)
        # then
        expected_restore_request = BackupListRestoreRequest(
            [BackupItem(backup.key)],
            target_project_id,
            target_dataset_id,
            create_disposition,
            write_disposition
        )
        restore.assert_called_once_with(expected_restore_request)
