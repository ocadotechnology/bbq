import logging
import uuid

from src.restore.list.backup_list_restore_service import \
    BackupListRestoreRequest, BackupItem, BackupListRestoreService
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService
from src.backup.datastore.backup_finder import BackupFinder


class TableRestoreService(object):
    @classmethod
    def restore(cls, table_reference, target_dataset_id, restoration_datetime):
        backup = BackupFinder.for_table(table_reference, restoration_datetime)

        restore_request = BackupListRestoreRequest([BackupItem(backup.key)],
                                                   target_dataset_id)

        restoration_job_id = str(uuid.uuid4())
        logging.info("Created restoration_job_id: %s", restoration_job_id)

        BackupListRestoreService().restore(restoration_job_id, restore_request)

        restore_data = {
            'restorationJobId': restoration_job_id,
            'restorationStatusEndpoint':
                RestorationJobStatusService.get_status_endpoint(
                    restoration_job_id),
            'restorationWarningsOnlyStatusEndpoint':
                RestorationJobStatusService.get_warnings_only_status_endpoint(
                    restoration_job_id)
        }
        return restore_data
