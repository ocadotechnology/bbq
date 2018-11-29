import logging

from src.backup.datastore.backup_finder import BackupFinder
from src.restore.list.backup_list_restore_service import \
    BackupListRestoreRequest, BackupItem, BackupListRestoreService
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class TableRestoreService(object):

    @classmethod
    def restore(cls, table_reference, target_project_id, target_dataset_id,
                create_disposition, write_disposition, restoration_datetime):

        backup = BackupFinder.for_table(table_reference, restoration_datetime)

        restore_request = BackupListRestoreRequest([BackupItem(backup.key)],
                                                   target_project_id,
                                                   target_dataset_id,
                                                   create_disposition,
                                                   write_disposition)

        restoration_job_id = BackupListRestoreService().restore(restore_request)
        logging.info("Scheduled restoration job: %s", restoration_job_id)

        return {
            'restorationJobId': restoration_job_id,
            'restorationStatusEndpoint': RestorationJobStatusService.get_status_endpoint(restoration_job_id),
            'restorationWarningsOnlyStatusEndpoint': RestorationJobStatusService.get_warnings_only_status_endpoint(restoration_job_id)
        }
