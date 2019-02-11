import logging
import uuid

import webapp2

from src.commons.config.configuration import configuration
from src.commons.handlers import validators
from src.commons.handlers.json_handler import JsonHandler
from src.restore.dataset.dataset_restore_service import \
    DatasetRestoreService
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class DatasetRestoreHandler(JsonHandler):

    def post(self, project_id, dataset_id):
        is_restore_to_source_project = self.request.get('isRestoreToSourceProject', None)
        target_dataset_id = self.request.get('targetDatasetId', None)
        create_disposition = self.request.get('createDisposition', None)
        write_disposition = self.request.get('writeDisposition', None)
        max_partition_days = self.__get_max_partition_days()

        target_project_id = project_id if is_restore_to_source_project \
            else configuration.default_restoration_project_id

        validators.validate_restore_request_params(
            source_project_id=project_id,
            source_dataset_id=dataset_id,
            target_project_id=target_project_id,
            target_dataset_id=target_dataset_id,
            create_disposition=create_disposition,
            write_disposition=write_disposition
        )

        restoration_job_id = str(uuid.uuid4())
        logging.info("Created restoration_job_id: %s", restoration_job_id)

        DatasetRestoreService().restore(
            restoration_job_id=restoration_job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            target_project_id=target_project_id,
            target_dataset_id=target_dataset_id,
            create_disposition=create_disposition,
            write_disposition=write_disposition,
            max_partition_days=max_partition_days
        )

        restore_data = {
            'restorationJobId': restoration_job_id,
            'projectId': project_id,
            'datasetId': dataset_id,
            'restorationStatusEndpoint': RestorationJobStatusService.get_status_endpoint(restoration_job_id),
            'restorationWarningsOnlyStatusEndpoint': RestorationJobStatusService.get_warnings_only_status_endpoint(restoration_job_id)
        }
        self._finish_with_success(restore_data)

    def __get_max_partition_days(self):
        max_partition_days = self.request.get('maxPartitionDays', None)
        return int(max_partition_days) if max_partition_days else None


app = webapp2.WSGIApplication([
    webapp2.Route(
        '/restore/project/<project_id:.*>/dataset/<dataset_id:.*>',
        DatasetRestoreHandler)
], debug=configuration.debug_mode)
