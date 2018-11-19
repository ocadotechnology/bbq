import logging
import uuid

import webapp2

from src.commons.big_query import validators
from src.commons.config.configuration import configuration
from src.commons.handlers.bbq_authenticated_handler import \
    BbqAuthenticatedHandler
from src.commons.handlers.json_handler import JsonHandler
from src.restore.dataset.dataset_restore_service import \
    DatasetRestoreService
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class DatasetRestoreHandler(JsonHandler):

    def post(self, project_id, dataset_id):
        target_project_id = self.request.get('targetProjectId', None)
        target_dataset_id = self.request.get('targetDatasetId', None)
        create_disposition = self.request.get('createDisposition', None)
        write_disposition = self.request.get('writeDisposition', None)
        max_partition_days = self.__get_max_partition_days()

        validators.validate_dataset_restore_params(
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


class DatasetRestoreAuthenticatedHandler(DatasetRestoreHandler,
                                         BbqAuthenticatedHandler):

    def __init__(self, request=None, response=None):
        super(DatasetRestoreAuthenticatedHandler, self). \
            __init__(request, response)


app = webapp2.WSGIApplication([
    webapp2.Route(
        '/restore/project/<project_id:.*>/dataset/<dataset_id:.*>',
        DatasetRestoreHandler),
    webapp2.Route(
        '/schedule/restore/project/<project_id:.*>/dataset/<dataset_id:.*>',
        DatasetRestoreAuthenticatedHandler)
], debug=configuration.debug_mode)
