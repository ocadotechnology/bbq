import logging

from google.appengine.ext import deferred

from src.restore.async_batch_restore_service import AsyncBatchRestoreService
from src.restore.dataset.dataset_restore_items_generator import \
    DatasetRestoreItemsGenerator
from src.restore.dataset.dataset_restore_parameters_validator import \
    DatasetRestoreParametersValidator
from src.restore.datastore.restoration_job import RestorationJob, \
    DuplicatedRestorationJobIdException
from src.restore.restore_workspace_creator import RestoreWorkspaceCreator


class _DatasetRestoreService(object):

    def restore(self, restoration_job_id, project_id, dataset_id,
                target_dataset_id, max_partition_days):

        logging.info(
            "Executing restoration job '%s' of dataset '%s:%s' "
            "(target_dataset_id: '%s', max_partition_days:'%s')",
            restoration_job_id, project_id, dataset_id, target_dataset_id,
            max_partition_days)

        try:
            restoration_job_key = RestorationJob.create(
                restoration_job_id,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY")
        except DuplicatedRestorationJobIdException:
            logging.warning(
                "Trying to create RestorationJob with already existed restoration job id")
            return

        restore_items = DatasetRestoreItemsGenerator.generate_restore_items(
            project_id=project_id,
            dataset_id=dataset_id,
            target_dataset_id=target_dataset_id,
            max_partition_days=max_partition_days)

        AsyncBatchRestoreService().restore(restoration_job_key, restore_items)


class DatasetRestoreService(_DatasetRestoreService):

    def restore(self, restoration_job_id, project_id, dataset_id,
                target_dataset_id, max_partition_days):
        if target_dataset_id is None:
            target_dataset_id = RestoreWorkspaceCreator \
                .create_default_target_dataset_id(project_id=project_id,
                                                  dataset_id=dataset_id)

        logging.info("Checking dataset restore service parameters")
        DatasetRestoreParametersValidator().validate_parameters(
            project_id=project_id,
            dataset_id=dataset_id,
            target_dataset_id=target_dataset_id,
            max_partition_days=max_partition_days)

        logging.info("Executing deferred task DatasetRestoreService().restore")
        deferred.defer(
            _DatasetRestoreService().restore,
            restoration_job_id,
            project_id,
            dataset_id,
            target_dataset_id,
            max_partition_days,
        )
