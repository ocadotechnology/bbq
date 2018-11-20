from freezegun import freeze_time
from google.appengine.ext import testbed, ndb
from mock import patch, PropertyMock, ANY
from unittest import TestCase

from src.commons.config.configuration import Configuration
from src.restore.dataset.dataset_restore_service import _DatasetRestoreService, \
  DatasetRestoreService

CREATE_DISPOSITION = "CREATE_IF_NEEDED"
WRITE_DISPOSITION = "WRITE_EMPTY"

RESTORATION_JOB_ID = 'restoration_job_id'

BACKUP_PROJECT_ID = 'backup_project_id'
RESTORATION_PROJECT_ID = 'restoration_project_id'

PROJECT_TO_RESTORE = 'project-x'
DATASET_TO_RESTORE = 'dataset_y'


class TestDatasetRestoreService(TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)
        ndb.get_context().clear_cache()

        patch.object(Configuration, 'backup_project_id',
                     return_value=BACKUP_PROJECT_ID,
                     new_callable=PropertyMock).start()

        self.restore_service = patch(
            'src.restore.dataset'
            '.dataset_restore_service.AsyncBatchRestoreService').start()
        self.restore_service.return_value = self.restore_service

        self.location_validator = patch(
            'src.restore.dataset.dataset_restore_service.DatasetRestoreParametersValidator').start()
        self.location_validator.return_value = self.location_validator

        self.restore_items_generator = patch(
            'src.restore.dataset.dataset_restore_service.DatasetRestoreItemsGenerator').start()
        self.restore_items_generator.return_value = self.restore_items_generator

        self.freezer = freeze_time("2017-12-06")
        self.freezer.start()

    def tearDown(self):
        patch.stopall()
        self.freezer.stop()
        self.testbed.deactivate()

    def test_should_call_dataset_restore_parameters_validator(self):
        # when
        DatasetRestoreService().restore(
            restoration_job_id=RESTORATION_JOB_ID,
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            target_project_id=RESTORATION_PROJECT_ID,
            target_dataset_id=None,
            create_disposition=CREATE_DISPOSITION,
            write_disposition=WRITE_DISPOSITION,
            max_partition_days=None)

        # then
        self.location_validator.validate_parameters.assert_called_once_with(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            target_project_id=RESTORATION_PROJECT_ID,
            target_dataset_id='dataset_y',
            max_partition_days=None)

    def test_dataset_restore_service_should_create_defered_task(self):
        # when
        DatasetRestoreService().restore(
            restoration_job_id=RESTORATION_JOB_ID,
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            target_project_id=RESTORATION_PROJECT_ID,
            target_dataset_id=None,
            create_disposition=CREATE_DISPOSITION,
            write_disposition=WRITE_DISPOSITION,
            max_partition_days=None)

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(1, len(tasks))
        self.assertIn("_DatasetRestoreService", tasks[0].payload)

    def test_restore_items_generator_should_be_called_and_result_passed(self):
        # given
        self.restore_items_generator \
            .generate_restore_items \
            .return_value = "<GENERATED ITEMS>"
        # when
        _DatasetRestoreService().restore(restoration_job_id=RESTORATION_JOB_ID,
                                         project_id=PROJECT_TO_RESTORE,
                                         dataset_id=DATASET_TO_RESTORE,
                                         target_project_id=RESTORATION_PROJECT_ID,
                                         target_dataset_id=None,
                                         create_disposition=CREATE_DISPOSITION,
                                         write_disposition=WRITE_DISPOSITION,
                                         max_partition_days=None)

        # then
        self.restore_items_generator \
            .generate_restore_items \
            .assert_called_once_with(
                project_id=PROJECT_TO_RESTORE,
                dataset_id=DATASET_TO_RESTORE,
                target_project_id=RESTORATION_PROJECT_ID,
                target_dataset_id=None,
                max_partition_days=None)
        self.restore_service.restore.assert_called_once_with(
            ndb.Key('RestorationJob', RESTORATION_JOB_ID),
            "<GENERATED ITEMS>")

    def test_should_not_call_async_service_twice_if_restoration_job_exist(
            self):
        # when
        _DatasetRestoreService().restore(restoration_job_id=RESTORATION_JOB_ID,
                                         project_id=PROJECT_TO_RESTORE,
                                         dataset_id=DATASET_TO_RESTORE,
                                         target_project_id=RESTORATION_PROJECT_ID,
                                         target_dataset_id=None,
                                         create_disposition=CREATE_DISPOSITION,
                                         write_disposition=WRITE_DISPOSITION,
                                         max_partition_days=None)
        _DatasetRestoreService().restore(restoration_job_id=RESTORATION_JOB_ID,
                                         project_id=PROJECT_TO_RESTORE,
                                         dataset_id=DATASET_TO_RESTORE,
                                         target_project_id=RESTORATION_PROJECT_ID,
                                         target_dataset_id=None,
                                         create_disposition=CREATE_DISPOSITION,
                                         write_disposition=WRITE_DISPOSITION,
                                         max_partition_days=None)
        # then
        self.restore_service.restore.assert_called_once_with(
            ndb.Key('RestorationJob', RESTORATION_JOB_ID),
            ANY)
