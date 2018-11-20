import unittest
import uuid

from google.appengine.ext import testbed, ndb
from mock import patch, PropertyMock

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.commons.config.configuration import Configuration
from src.commons.exceptions import ParameterValidationException
from src.commons.table_reference import TableReference
from src.restore.async_batch_restore_service import AsyncBatchRestoreService
from src.restore.datastore.restore_item import RestoreItem
from src.restore.list.backup_list_restore_service import \
    BackupItem, BackupListRestoreRequest, BackupListRestoreService


class TestBackupListRestoreService(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        patch('src.restore.async_batch_restore_service.BigQuery').start()
        patch.object(Configuration, 'backup_project_id', return_value='source_project_id', new_callable=PropertyMock).start()
        ndb.get_context().clear_cache()

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    @patch.object(uuid, 'uuid4', return_value=123)
    @patch.object(AsyncBatchRestoreService, 'restore')
    def test_that_restore_service_will_receive_suitable_request(
            self, mocked_restore_service, _):
        # given
        source_entity = self.__create_table_entity("source_project_id",
                                                   "source_dataset_id",
                                                   "source_table_id",
                                                   "source_partition_id")
        source_entity.put()

        backup_entity = self.__create_backup_entity(source_entity,
                                                    "backup_dataset_id",
                                                    "backup_table_id")
        backup_key = backup_entity.put()

        output_parameters = "{\"test_param_key\": \"test_value\"}"
        backup_item = BackupItem(backup_key, output_parameters)

        request = BackupListRestoreRequest(
            backup_items=[backup_item],
            target_project_id="target_project_id",
            target_dataset_id="target_dataset_id",
            write_disposition="write_disposition",
            create_disposition="create_disposition"
        )

        expected_source_table_reference = TableReference(
            Configuration.backup_project_id,
            "backup_dataset_id",
            "backup_table_id",
            "source_partition_id"
        )
        expected_target_table_reference = TableReference(
            "target_project_id",
            "target_dataset_id",
            "source_table_id",
            "source_partition_id"
        )
        expected_restore_item = RestoreItem.create(
            expected_source_table_reference,
            expected_target_table_reference,
            output_parameters
        )

        # when
        restoration_job_id = BackupListRestoreService().restore(request)

        # then
        mocked_restore_service.assert_called_once_with(
            ndb.Key('RestorationJob', restoration_job_id),
            [[expected_restore_item]]
        )

    @patch.object(uuid, 'uuid4', return_value=1234)
    @patch.object(AsyncBatchRestoreService, 'restore',
                  return_value="restorationId")
    def test_that_restore_service_will_generate_default_values_if_missing(
            self, mocked_restore_service, _):
        source_entity = self.__create_table_entity("source_project_id",
                                                   "source_dataset_id",
                                                   "source_table_id",
                                                   "source_partition_id")
        source_entity.put()

        backup_entity = self.__create_backup_entity(source_entity,
                                                    "backup_dataset_id",
                                                    "backup_table_id")
        backup_key = backup_entity.put()
        request = BackupListRestoreRequest(
            backup_items=iter([BackupItem(backup_key)]),
            target_project_id=None,
            target_dataset_id=None,
            write_disposition=None,
            create_disposition=None
        )

        expected_source_table_reference = TableReference(
            Configuration.backup_project_id,
            "backup_dataset_id",
            "backup_table_id",
            "source_partition_id"
        )
        expected_target_table_reference = TableReference(
            "source_project_id",
            "source_dataset_id",
            "source_table_id",
            "source_partition_id"
        )
        expected_restore_item = RestoreItem.create(
            expected_source_table_reference, expected_target_table_reference
        )

        # when
        restoration_job_id = BackupListRestoreService().restore(request)

        # then
        expected_key = ndb.Key('RestorationJob', restoration_job_id)

        mocked_restore_service.assert_called_once()
        mocked_restore_service.assert_called_with(expected_key,
                                                  [[expected_restore_item]])

    def test_that_restore_will_fail_if_backup_not_exists_in_ds(self):
        # given
        url_safe_key = "ahFlfmRldi1wcm9qZWN0LWJicXIlCxIFVGFibG" \
                       "UYgICAgJTOzAgMCxIGQmFja3VwGICAgICAgIAKDA"
        backup_key = ndb.Key(urlsafe=url_safe_key)

        request = BackupListRestoreRequest(
            backup_items=iter([BackupItem(backup_key)]),
            target_project_id=None,
            target_dataset_id=None,
            write_disposition=None,
            create_disposition=None
        )

        error_message = "Couldn\'t obtain backup entity in datastore. Error:"

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupListRestoreService().restore(request)

        # then
        self.assertTrue(error_message in str(context.exception))

    def test_that_restore_will_fail_if_backup_source_table_not_exists_in_ds(
            self):
        # given
        source_entity = self.__create_table_entity("source_project_id",
                                                   "source_dataset_id",
                                                   "source_table_id",
                                                   "source_partition_id")
        source_entity.put()
        backup_entity = self.__create_backup_entity(source_entity,
                                                    "backup_dataset_id",
                                                    "backup_table_id")
        backup_key = backup_entity.put()
        source_entity.key.delete()

        request = BackupListRestoreRequest(
            backup_items=iter([BackupItem(backup_key)]),
            target_project_id=None,
            target_dataset_id=None,
            write_disposition=None,
            create_disposition=None
        )

        error_message = "Backup ancestor doesn't exists: '{}:{}'. " \
            .format(backup_entity.dataset_id, backup_entity.table_id)

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupListRestoreService().restore(request)

        # then
        self.assertEquals(error_message, str(context.exception))

    @staticmethod
    def __create_table_entity(project_id, dataset_id, table_id, partition_id):
        return Table(project_id=project_id,
                     dataset_id=dataset_id,
                     table_id=table_id,
                     partition_id=partition_id)

    @staticmethod
    def __create_backup_entity(source_table_entity, dataset_id, table_id):
        return Backup(dataset_id=dataset_id,
                      table_id=table_id,
                      parent=source_table_entity.key)

    @staticmethod
    def __get_mocked_service_argument(mocked_service):
        return mocked_service.call_args[0][0]
