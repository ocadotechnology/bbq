import uuid

from apiclient.errors import HttpError
from google.appengine.ext import testbed, ndb
from mock import patch, call, mock
from unittest import TestCase

from src.commons.big_query.copy_job_async.post_copy_action_request \
  import PostCopyActionRequest
from src.commons.table_reference import TableReference
from src.restore.async_batch_restore_service import AsyncBatchRestoreService
from src.restore.datastore.restoration_job import RestorationJob
from src.restore.datastore.restore_item import RestoreItem
from src.restore.restore_workspace_creator import RestoreWorkspaceCreator

HARDCODED_UUID = '123'


class TestAsyncRestoreService(TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()
        ndb.get_context().clear_cache()
        self.copy_service = \
            patch(
                'src.restore.async_batch_restore_service.CopyJobServiceAsync').start()
        patch('src.restore.async_batch_restore_service.BigQuery').start()
        patch.object(uuid, 'uuid4', return_value=HARDCODED_UUID).start()
        self.enforcer = patch.object(RestoreWorkspaceCreator,
                                     'create_workspace').start()

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    def test_for_single_item_should_create_post_copy_action(self):
        # given
        restoration_job_key = RestorationJob.create(
            HARDCODED_UUID,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY")
        restore_items_tuple = self.__create_restore_items(count=1)
        restore_item = restore_items_tuple[0][0]
        source_bq = restore_items_tuple[0][1].create_big_query_table()
        target_bq = restore_items_tuple[0][2].create_big_query_table()

        # when
        AsyncBatchRestoreService().restore(restoration_job_key, [[restore_item]])

        # then
        self.copy_service.assert_has_calls(
            [
                call(copy_job_type_id='restore', task_name_suffix='123'),
                call().with_post_action(
                    PostCopyActionRequest(
                        url='/callback/restore-finished/',
                        data={
                            'restoreItemKey': (
                                restore_item.key.urlsafe())
                        })),
                call().with_post_action().with_create_disposition('CREATE_IF_NEEDED'),
                call().with_post_action().with_create_disposition().with_write_disposition('WRITE_EMPTY'),
                call().with_post_action().with_create_disposition().with_write_disposition().copy_table(source_bq, target_bq)
            ])

    @patch.object(RestoreWorkspaceCreator, 'create_workspace', side_effect=
        HttpError(mock.Mock(status=403), 'Forbidden'))
    def test_failing_creating_dataset_should_update_restore_item_status(self,
        _):
        # given
        restoration_job_key = RestorationJob.create(HARDCODED_UUID, create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_EMPTY")
        restore_items_tuple = self.__create_restore_items(count=1)
        restore_item = restore_items_tuple[0][0]

        # when
        AsyncBatchRestoreService().restore(restoration_job_key, [[restore_item]])

        # then
        restore_items = list(RestoreItem.query().filter(
            RestoreItem.restoration_job_key == restoration_job_key))

        self.assertEqual(restore_items[0].status,
                         RestoreItem.STATUS_FAILED)

    def test_multiple_items_restore(self):
        # given
        restoration_job_key = RestorationJob.create(
            HARDCODED_UUID,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY")
        restore_items_tuple = self.__create_restore_items(count=2)
        restore_item1 = restore_items_tuple[0][0]
        restore_item2 = restore_items_tuple[1][0]
        source_bq1 = restore_items_tuple[0][1].create_big_query_table()
        target_bq1 = restore_items_tuple[0][2].create_big_query_table()
        source_bq2 = restore_items_tuple[1][1].create_big_query_table()
        target_bq2 = restore_items_tuple[1][2].create_big_query_table()
        # when
        AsyncBatchRestoreService().restore(restoration_job_key,
                                           [[restore_items_tuple[0][0],
                                             restore_items_tuple[1][0]]])

        # then
        calls = [call(copy_job_type_id='restore', task_name_suffix='123'),
                 call().with_post_action(
                     PostCopyActionRequest(
                         url='/callback/restore-finished/',
                         data={
                             'restoreItemKey': (
                                 restore_item1.key.urlsafe())
                         })),
                 call().with_post_action().with_create_disposition('CREATE_IF_NEEDED'),
                 call().with_post_action().with_create_disposition().with_write_disposition('WRITE_EMPTY'),
                 call().with_post_action().with_create_disposition().with_write_disposition().copy_table(source_bq1, target_bq1),
                 call(copy_job_type_id='restore', task_name_suffix='123'),
                 call().with_post_action(
                     PostCopyActionRequest(
                         url='/callback/restore-finished/',
                         data={
                             'restoreItemKey': (
                                 restore_item2.key.urlsafe())
                         })),
                 call().with_post_action().with_create_disposition('CREATE_IF_NEEDED'),
                 call().with_post_action().with_create_disposition().with_write_disposition('WRITE_EMPTY'),
                 call().with_post_action().with_create_disposition().with_write_disposition().copy_table(source_bq2, target_bq2)
                 ]
        self.copy_service.assert_has_calls(calls)

    def test_that_enforcer_is_called_for_each_restore_item(self):
        # given
        restoration_job_key= RestorationJob.create(HARDCODED_UUID, create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_EMPTY")
        restore_items_tuple = self.__create_restore_items(count=2)

        # when
        AsyncBatchRestoreService().restore(restoration_job_key,
                                           [[restore_items_tuple[0][0],
                                             restore_items_tuple[1][0]]])

        # then
        expected_calls = [call(restore_items_tuple[0][1],
                               restore_items_tuple[0][2]),
                          call(restore_items_tuple[1][1],
                               restore_items_tuple[1][2])]

        self.enforcer.assert_has_calls(expected_calls)

    def test_that_proper_entities_were_stored_in_datastore(self):
        # given
        restoration_job_key = RestorationJob.create(
            HARDCODED_UUID,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY")
        restore_item_tuples = self.__create_restore_items(count=3)
        # when
        AsyncBatchRestoreService().restore(restoration_job_key,
                                           [[restore_item_tuples[0][0],
                                             restore_item_tuples[1][0]],
                                            [restore_item_tuples[2][0]]])

        # then
        restoration_job = RestorationJob.get_by_id(HARDCODED_UUID)
        self.assertEqual(restoration_job.items_count, 3)

        restore_items = list(RestoreItem.query().filter(
            RestoreItem.restoration_job_key == restoration_job.key))

        self.assertEqual(restore_items[0].status,
                         RestoreItem.STATUS_IN_PROGRESS)
        self.assertEqual(restore_items[0].completed, None)
        self.assertEqual(restore_items[0].source_table_reference,
                         restore_item_tuples[0][1])
        self.assertEqual(restore_items[0].target_table_reference,
                         restore_item_tuples[0][2])

        self.assertEqual(restore_items[1].status,
                         RestoreItem.STATUS_IN_PROGRESS)
        self.assertEqual(restore_items[1].completed, None)
        self.assertEqual(restore_items[1].source_table_reference,
                         restore_item_tuples[1][1])
        self.assertEqual(restore_items[1].target_table_reference,
                         restore_item_tuples[1][2])

    @staticmethod
    def __create_restore_items(count=1):
        result = []
        for i in range(0, count):
            source_table_reference = TableReference(
                "source_project_id_" + str(i),
                "source_dataset_id_" + str(i),
                "source_table_id_" + str(i),
                "source_partition_id_" + str(i))
            target_table_reference = TableReference(
                "target_project_id_" + str(i),
                "target_dataset_id_" + str(i),
                "target_table_id_" + str(i),
                "target_partition_id_" + str(i))
            restore_item = RestoreItem.create(source_table_reference,
                                              target_table_reference)
            result.append(
                (restore_item, source_table_reference, target_table_reference)
            )
        return result
