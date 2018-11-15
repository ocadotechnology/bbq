import unittest
from datetime import datetime
from freezegun import freeze_time

from google.appengine.ext import ndb, testbed

from src.restore.datastore.restoration_job import RestorationJob

from src.restore.datastore.restore_item import RestoreItem, TableReferenceEntity
from src.commons.table_reference import TableReference


class TestRestoreItem(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    def test_restore_item_default_state_is_in_progress(self):
        # given
        source_table = TableReference(project_id='source_project_id',
                                      dataset_id='source_dataset_id',
                                      table_id='source_table_id')
        target_table = TableReference(project_id='target_project_id',
                                      dataset_id='target_dataset_id',
                                      table_id='target_table_id')

        # when
        result = RestoreItem.create(source_table, target_table)

        # then
        self.assertEqual(RestoreItem.STATUS_IN_PROGRESS, result.status)

    def test_should_store_restore_item_with_key_to_restoration_job(self):
        restoration_job_key, _, restore_item = \
            self.__create_restoration_job_with_one_item("111")

        item_entity = restore_item.key.get()
        self.assertEqual(RestoreItem.STATUS_IN_PROGRESS, item_entity.status)
        self.assertEqual(restoration_job_key, item_entity.restoration_job_key)
        self.assertEqual('{"external-id": "13-424"}',
                         item_entity.custom_parameters)

    def test_should_return_item_when_quering_model_by_source_project_id(self):
        # given
        self.__create_restoration_job_with_one_item("111")
        _, _, other_restore_item = self.__create_restoration_job_with_one_item(
            "222")
        other_restore_item.source_table.project_id = 'other_project'
        other_restore_item.put()
        # when
        items = RestoreItem.query() \
            .filter(RestoreItem.source_table.project_id == 'project-abc')

        # then
        self.assertEquals(1, items.count())

    def test_should_return_items_by_restoration_job_key(self):
        # given
        restoration_job_key1 = self.__create_restoration_job_with_two_items(
            "222")
        restoration_job_key2 = self.__create_restoration_job_with_one_item(
            "111")

        # when
        items = RestoreItem.get_restoration_items(restoration_job_key1)

        # then
        self.assertEquals(2, len(list(items)))

    def test_should_return_item_by_url_safe_key(self):
        # given
        _, restore_item_key, restore_item = \
            self.__create_restoration_job_with_one_item("111")

        # when
        returned_item = RestoreItem.get_by_key(restore_item_key)

        # then
        self.assertEquals(returned_item, restore_item)

    def test_should_not_update_item_with_success_twice(self):
        # given
        _, restore_item_key, restore_item = \
            self.__create_restoration_job_with_one_item("111")

        # when
        with freeze_time("2012-01-14") as frozen_datetime:
            RestoreItem.update_with_done(restore_item_key)

            frozen_datetime.move_to("2012-01-15")
            RestoreItem.update_with_done(restore_item_key)

        # then
        updated_restore_item = RestoreItem.get_by_key(restore_item_key)
        self.assertEqual(updated_restore_item.completed, datetime(2012, 1, 14))
        self.assertEqual(updated_restore_item.status_message, None)

    def test_should_update_restore_item_with_failed_status(self):
        # given
        _, restore_item_key, restore_item = \
            self.__create_restoration_job_with_one_item("111")

        error_message = "Cannot read a table without a schema"

        # when
        RestoreItem.update_with_failed(restore_item_key, error_message)

        # then
        updated_restore_item = RestoreItem.get_by_key(restore_item_key)
        self.assertEqual(updated_restore_item.status, RestoreItem.STATUS_FAILED)
        self.assertEqual(updated_restore_item.status_message, error_message)

    def test_should_not_update_item_with_failed_twice(self):
        # given
        restoration_job_key, restore_item_key, restore_item = \
            self.__create_restoration_job_with_one_item("111")

        error_message = "Cannot read a table without a schema"

        # when
        with freeze_time("2012-01-14") as frozen_datetime:
            RestoreItem.update_with_failed(restore_item_key, error_message)

            frozen_datetime.move_to("2012-01-15")
            RestoreItem.update_with_failed(restore_item_key, error_message)

        # then
        updated_restore_item = RestoreItem.get_by_key(restore_item_key)
        self.assertEqual(updated_restore_item.completed, datetime(2012, 1, 14))
        self.assertEqual(updated_restore_item.status_message, error_message)

    @staticmethod
    def __create_restoration_job_with_one_item(restoration_job_id):
        restoration_job_key = RestorationJob.create(
            restoration_job_id=restoration_job_id,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY")
        restoration_job_key.get().increment_count_by(1)

        restore_item = TestRestoreItem.__create_restore_item_example(
            restoration_job_key)
        restore_item_key = restore_item.put()
        return restoration_job_key, restore_item_key, restore_item

    @staticmethod
    def __create_restoration_job_with_two_items(restoration_job):
        restoration_job_key, _, _ = TestRestoreItem. \
            __create_restoration_job_with_one_item(restoration_job)
        TestRestoreItem.__create_restore_item_example(restoration_job_key).put()
        return restoration_job_key

    @staticmethod
    def __create_restore_item_example(restoration_job_key):
        return RestoreItem(restoration_job_key=restoration_job_key,
                           status=RestoreItem.STATUS_IN_PROGRESS,
                           completed=None,
                           source_table=TableReferenceEntity(
                               project_id='project-abc',
                               dataset_id='dataset_xyz',
                               table_id='23423_table-43-logs_213213',
                               partition_id='20171113'),
                           target_table=TableReferenceEntity(
                               project_id='target-project',
                               dataset_id='dataset_original',
                               table_id='table-43-logs',
                               partition_id='20171113'),
                           custom_parameters='{"external-id": "13-424"}'
                           )
