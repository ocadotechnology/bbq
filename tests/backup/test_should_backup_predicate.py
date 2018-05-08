import unittest

from datetime import datetime
from google.appengine.ext import ndb
from google.appengine.ext import testbed

from mock import patch
from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.backup.should_backup_predicate import ShouldBackupPredicate
from src.big_query.big_query_table_metadata import BigQueryTableMetadata


class TestShouldBackupPredicate(unittest.TestCase):

    def setUp(self):
        self.initTestBedForDatastore()

        self.table = Table(
            project_id="p1",
            dataset_id="d1",
            table_id="t1"
        )

        self.big_query_table_metadata = BigQueryTableMetadata({})
        patch('src.big_query.big_query_table_metadata.BigQueryTableMetadata.is_empty',
              return_value=False).start()
        patch('src.big_query.big_query_table_metadata.BigQueryTableMetadata.'
              'is_external_or_view_type', return_value=False).start()

    def initTestBedForDatastore(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(BigQueryTableMetadata, 'is_empty', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime',
                  return_value=datetime(2016, 11, 13, 15, 00))
    def test_should_return_true_if_table_is_empty(self, _, _1):
        # given
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)
        # when
        result = predicate.test(self.table)
        # then
        self.assertTrue(result, "ShouldBackupPredicate should return TRUE "
                                "if table is empty")

    @patch.object(BigQueryTableMetadata, 'table_exists',
                  return_value=False)
    def test_should_return_false_if_table_not_exist_anymore(self, _):
        # given
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)
        # when
        result = predicate.test(self.table)
        # then
        self.assertFalse(result, "ShouldBackupPredicate should return FALSE "
                                 "if table not exists")

    @patch.object(BigQueryTableMetadata, 'is_external_or_view_type',
                  return_value=True)
    def test_should_return_false_if_table_is_external_or_view_type(self, _):
        # given
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)
        # when
        result = predicate.test(self.table)
        # then
        self.assertFalse(result, "ShouldBackupPredicate should return FALSE "
                                 "if object is table or external type")

    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime',
                  return_value=datetime(2016, 11, 13, 15, 00))
    def test_should_return_true_if_there_is_no_backups(self, _):
        # given
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)
        # when
        result = predicate.test(self.table)
        # then
        self.assertTrue(result, "ShouldBackupPredicate should return TRUE "
                                "if there is no backups")

    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime',
                  return_value=datetime(2016, 11, 13, 16, 00))
    def test_should_return_true_if_table_was_changed_after_last_backup(self, _):
        # given
        backup = Backup(
            parent=self.table.key,
            last_modified=datetime(2016, 11, 13, 15, 00)
        )
        backup.put()
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)

        # when
        result = predicate.test(self.table)
        # then
        self.assertTrue(result, "ShouldBackupPredicate should return TRUE "
                                "if table was changed after last backup")

    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime',
                  return_value=datetime(2016, 11, 13, 15, 00))
    def test_should_return_false_if_table_was_changed_at_the_same_time_when_last_backup(self, _):    # nopep8 pylint: disable=C0301
        # given
        backup = Backup(
            parent=self.table.key,
            last_modified=datetime(2016, 11, 13, 15, 00)
        )
        backup.put()
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)

        # when
        result = predicate.test(self.table)
        # then
        self.assertFalse(result, "ShouldBackupPredicate should return False "
                                 "if table was change at the same time when "
                                 "last backup was made")

    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime',
                  return_value=datetime(2016, 11, 13, 14, 00))
    def test_should_return_false_if_table_was_changed_before_last_backup(self, _):  # nopep8 pylint: disable=C0301
        # given
        backup = Backup(
            parent=self.table.key,
            last_modified=datetime(2016, 11, 13, 15, 00)
        )
        backup.put()
        predicate = ShouldBackupPredicate(self.big_query_table_metadata)

        # when
        result = predicate.test(self.table)
        # then
        self.assertFalse(result, "ShouldBackupPredicate should return FALSE "
                                 "if table was changed before "
                                 "last backup was made")
