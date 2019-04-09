import unittest
from datetime import datetime

from apiclient.errors import HttpError
from freezegun import freeze_time
from google.appengine.ext import ndb, testbed
from mock import patch, Mock

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.table_reference import TableReference
from src.retention.policy.filter.grace_period_after_deletion_filter import \
    GracePeriodAfterDeletionFilter
from tests.utils.backup_utils import create_backup


class TestGracePeriodAfterDeletionFilter(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self.under_test = GracePeriodAfterDeletionFilter()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=False)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 7, 01)))
    @freeze_time("2017-08-20")
    def test_should_keep_old_backup_when_source_table_was_deleted_only_recently(self, _, _1, _2):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2015, 06, 01))
        # when
        backups_to_retain = self.under_test.filter(
            backups=[b1],
            table_reference=reference
        )
        # then
        self.assertListEqual([b1], backups_to_retain)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=False)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 7, 01)))
    @freeze_time("2017-08-20")
    def test_should_keep_young_backups_even_if_source_table_is_deleted(self, _, _1, _2):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2017, 06, 01))
        # when
        backups_to_retain = self.under_test.filter(
            backups=[b1],
            table_reference=reference
        )
        # then
        self.assertListEqual([b1], backups_to_retain)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=False)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2015, 7, 01)))
    @freeze_time("2017-08-20")
    def test_should_delete_old_backups_if_source_table_is_gone_for_long(self, _, _1, _2):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2015, 06, 01))
        # when
        backups_to_retain = self.under_test.filter(
            backups=[b1],
            table_reference=reference
        )
        # then
        self.assertFalse(backups_to_retain)

    is_not_parititoned_http_error_response = '''{
                             "error": {
                              "code": 400,
                              "message": "Cannot read partition information from a table that is not partitioned: sit-cymes-euw1-mlservices:MarketingRecommendations_Derived_Restricted.DeliveredOrder$2420180805",
                              "errors": [
                               {
                                "message": "Cannot read partition information from a table that is not partitioned: sit-cymes-euw1-mlservices:MarketingRecommendations_Derived_Restricted.DeliveredOrder$2420180805",
                                "domain": "global",
                                "reason": "invalid"
                               }
                              ],
                              "status": "INVALID_ARGUMENT"
                             }
                            } '''


    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  side_effect=HttpError(Mock(status=400), is_not_parititoned_http_error_response))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2015, 7, 01)))
    @freeze_time("2017-08-20")
    def test_should_delete_old_backups_if_source_partitioned_table_is_gone_for_long_and_new_table_with_the_same_name_is_not_partitioned(self, _, _1):  # nopep8 pylint: disable=C0301
        # given
        partitioned_reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id', 'example-partition-id')
        b1 = create_backup(datetime(2015, 06, 01))
        # when
        backups_to_retain = self.under_test.filter(
            backups=[b1],
            table_reference=partitioned_reference
        )
        # then
        self.assertFalse(backups_to_retain)


    @patch.object(BigQueryTableMetadata, 'get_table_by_reference',
                  side_effect=HttpError(Mock(status=400), is_not_parititoned_http_error_response))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2015, 7, 01)))
    @freeze_time("2017-08-20")
    def test_should_raise_exception_if_source_non_partitioned_table_is_gone_for_long_and_new_table_with_the_same_name_is_not_partitioned(self, _, _1):  # nopep8 pylint: disable=C0301
        # given
        non_partitioned_reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2015, 06, 01))

        # when &then
        self.assertRaises(HttpError, self.under_test.filter, [b1],
                          non_partitioned_reference)


    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2015, 7, 01)))
    @freeze_time("2017-08-20")
    def test_should_retain_backups_if_source_table_still_exists(self, _, _1, _2):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2015, 06, 01))
        # when
        backups_to_retain = self.under_test.filter(
            backups=[b1],
            table_reference=reference
        )
        # then
        self.assertListEqual([b1], backups_to_retain)

    def test_should_gracefully_deal_with_empty_backup_list(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        # when
        backups_to_retain = self.under_test.filter(
            backups=[],
            table_reference=reference
        )
        # then
        self.assertFalse(backups_to_retain)