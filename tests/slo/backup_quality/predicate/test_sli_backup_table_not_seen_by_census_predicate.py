import unittest

from mock import Mock, patch

from src.commons.big_query.big_query import BigQuery
from src.slo.backup_quality.predicate.sli_backup_table_not_seen_by_census_predicate import \
    SLIBackupTableNotSeenByCensusPredicate
from src.slo.backup_quality.quality_query_specification import \
    QualityQuerySpecification


class TestSLIBackupTableNotSeenByCensusPredicate(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p',
                              'numBytes': 100,
                              'schema': {'fields': []}})
           )
    def test_should_return_true_if_backup_table_havent_data_from_census_but_table_exists(
        self):
        # given
        sli_table = self.__create_sli_entry_without_census_data()

        # when
        is_not_seen_by_census = SLIBackupTableNotSeenByCensusPredicate(
            BigQuery(), QualityQuerySpecification).is_not_seen_by_census(
            sli_table)

        # then
        self.assertTrue(is_not_seen_by_census)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p',
                              'numBytes': 12345,
                              'schema': {'fields': []}})
           )
    def test_should_return_false_if_backup_table_havent_data_from_census_and_datastore_num_bytes_are_different_than_reality(
        self):
        # given
        sli_table = self.__create_sli_entry_without_census_data()

        # when
        is_not_seen_by_census = SLIBackupTableNotSeenByCensusPredicate(
            BigQuery(), QualityQuerySpecification).is_not_seen_by_census(
            sli_table)

        # then
        self.assertFalse(is_not_seen_by_census)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p',
                              'numBytes': 12345,
                              'schema': {'fields': []}})
           )
    def test_should_return_false_if_backup_table_have_data_from_census(self):
        # given
        sli_table = self.__create_sli_entry_with_census_data()

        # when
        is_not_seen_by_census = SLIBackupTableNotSeenByCensusPredicate(
            BigQuery(), QualityQuerySpecification).is_not_seen_by_census(
            sli_table)

        # then
        self.assertFalse(is_not_seen_by_census)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value=None))
    def test_should_return_false_if_backup_table_doesnt_exists(self):
        # given
        sli_table = self.__create_sli_entry_without_census_data()

        # when
        is_not_seen_by_census = SLIBackupTableNotSeenByCensusPredicate(
            BigQuery(), QualityQuerySpecification).is_not_seen_by_census(
            sli_table)

        # then
        self.assertFalse(is_not_seen_by_census)

    def __create_sli_entry_without_census_data(self):
        return {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            'backupDatasetId': 'b_d',
            'backupTableId': 'b_t',
            "partitionId": None,
            "creationTime": '1500000000000',
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModifiedTime": None,
            "backupEntityNumBytes": 100,
            "backupNumBytes": None,
            "xDays": 4
        }

    def __create_sli_entry_with_census_data(self):
        return {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            'backupDatasetId': 'b_d',
            'backupTableId': 'b_t',
            "partitionId": None,
            "creationTime": '1500000000000',
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModifiedTime": '1500000000212',
            "backupEntityNumBytes": 100,
            "backupNumBytes": None,
            "xDays": 4
        }
