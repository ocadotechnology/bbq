import unittest

from mock import Mock, patch

from src.slo.backup_creation_latency.latency_query_specification import \
    LatencyQuerySpecification
from src.slo.predicate.sli_table_exists_predicate import SLITableExistsPredicate
from src.commons.big_query.big_query import BigQuery


class TestSLITableExistsPredicate(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value=None))
    def test_should_return_false_for_not_existing_table(self):
        # given
        sli_table = self.__create_non_partitioned_sli_table()

        # when
        exists = SLITableExistsPredicate(BigQuery(), LatencyQuerySpecification).exists(sli_table)

        # then
        self.assertFalse(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'schema': {'fields': []}}))
    def test_should_return_true_for_existing_table(self):
        # given
        sli_table = self.__create_non_partitioned_sli_table()

        # when
        exists = SLITableExistsPredicate(BigQuery(), LatencyQuerySpecification).exists(sli_table)

        # then
        self.assertTrue(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'schema': {'fields': []},
                              'timePartitioning': {'type': 'DAY'}}))
    @patch('src.commons.big_query.big_query.BigQuery.list_table_partitions',
           Mock(return_value=[]))
    def test_should_return_false_for_not_existing_partition(self):
        # given
        sli_table = self.__create_partitioned_sli_table()

        # when
        exists = SLITableExistsPredicate(BigQuery(), LatencyQuerySpecification).exists(sli_table)

        # then
        self.assertFalse(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'schema': {'fields': []}}))
    @patch('src.commons.big_query.big_query.BigQuery.list_table_partitions',
           Mock(return_value=[{'partitionId': '20180808'}]))
    def test_should_return_true_for_existing_partition(self):
        # given
        sli_table = self.__create_partitioned_sli_table()

        # when
        exists = SLITableExistsPredicate(BigQuery(), LatencyQuerySpecification).exists(sli_table)

        # then
        self.assertTrue(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p'}))
    def test_should_return_false_when_there_is_no_schema(self):
        # given
        sli_table = self.__create_non_partitioned_sli_table()

        # when
        exists = SLITableExistsPredicate(BigQuery(), LatencyQuerySpecification).exists(sli_table)

        # then
        self.assertFalse(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'schema': {'fields': []}}))
    @patch.object(BigQuery, 'list_table_partitions')
    def test_should_not_list_partitions_in_non_partitioned_table(self, list_table_partitions):
        # given
        sli_table = self.__create_non_partitioned_sli_table()

        # when
        exists = SLITableExistsPredicate(BigQuery(), LatencyQuerySpecification).exists(sli_table)

        # then
        self.assertTrue(exists)
        list_table_partitions.assert_not_called()

    def __create_non_partitioned_sli_table(self):
        return {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": None,
            "creationTime": '1500000000000',
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

    def __create_partitioned_sli_table(self):
        return {
            "snapshotTime": None,
            "projectId": 'p',
            "datasetId": 'd',
            "tableId": 'd',
            "partitionId": '20180808',
            "creationTime": '1500000000000',
            "lastModifiedTime": None,
            "backupCreated": None,
            "backupLastModified": None,
            "xDays": 4
        }

