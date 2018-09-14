import unittest

from mock import Mock, MagicMock, patch

from src.commons.table_reference import TableReference
from src.slo.x_days_sli.sli_table_exists_filter import SLITableExistsFilter
from src.commons.big_query.big_query import BigQuery


class TestSLITableExistsFilter(unittest.TestCase):

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value=None))
    def test_should_return_false_for_not_existing_table(self):
        # given
        table_reference = TableReference('p', 'd', 't')

        # when
        exists = SLITableExistsFilter(BigQuery()).exists(table_reference)

        # then
        self.assertFalse(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'schema': {'fields': []}}))
    def test_should_return_true_for_existing_table(self):
        # given
        table_reference = TableReference('p', 'd', 't')

        # when
        exists = SLITableExistsFilter(BigQuery()).exists(table_reference)

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
        table_reference = TableReference('p', 'd', 't', '20180808')

        # when
        exists = SLITableExistsFilter(BigQuery()).exists(table_reference)

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
        table_reference = TableReference('p', 'd', 't', '20180808')

        # when
        exists = SLITableExistsFilter(BigQuery()).exists(table_reference)

        # then
        self.assertTrue(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p'}))
    def test_should_return_false_when_there_is_no_schema(self):
        # given
        table_reference = TableReference('p', 'd', 't')

        # when
        exists = SLITableExistsFilter(BigQuery()).exists(table_reference)

        # then
        self.assertFalse(exists)

    @patch('src.commons.big_query.big_query.BigQuery.__init__',
           Mock(return_value=None))
    @patch('src.commons.big_query.big_query.BigQuery.get_table',
           Mock(return_value={'projectId': 'p', 'schema': {'fields': []}}))
    @patch.object(BigQuery, 'list_table_partitions')
    def test_should_not_list_partitions_in_non_partitioned_table(self, list_table_partitions):
        # given
        table_reference = TableReference('p', 'd', 't')

        # when
        exists = SLITableExistsFilter(BigQuery()).exists(table_reference)

        # then
        self.assertTrue(exists)
        list_table_partitions.assert_not_called()
