import unittest

from mock import patch
from src.backup.datastore.Table import Table
from src.backup.on_demand.on_demand_table_backup import OnDemandTableBackup
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.exceptions import ParameterValidationException
from src.commons.table_reference import TableReference


class TestOnDemandTableBackup(unittest.TestCase):

    @patch.object(Table, "get_table", return_value=None)
    @patch('src.commons.big_query.big_query_table_metadata.BigQueryTableMetadata.get_table_by_reference',
           return_value=BigQueryTableMetadata(
               {"tableReference": {
                   "projectId": "test-project",
                   "datasetId": "test-dataset",
                   "tableId": "test-table-without-partition"
               },
                   "timePartitioning": {
                       "type": "DAY"
                   }}
           ))
    def test_should_throw_parameter_validation_exception_if_table_is_partitioned_but_partition_number_was_not_given(
            self, _1, _2):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id="")

        # when-then
        with self.assertRaises(ParameterValidationException):
            OnDemandTableBackup.start(table_reference)
