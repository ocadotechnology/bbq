import unittest

from mock import patch

from src.slo.x_days_sli.sli_results_streamer import SLIResultsStreamer
from src.slo.x_days_sli.sli_table_exists_filter import SLITableExistsFilter
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier
from src.slo.x_days_sli.x_days_sli_service import XDaysSLIService


class TestXDaysSLIService(unittest.TestCase):

    @patch.object(SLIViewQuerier, 'query', return_value=[
        {"projectId":"p1", "datasetId":"d1", "tableId":"t1", "partitionId":"part1"}
    ])
    @patch.object(SLITableExistsFilter, 'exists', side_effect=Exception("An error"))
    @patch.object(SLIResultsStreamer, 'stream')
    def test_table_that_caused_exception_should_not_be_filtered_out(self, stream, _, query):
        XDaysSLIService(3).recalculate_sli()
        stream.assert_called_with([{"projectId":"p1", "datasetId":"d1", "tableId":"t1", "partitionId":"part1"}])
