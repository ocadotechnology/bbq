import unittest

from mock import patch

from src.commons.big_query.streaming.data_streamer import DataStreamer
from src.slo.sli_results_streamer import SLIResultsStreamer


class TestSliResultsStreamer(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()

    def tearDown(self):
        patch.stopall()

    @patch.object(DataStreamer, "stream_stats")
    def test_snapshot_MARKER_is_added_to_the_streaming_elements(self, stream_stats):
        # pass
        under_test = SLIResultsStreamer("table1", "dataset1", "project1")

        under_test.stream([{"col1": "MARKER", "col2": "MARKER"}])

        stream_stats.assert_called_once_with([{"col1": "MARKER", "col2": "MARKER"}])
