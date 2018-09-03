import logging

from src.commons.big_query.streaming.data_streamer import DataStreamer
from src.commons.config.configuration import configuration


class SLIResultsStreamer(object):
    def __init__(self,
                 project_id=configuration.backup_project_id,
                 dataset_id="SLO_views_legacy",
                 table_id="SLI_filtered"
                 ):
        self.data_streamer = DataStreamer(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    def stream(self, sli_results):
        if len(sli_results) == 0:
            logging.info("Nothing to stream")
            return

        logging.info("Streaming SLI results: %s", sli_results)
        self.data_streamer.stream_stats(sli_results)
        logging.info("SLI results streamed")
