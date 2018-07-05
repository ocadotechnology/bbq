import json
import logging
import time

import httplib2

from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class JobInProgressException(Exception):
    pass


class TableRestoreInvoker(object):

    def __init__(self, host_url):
        self.http = self._create_http()
        self.host_url = host_url

    @staticmethod
    def _create_http():
        return httplib2.Http(timeout=60)

    def invoke(self, src_table_reference, target_dataset,
               restoration_point_date):

        url = self.__build_restore_url(self.host_url, src_table_reference,
                                       target_dataset, restoration_point_date)
        resp, content = self.http.request(url)  # pylint: disable=W0612
        logging.info("Response: %s", resp)
        resp_data = json.loads(content)
        return resp_data['restorationJobId']

    @staticmethod
    def wait_till_done(restoration_job_id, timeout, period=20):
        finish_time = time.time() + timeout
        while time.time() < finish_time:
            result = RestorationJobStatusService() \
                .get_restoration_job(restoration_job_id)
            if result["status"]["state"] in "Done":
                return result
            time.sleep(period)
        return RestorationJobStatusService()\
            .get_restoration_job(restoration_job_id)

    @staticmethod
    def __build_restore_url(host_url, src_table_reference,
                            target_dataset, restore_point_date):
        url = host_url + "/restore/project/{0}/dataset/{1}/table/{2}?" \
                         "targetDatasetId={3}&" \
                         "restorationDate={4}" \
            .format(src_table_reference.get_project_id(),
                    src_table_reference.get_dataset_id(),
                    src_table_reference.get_table_id(),
                    target_dataset,
                    restore_point_date)

        if src_table_reference.is_partition():
            url += "&partitionId={0}".format(
                src_table_reference.get_partition_id())

        logging.info("Restore request URL: '%s'", url)
        return url
