import json
import logging


import httplib2



class TableRestoreInvoker(object):

    def __init__(self, host_url):
        self.http = self._create_http()
        self.host_url = host_url

    @staticmethod
    def _create_http():
        return httplib2.Http(timeout=60)

    def invoke(self, src_table_reference, target_dataset, restoration_point_date):
        url = self.__build_restore_url(self.host_url, src_table_reference, target_dataset, restoration_point_date)
        resp, content = self.http.request(url)  # pylint: disable=W0612
        logging.info("Response: %s", resp)
        resp_data = json.loads(content)
        logging.info("Restored table details: %s", resp_data)
        return resp_data

    def __build_restore_url(self, host_url, src_table_reference,
                            target_dataset,
                            restore_point_date):
        url = host_url + "/restore/table/{0}/{1}/{2}?" \
                                      "target_dataset_id={3}&" \
                                      "restoration_date={4}"\
            .format(
                src_table_reference.get_project_id(),
                src_table_reference.get_dataset_id(),
                src_table_reference.get_table_id(),
                target_dataset,
                restore_point_date)

        if src_table_reference.is_partition():
            url += "&partition_id={0}".format(
                src_table_reference.get_partition_id())

        logging.info("Restore request URL: '%s'", url)
        return url
