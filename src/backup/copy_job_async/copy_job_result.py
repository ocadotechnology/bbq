from datetime import datetime

from src.big_query.big_query_table import BigQueryTable
from src.table_reference import TableReference


class CopyJobResult(object):
    def __init__(self, job_json):
        self.__job_json = job_json
        self.__configuration = job_json['configuration']['copy']

    @property
    def error_message(self):
        errors = [x['reason'] + ':' + x.get('message', 'No message provided')
                  for x in self.__job_json['status']['errors']]
        return 'Copy job finished with errors: {}'.format(', '.join(errors))

    @property
    def error_result(self):
        return self.__job_json['status']['errorResult']

    @property
    def source_project_id(self):
        return self.__configuration['sourceTable']['projectId']

    @property
    def source_dataset_id(self):
        return self.__configuration['sourceTable']['datasetId']

    @property
    def source_table_id(self):
        return self.__configuration['sourceTable']['tableId']

    @property
    def source_bq_table(self):
        return BigQueryTable(self.source_project_id, self.source_dataset_id,
                             self.source_table_id)

    @property
    def source_table_reference(self):
        table_id, partition_id = BigQueryTable \
            .split_table_and_partition_id(self.source_table_id)

        return TableReference(project_id=self.source_project_id,
                              dataset_id=self.source_dataset_id,
                              table_id=table_id,
                              partition_id=partition_id)

    @property
    def start_time(self):
        start_time = float(self.__job_json['statistics']['startTime']) / 1000.0
        return datetime.fromtimestamp(start_time)

    @property
    def end_time(self):
        end_time = float(self.__job_json['statistics']['endTime']) / 1000.0
        return datetime.fromtimestamp(end_time)

    @property
    def target_project_id(self):
        return self.__configuration['destinationTable']['projectId']

    @property
    def target_dataset_id(self):
        return self.__configuration['destinationTable']['datasetId']

    @property
    def target_table_id(self):
        return self.__configuration['destinationTable']['tableId']

    @property
    def target_bq_table(self):
        return BigQueryTable(self.target_project_id, self.target_dataset_id,
                             self.target_table_id)

    @property
    def target_table_reference(self):
        table_id, partition_id = BigQueryTable\
            .split_table_and_partition_id(self.target_table_id)

        return TableReference(project_id=self.target_project_id,
                              dataset_id=self.target_dataset_id,
                              table_id=table_id,
                              partition_id=partition_id)

    def has_errors(self):
        return 'errors' in self.__job_json['status']

    def is_done(self):
        return self.__job_json['status']['state'] == 'DONE'

    @property
    def status(self):
        return self.__job_json['status']

    def get_raw_job_json(self):
        return self.__job_json
