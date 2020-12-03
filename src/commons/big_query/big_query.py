import json
import logging

import googleapiclient.discovery
from apiclient.errors import HttpError, Error
from oauth2client.client import GoogleCredentials

from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.config.configuration import configuration
from src.commons.decorators.cached import cached
from src.commons.decorators.google_http_error_retry import \
    google_http_error_retry
from src.commons.decorators.log_time import log_time
from src.commons.decorators.retry import retry
from src.commons.table_reference import TableReference


class TableNotFoundException(Exception):
    pass


class DatasetNotFoundException(Exception):
    pass


class BigQuery(object):
    def __init__(self):
        self.service = googleapiclient.discovery.build(
            'bigquery',
            'v2',
            credentials=self._create_credentials(),
            http=self._create_http(),
            cache_discovery=self._cache_discovery()
        )

    @staticmethod
    def _create_credentials():
        return GoogleCredentials.get_application_default()

    @staticmethod
    def _create_http():
        return None

    @staticmethod
    def _cache_discovery():
        return True

    @retry(Error, tries=3, delay=2, backoff=1)
    def list_project_ids(self, page_token=None, max_results=1000):
        projects_list_result = self.service.projects().list(
            maxResults=max_results,
            pageToken=page_token
        ).execute()
        project_ids = [project['projectReference']['projectId']
                       for project in projects_list_result.get('projects', [])]

        return project_ids, projects_list_result.get('nextPageToken')

    @retry(Error, tries=3, delay=2, backoff=2)
    def list_dataset_ids(self, project_id, page_token=None, max_results=1000):
        datasets_list_result = self.service.datasets().list(
            projectId=project_id,
            maxResults=max_results,
            pageToken=page_token
        ).execute()

        dataset_ids = [dataset['datasetReference']['datasetId']
                       for dataset in datasets_list_result.get('datasets', [])]

        return dataset_ids, datasets_list_result.get('nextPageToken')

    @retry(Error, tries=3, delay=2, backoff=2)
    def list_table_ids(self, project_id, dataset_id, page_token=None,
        max_results=1000):
        tables_list_result = {}
        try:
            tables_list_result = self.service.tables().list(
                projectId=project_id,
                datasetId=dataset_id,
                maxResults=max_results,
                pageToken=page_token
            ).execute()
        except HttpError as ex:
            if ex.resp.status == 404 and 'Not found: Dataset' in ex.content:
                logging.warning('Dataset \'%s:%s\' is not found', project_id,
                                dataset_id)
                # TODO consider throwing DatasetNotFoundException instead
                return [], None
            raise ex

        tables_ids = [table['tableReference']['tableId']
                      for table in tables_list_result.get('tables', [])]

        return tables_ids, tables_list_result.get('nextPageToken')

    def execute_query(self, query, use_legacy_sql=True):
        query_job = self.__sync_query(query=query,
                                      use_legacy_sql=use_legacy_sql)
        results = []
        page_token = None

        while True:
            response = self.service.jobs().getQueryResults(
                pageToken=page_token,
                **query_job['jobReference']).execute(num_retries=2)
            job_complete = response.get('jobComplete')
            if job_complete:
                results.extend(response.get('rows', []))
                page_token = response.get('pageToken')
                if not page_token:
                    break

        return results

    @log_time
    @google_http_error_retry(tries=6, delay=2, backoff=2)
    def list_table_partitions(self, project_id, dataset_id, table_id):
        results = []
        try:
            results = self.execute_query(
                self.create_partition_query(project_id, dataset_id, table_id))
        except HttpError as ex:
            if ex.resp.status == 404 and 'Not found: Table' in ex.content:
                raise TableNotFoundException(u'Table {}:{}.{} not found'.
                                             format(project_id, dataset_id,
                                                    table_id))
        partitions = [
            {'partitionId': _partition['f'][0]['v'],
             'creationTime': _partition['f'][1]['v'],
             'lastModifiedTime': _partition['f'][2]['v']}
            for _partition in results
        ]
        return partitions

    @staticmethod
    def create_partition_query(project_id, dataset_id, table_id):
        return u'SELECT partition_id, FORMAT_UTC_USEC(creation_time*1000) AS ' \
               u'creation_time, FORMAT_UTC_USEC(last_modified_time*1000)' \
               u' AS last_modified FROM [{0}:{1}.{2}$__PARTITIONS_SUMMARY__]' \
            .format(project_id, dataset_id, table_id)

    def __sync_query(self, query, timeout=30000, use_legacy_sql=False):
        query_data = {
            'query': query,
            'timeoutMs': timeout,
            'useLegacySql': use_legacy_sql
        }
        return self.service.jobs().query(
            projectId=configuration.backup_project_id,
            body=query_data).execute(num_retries=3)

    @google_http_error_retry(tries=6, delay=2, backoff=2)
    def get_table(self, project_id, dataset_id, table_id, log_table=True):
        logging.info(u'Getting table %s',
                     BigQueryTable(project_id, dataset_id, table_id))
        try:
            table = self.service.tables().get(
                projectId=project_id, datasetId=dataset_id,
                tableId=table_id if isinstance(table_id,
                                               str) else table_id.encode('utf8')
            ).execute(num_retries=3)

            if log_table and table:
                self.__log_table(table)

            return table

        except HttpError as ex:
            if ex.resp.status == 404:
                logging.info(
                    "Table '%s' Not Found",
                    TableReference(project_id, dataset_id, table_id)
                )
                return None
            raise ex

    def __log_table(self, table):
        table_copy = table.copy()
        if 'schema' in table_copy:
            del table_copy['schema']
        logging.info(u'Table: ' + json.dumps(table_copy))

    @google_http_error_retry(tries=6, delay=2, backoff=2)
    def get_dataset(self, project_id, dataset_id):
        try:
            dataset = self.service.datasets().get(
                projectId=project_id, datasetId=dataset_id
            ).execute(num_retries=3)

            logging.info("Dataset: " + json.dumps(dataset))
            return dataset
        except HttpError as ex:
            if ex.resp.status == 404:
                logging.warning(
                    "Dataset '%s:%s' Not Found", project_id, dataset_id
                )
                return None
            logging.info('Can\'t fetch dataset: %s', ex.resp)
            raise ex

    @cached(time=300)
    def get_dataset_cached(self, project_id, dataset_id):
        return self.get_dataset(project_id, dataset_id)

    def insert_job(self, project_id, body):
        response = self.service.jobs().insert(
            projectId=project_id, body=body
        ).execute()
        logging.info('Insert job response: ' + json.dumps(response))
        return BigQueryJobReference(
            project_id=response['jobReference']['projectId'],
            job_id=response['jobReference']['jobId'],
            location=response['jobReference']['location'])

    def get_job(self, job_reference):
        return self.service.jobs().get(
            projectId=job_reference.project_id,
            jobId=job_reference.job_id,
            location=job_reference.location
        ).execute(num_retries=3)

    @retry(Error, tries=3, delay=2, backoff=2)
    def create_table(self, projectId, datasetId, body):
        table = BigQueryTable(projectId, datasetId,
                              body.get("tableReference").get("tableId"))

        logging.info("Creating table %s", table)
        logging.info("BODY: %s", json.dumps(body))

        try:
            self.service.tables().insert(
                projectId=projectId,
                datasetId=datasetId,
                body=body
            ).execute()
        except HttpError as error:
            if error.resp.status == 409:
                logging.info('Table already exists %s', table)
            else:
                raise

    # @refactor - boolean argument
    @retry(Error, tries=6, delay=2, backoff=2)
    def create_dataset(
        self, project_id, dataset_id, location, table_expiration_in_ms=False
    ):
        logging.info(
            "Creating dataset %s / %s (location: %s)",
            project_id, dataset_id, location
        )
        body = {
            'datasetReference': {
                'projectId': project_id,
                'datasetId': dataset_id
            },
            'location': location
        }

        if table_expiration_in_ms and \
            isinstance(table_expiration_in_ms,
                       (int, long)):  # pylint: disable=E0602
            body['defaultTableExpirationMs'] = table_expiration_in_ms

        try:
            self.service.datasets().insert(
                projectId=project_id, body=body
            ).execute()
        except HttpError as error:
            if self.__is_access_denied(error):
                logging.warning('Access Denied, can not create dataset %s:%s',
                                project_id, dataset_id)
            elif error.resp.status == 409:
                logging.info('Dataset %s / %s already exists',
                             project_id, dataset_id)
            else:
                raise

    @staticmethod
    def __is_access_denied(error):
        return error.resp.status == 403 and \
               error._get_reason().find('Access Denied') != -1

    @retry(Error, tries=6, delay=2, backoff=2)
    def delete_table(self, table_reference):
        try:
            logging.info(u"Deleting table '%s'", table_reference)
            table_id = table_reference.get_table_id()
            self.service.tables().delete(
                datasetId=table_reference.get_dataset_id(),
                projectId=table_reference.get_project_id(),
                tableId=table_id if isinstance(table_id, str)
                else table_id.encode('utf8')).execute()
        except HttpError as ex:
            if ex.resp.status == 404:
                raise TableNotFoundException("Table '{}' Not Found".format(
                    table_reference))
            else:
                raise ex

    def get_dataset_location(self, project_id, dataset_id):
        dataset = self.get_dataset(project_id, dataset_id)
        if not dataset:
            raise DatasetNotFoundException(
                'Dataset Not Found: {}:{}'.format(
                    project_id, dataset_id
                )
            )
        return dataset.get('location')

    @retry(HttpError, tries=6, delay=2, backoff=2)
    def disable_partition_expiration(self, project_id, dataset_id, table_id):
        logging.info(u"Disabling partition expiration for table %s:%s.%s",
                     project_id, dataset_id, table_id)
        table_data = {
            "timePartitioning": {
                "expirationMs": None,
            }
        }
        self.service.tables().patch(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=table_data).execute()


class RandomizationError(BaseException):
    pass
