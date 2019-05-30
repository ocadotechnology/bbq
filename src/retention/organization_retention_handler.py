import datetime

import webapp2
from dateutil.relativedelta import relativedelta
from google.appengine.api.taskqueue import Task
from google.appengine.datastore.datastore_query import Cursor

from src.backup.datastore.Table import Table
from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class OrganizationRetentionHandler(webapp2.RequestHandler):

    QUERY_PAGE_SIZE = 2500

    def get(self):
        cursor = Cursor(urlsafe=self.request.get('cursor'))
        self.__schedule_retention_starting_from_cursor(cursor)

    @classmethod
    def __schedule_retention_starting_from_cursor(cls, table_cursor):
        results, next_cursor, more = Table.query() \
            .filter(OrganizationRetentionHandler.table_with_backup()) \
            .order(Table.last_checked, Table.key) \
            .fetch_page(
            page_size=cls.QUERY_PAGE_SIZE,
            start_cursor=table_cursor
        )
        tasks = [cls.__create_table_retention_task(result)
                 for result in results]
        Tasks.schedule(queue_name='table-retention', tasks=tasks)
        if more and next_cursor:
            task = Task(
                method='GET',
                url='/cron/retention',
                params={
                    'cursor': next_cursor.urlsafe(),
                })

            Tasks.schedule(queue_name='table-retention-scheduler', tasks=[task])

    @staticmethod
    def table_with_backup():
        age_threshold_datetime = datetime.datetime.today() - relativedelta(
            months=(configuration.grace_period_after_source_table_deletion_in_months + 1))

        return Table.last_checked >= age_threshold_datetime

    @staticmethod
    def __create_table_retention_task(table):
        params = {'projectId': table.project_id,
                  'datasetId': table.dataset_id,
                  'tableId': table.table_id,
                  'tableKey': table.key.urlsafe()}
        if table.partition_id:
            params['partitionId'] = table.partition_id
        return Task(
            method='GET',
            url='/tasks/retention/table',
            params=params)
