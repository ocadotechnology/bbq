import webapp2
from google.appengine.api.taskqueue import Task
from google.appengine.datastore.datastore_query import Cursor

from src.commons.tasks import Tasks
from src.backup.datastore.Table import Table
from src.commons.handlers.bbq_authenticated_handler import BbqAuthenticatedHandler


class OrganizationRetentionHandler(webapp2.RequestHandler):

    QUERY_PAGE_SIZE = 5000

    def get(self):
        cursor = Cursor(urlsafe=self.request.get('cursor'))
        self.__schedule_retention_starting_from_cursor(cursor)

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

    @classmethod
    def __schedule_retention_starting_from_cursor(cls, table_cursor):
        results, next_cursor, more = Table.query().fetch_page(
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


class OrganizationRetentionAuthenticatedHandler(
        BbqAuthenticatedHandler,
        OrganizationRetentionHandler
):

    def __init__(self, request=None, response=None):
        super(OrganizationRetentionAuthenticatedHandler, self). \
            __init__(request, response)
