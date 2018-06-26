from datetime import datetime

import webapp2

from commons.exceptions import ParameterValidationException
from commons.json_handler import JsonHandler
from src.bbq_authenticated_handler import BbqAuthenticatedHandler
from src.big_query import validators
from src.configuration import configuration
from src.restore.table.table_restore_service import TableRestoreService
from src.table_reference import TableReference


class TableRestoreHandler(JsonHandler):

    def post(self, project_id, dataset_id, table_id):
        partition_id = self.request.get('partitionId', None)
        target_dataset_id = self.request.get('targetDatasetId', None)
        if target_dataset_id:
            validators.validate_dataset_id(target_dataset_id)

        restoration_datetime = self.__get_restoration_datetime()

        table_reference = TableReference(project_id, dataset_id,
                                         table_id, partition_id)

        restore_data = TableRestoreService.restore(
            table_reference, target_dataset_id, restoration_datetime
        )
        self._finish_with_success(restore_data)

    def __get_restoration_datetime(self):
        restoration_date = self.request.get('restorationDate', None)
        if restoration_date:
            self.__validate_restoration_date(restoration_date)
            restoration_date = datetime.strptime(
                '{} 23:59:59'.format(restoration_date), '%Y-%m-%d %H:%M:%S'
            )
        return restoration_date

    @staticmethod
    def __validate_restoration_date(date):
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise ParameterValidationException(
                "Wrong date value format for parameter 'restoration_date'. "
                "Should be 'YYYY-mm-dd'")


class TableRestoreAuthenticatedHandler(BbqAuthenticatedHandler,
                                       TableRestoreHandler):
    def __init__(self, request=None, response=None):
        super(TableRestoreAuthenticatedHandler, self). \
            __init__(request, response)


app = webapp2.WSGIApplication([
    webapp2.Route(
        '/restore/table/<project_id:.*>/<dataset_id:.*>/<table_id:.*>',
        TableRestoreHandler
    ),
    webapp2.Route(
        '/restore/schedule/table/<project_id:.*>/<dataset_id:.*>/<table_id:.*>/<partition_id:.*>',
        TableRestoreAuthenticatedHandler
    )
], debug=configuration.debug_mode)
