import webapp2
from datetime import datetime

from src.commons.config.configuration import configuration
from src.commons.exceptions import ParameterValidationException
from src.commons.handlers import validators
from src.commons.handlers.json_handler import JsonHandler
from src.commons.table_reference import TableReference
from src.restore.table.table_restore_service import TableRestoreService


class TableRestoreHandler(JsonHandler):

    def get(self, project_id, dataset_id, table_id):
        partition_id = self.request.get('partitionId', None)
        is_restore_to_source_project = self.request.get('isRestoreToSourceProject', None)
        target_dataset_id = self.request.get('targetDatasetId', None)
        create_disposition = self.request.get('createDisposition', None)
        write_disposition = self.request.get('writeDisposition', None)

        target_project_id = None if is_restore_to_source_project \
            else configuration.default_restoration_project_id

        validators.validate_restore_request_params(
            target_project_id=target_project_id,
            target_dataset_id=target_dataset_id,
            create_disposition=create_disposition,
            write_disposition=write_disposition
        )

        restoration_datetime = self.__get_restoration_datetime()

        table_reference = TableReference(project_id, dataset_id,
                                         table_id, partition_id)

        restore_data = TableRestoreService.restore(table_reference,
                                                   target_project_id,
                                                   target_dataset_id,
                                                   create_disposition,
                                                   write_disposition,
                                                   restoration_datetime)
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


app = webapp2.WSGIApplication([
    webapp2.Route(
        '/restore'
        '/project/<project_id:.*>/dataset/<dataset_id:.*>/table/<table_id:.*>',
        TableRestoreHandler
    )
], debug=configuration.debug_mode)
