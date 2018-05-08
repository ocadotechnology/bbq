import webapp2
from datetime import datetime

from commons.exceptions import ParameterValidationException
from commons.json_handler import JsonHandler
from src.environment import Environment
from src.bbq_authenticated_handler import BbqAuthenticatedHandler
from src.big_query import validators
from src.restore.big_query_table_restorer import DatasetNotFoundException, \
    RestoreTableException
from src.restore.restore_service import RestoreService, \
    TableNotFoundException, \
    BackupNotFoundException, Restoration
from src.table_reference import TableReference


class TableRestoreHandler(JsonHandler):

    def get(self, **kwargs):
        try:
            self.__validate_query_params(self.request)
            restoration = self.__create_restoration(kwargs)
            restore_data = RestoreService().try_to_restore(restoration)
        except ParameterValidationException as e:
            self._finish_with_error(
                response_code=400,
                msg='Wrong request params provided:{}'.format(e.message)
            )
        except (TableNotFoundException, DatasetNotFoundException) as e:
            self._finish_with_error(response_code=410, msg=e.message)
        except RestoreTableException as e:
            self._finish_with_error(response_code=500, msg=e.message)
        except BackupNotFoundException:
            msg = "Backup for table {} made {} does not exist.".format(
                TableReference(kwargs.get('project_id'),
                               kwargs.get('dataset_id'),
                               kwargs.get('table_id')),
                self.request.get('restoration_date'), None)
            self._finish_with_error(response_code=404, msg=msg)
        else:
            self._finish_with_success(restore_data)

    def __create_restoration(self, url_params):
        source_project_id = url_params.get('project_id')
        source_dataset_id = url_params.get('dataset_id')
        source_table_id = url_params.get('table_id')
        source_partition_id = self.request.get('partition_id', None)
        restoration_date = self.request.get('restoration_date',
                                            datetime.utcnow()
                                            .strftime('%Y-%m-%d'))
        target_dataset_id = self.request.get('target_dataset_id',
                                             self.__create_default_dataset_id(
                                                 source_project_id,
                                                 source_dataset_id))
        restoration = Restoration(source_project_id, source_dataset_id,
                                  source_table_id, target_dataset_id,
                                  restoration_date,
                                  source_partition_id)
        return restoration

    @staticmethod
    def __create_default_dataset_id(project_id, dataset_id):
        return '{}___{}'.format(project_id, dataset_id).replace('-', '_')

    def __validate_query_params(self, request):  # pylint: disable=R0201
        possible_params = ['target_dataset_id', 'restoration_date',
                           'partition_id']
        if not set(request.arguments()).issubset(set(possible_params)):
            raise ParameterValidationException(
                "There are some unknown parameters provided. Possible params: {}. Provided params: {}".format(
                    possible_params, request.arguments()))

        if not all(request.get(value) for value in request.arguments()):
            raise ParameterValidationException(
                "Some provided parameters were empty")

        restoration_date = request.get('restoration_date', None)
        if restoration_date:
            try:
                datetime.strptime(restoration_date, '%Y-%m-%d')
            except ValueError:
                raise ParameterValidationException(
                    "Wrong date value format for parameter 'restoration_date'. "
                    "Should be 'YYYY-mm-dd'")

        target_dataset_id = self.request.get('target_dataset_id', None)
        if target_dataset_id:
            validators.validate_dataset_id(target_dataset_id)


class TableRestoreAuthenticatedHandler(BbqAuthenticatedHandler,
                                       TableRestoreHandler):
    pass


app = webapp2.WSGIApplication([
    webapp2.Route(
        '/restore/table/<project_id:.*>/<dataset_id:.*>/<table_id:.*>',
        TableRestoreHandler,
    ),
    webapp2.Route(
        '/restore/schedule/table/<project_id:.*>/<dataset_id:.*>/<table_id:.*>',
        TableRestoreAuthenticatedHandler,
    )
], debug=Environment.is_debug_mode_allowed())
