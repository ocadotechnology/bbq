import logging

from src.backup.datastore.Table import Table
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.exceptions import ParameterValidationException


class DatasetRestoreParametersValidator(object):

    def __init__(self):
        self.BQ = BigQuery()

    def validate_parameters(self, project_id, dataset_id, target_project_id,
            target_dataset_id, max_partition_days):

        if target_project_id is None:
            raise ParameterValidationException("Required target project id parameter is None")

        any_backup = self.__get_backup(project_id, dataset_id,
                                       max_partition_days)

        self.__validate_locations(any_backup, target_project_id, target_dataset_id)

    def __get_backup(self, project_id, dataset_id, max_partition_days):
        logging.info(
            "Getting backups for project '%s' for dataset '%s'"
            " with max_partition_days '%s'",
            project_id, dataset_id, max_partition_days)
        table_entities_found = False
        tables = self.__get_tables(project_id, dataset_id, max_partition_days)
        for table in tables:
            table_entities_found = True
            table_backup = table.last_backup
            if table_backup is not None:
                return table_backup

        if not table_entities_found:
            # @refactor: this should be NotFoundException that is mapped to 404.
            # Not ParameterValidationException that is mapped to 400
            raise ParameterValidationException(
                "No Tables was found in Datastore for project {}, dataset {}"
                    .format(project_id, dataset_id))

        # @refactor: same as above
        raise ParameterValidationException(
            "No Backups was found in Datastore for project {}, dataset {}"
            .format(project_id, dataset_id))

    def __get_tables(self, project_id, dataset_id, max_partition_days):
        if max_partition_days is None:
            return Table.get_tables(project_id, dataset_id, page_size=20)
        else:
            return Table.get_tables_with_max_partition_days(project_id,
                                                            dataset_id,
                                                            max_partition_days,
                                                            page_size=20)

    def __get_target_dataset_location(self, target_project_id, target_dataset_id):
        target_dataset = self.BQ.get_dataset(
            project_id=target_project_id,
            dataset_id=target_dataset_id)
        if target_dataset is None:
            return None
        return self.__extract_location(target_dataset)

    def __get_backup_dataset_location(self, any_backup):
        backup_dataset = self.BQ.get_dataset(configuration.backup_project_id,
                                             any_backup.dataset_id)
        if backup_dataset is None:
            return None
        return self.__extract_location(backup_dataset)

    def __validate_locations(self, any_backup, target_project_id, target_dataset_id):
        target_location = self.__get_target_dataset_location(target_project_id,target_dataset_id)

        if target_location is None:
            return

        backup_location = self.__get_backup_dataset_location(any_backup)
        if target_location != backup_location:
            raise ParameterValidationException(
                "Target dataset already exist and has different location than backup dataset")

    @staticmethod
    def __extract_location(dataset):
        return dataset.get('location')
