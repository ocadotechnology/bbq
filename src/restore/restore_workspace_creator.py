from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata


class RestoreWorkspaceCreator(object):

    def __init__(self, big_query):
        self.BQ = big_query

    def create_workspace(self, source_table_reference,
                         target_table_reference):

        self.__create_target_dataset_if_not_exists(
            source_table_reference,
            target_table_reference)

        self.__create_empty_partitioned_table_if_not_exists(
            source_table_reference, target_table_reference)

    def __create_target_dataset_if_not_exists(self, source_table_reference,
                                              target_table_reference):
        if not self.__has_dataset_cached(target_table_reference):
            self.__create_target_dataset(source_table_reference,
                                         target_table_reference)

    def __has_dataset_cached(self, target_table_reference):
        return self.BQ.get_dataset_cached(target_table_reference.project_id,
                                          target_table_reference.dataset_id) is not None

    def __create_target_dataset(self, source_table_reference,
                                target_table_reference):
        location = self.BQ.get_dataset_location(
            project_id=source_table_reference.project_id,
            dataset_id=source_table_reference.dataset_id)
        self.BQ.create_dataset(
            project_id=target_table_reference.project_id,
            dataset_id=target_table_reference.dataset_id,
            location=location
        )

    def __create_empty_partitioned_table_if_not_exists(self, source_table_reference, target_table_reference):
        if target_table_reference.is_partition():
            target_table_metadata = BigQueryTableMetadata.get_table_by_reference_cached(target_table_reference)
            if not target_table_metadata.table_exists():
                source_table_metadata = BigQueryTableMetadata.get_table_by_reference_cached(source_table_reference)
                source_table_metadata.create_the_same_empty_table(target_table_reference)
