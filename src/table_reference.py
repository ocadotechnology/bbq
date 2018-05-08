import re
from src.big_query.big_query_table import BigQueryTable


class TableReference(object):
    def __init__(self, project_id, dataset_id, table_id, partition_id=None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.partition_id = partition_id

    @staticmethod
    def from_table_entity(table):
        return TableReference(table.project_id, table.dataset_id,
                              table.table_id, table.partition_id)

    @staticmethod
    def parse_tab_ref(string):
        TableReference.assure_is_proper_full_table_path(string)

        project_id, dataset_id_table_id = string.split(":")
        dataset_id, table_id = dataset_id_table_id.split(".")
        partition_id = None
        if '$' in table_id:
            table_id, partition_id = table_id.split("$")
        tab_ref = TableReference(project_id, dataset_id, table_id, partition_id)
        return tab_ref

    @staticmethod
    def assure_is_proper_full_table_path(string):
        pattern = re.compile("^[a-zA-Z1-9\-]+:[a-zA-Z1-9_]+.[a-zA-Z1-9_]+(\$\d{8})?$")
        if not pattern.match(string):
            raise Exception("Full table path name (%s) doesn't match following regexp: ^\w+:\w+.\w+(\$\d{8})?$. Human readable pattern is PROJECT_ID:DATASETID.TABLE_ID or PROJECT_ID:DATASETID.TABLE_ID$20180314"%string)

    def get_project_id(self):
        return self.project_id

    def get_dataset_id(self):
        return self.dataset_id

    def get_table_id(self):
        return self.table_id

    def get_partition_id(self):
        return self.partition_id

    def is_partition(self):
        """Checks is this table whole table or just a single partition
        of a table"""
        return self.partition_id is not None

    def create_big_query_table(self):
        return BigQueryTable(self.project_id,
                             self.dataset_id,
                             self.get_table_id_with_partition_id())

    def get_table_id_with_partition_id(self):
        return self.table_id + ('$' + self.partition_id
                                if self.is_partition() else '')

    def __str__(self):
        if self.is_partition():
            return '{0}:{1}.{2}${3}'.format(self.project_id, self.dataset_id,
                                            self.table_id, self.partition_id)
        else:
            return '{0}:{1}.{2}'.format(self.project_id, self.dataset_id,
                                        self.table_id)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, o):
        return type(o) is TableReference \
               and self.project_id == o.project_id \
               and self.dataset_id == o.dataset_id \
               and self.table_id == o.table_id \
               and self.partition_id == o.partition_id

    def __ne__(self, other):
        return not self == other

    def __cmp__(self, other):
        return cmp(self.project_id, other.project_id) or \
               cmp(self.dataset_id, other.dataset_id) or \
               cmp(self.table_id, other.table_id) or \
               cmp(self.partition_id, other.partition_id)
