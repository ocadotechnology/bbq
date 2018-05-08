from src.backup.task_creator import TaskCreator


class TablePartitionsBackupScheduler(object):
    def __init__(self, table_reference, big_query):
        self.project_id = table_reference.get_project_id()
        self.dataset_id = table_reference.get_dataset_id()
        self.table_id = table_reference.get_table_id()
        self.big_query = big_query

    def start(self):
        partitions = self.big_query.list_table_partitions(self.project_id,
                                                          self.dataset_id,
                                                          self.table_id)
        for partition in partitions:
            TaskCreator.create_task_for_partition_backup(
                self.project_id,
                self.dataset_id,
                self.table_id,
                partition['partitionId'])
