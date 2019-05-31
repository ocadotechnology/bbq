import logging

from src.backup.task_creator import TaskCreator


class TablePartitionsBackupScheduler(object):
    def __init__(self, table_reference, big_query):
        self.table_reference = table_reference
        self.big_query = big_query

    def start(self):
        partitions = self.big_query \
            .list_table_partitions(self.table_reference.get_project_id(),
                                   self.table_reference.get_dataset_id(),
                                   self.table_reference.get_table_id())
        if not partitions:
            logging.info("Table %s doesn't contain any partitions",
                         self.table_reference)

        TaskCreator.schedule_tasks_for_partition_backup(
            self.table_reference.get_project_id(),
            self.table_reference.get_dataset_id(),
            self.table_reference.get_table_id(),
            [partition['partitionId'] for partition in partitions])
