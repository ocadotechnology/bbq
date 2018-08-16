import logging
from random import randint

from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class TaskCreator(object):

    @classmethod
    def create_task_for_partition_backup(cls, project_id, dataset_id,
                                         table_id, partition_id):
        init_delay = randint(0, configuration.backup_worker_max_countdown_in_sec)
        logging.info("Schedule_backup_task for partition: '%s/%s/%s$%s'"
                     "will be executed after '%s' seconds.",
                     project_id, dataset_id, table_id, partition_id, init_delay)
        task = Tasks.create(
            method='GET',
            url='/tasks/backups/table/{}/{}/{}/{}'
            .format(project_id, dataset_id, table_id, partition_id),
            countdown=init_delay
        )
        Tasks.schedule('backup-worker', task)
