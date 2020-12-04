import logging
import urllib2

from src.commons.tasks import Tasks


class TaskCreator(object):

    @staticmethod
    def create_organisation_backup_scheduler_task(
        page_token=None):
        logging.debug(
            u'Create Organisation Backup Scheduler task for page_token: %s',
            page_token)

        params = {}
        if page_token:
            params['pageToken'] = page_token

        return Tasks.create(
            method='GET',
            url='/tasks/schedulebackup/organization',
            params=params)

    @staticmethod
    def create_project_backup_scheduler_task(project_id,
        page_token=None):
        logging.debug(
            u'Create Project Backup Scheduler task for %s, page_token: %s',
            project_id, page_token)

        params = {'projectId': project_id}
        if page_token:
            params['pageToken'] = page_token

        return Tasks.create(
            url='/tasks/schedulebackup/project',
            params=params
        )

    @staticmethod
    def create_dataset_backup_scheduler_task(project_id, dataset_id,
        page_token=None):
        logging.debug(
            u'Create Dataset Backup Scheduler task for %s:%s, page_token: %s',
            project_id, dataset_id, page_token)

        params = {'projectId': project_id, 'datasetId': dataset_id}
        if page_token:
            params['pageToken'] = page_token

        return Tasks.create(
            url='/tasks/schedulebackup/dataset',
            params=params)

    @staticmethod
    def create_table_backup_task(project_id, dataset_id, table_id):
        logging.debug(
            u'Create Table Backup task for %s:%s.%s',
            project_id, dataset_id, table_id)
        table_id = urllib2.quote(table_id.encode('UTF-8'))
        return Tasks.create(
            method='GET',
            url=u'/tasks/backups/table/{0}/{1}/{2}'
                .format(project_id, dataset_id, table_id))

    @staticmethod
    def create_partitioned_table_backup_scheduler_task(project_id, dataset_id,
        table_id):
        logging.debug(
            u'Create Partitioned Table Backup Scheduler task for %s:%s.%s',
            project_id, dataset_id, table_id)

        return Tasks.create(
            url='/tasks/schedulebackup/partitionedtable',
            params={
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id
            })

    @staticmethod
    def create_partition_table_backup_task(project_id, dataset_id, table_id,
        partition_id):
        logging.debug(
            u'Create Partition Table Backup task for %s:%s.%s$%s',
            project_id, dataset_id, table_id, partition_id)
        table_id = urllib2.quote(table_id.encode('UTF-8'))
        return Tasks.create(
            method='GET',
            url=u'/tasks/backups/table/{0}/{1}/{2}/{3}'
                .format(project_id, dataset_id, table_id, partition_id))
