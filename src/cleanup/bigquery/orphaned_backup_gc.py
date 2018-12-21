import logging

from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class OrphanedBackupGC(object):
    def __init__(self):
        super(OrphanedBackupGC, self).__init__()
        self.big_query = BigQuery()

    def cleanup_orphaned_backups(self):
        query = self.big_query.create_orphaned_backups_query(configuration.backup_project_id)
        results = self.big_query.execute_query(query)
        logging.info('cleanup orphaned backups query finished with success.'
                     ' Start of deletion found tables is about to begin.')
        formatted_results = self.__format_query_results(results)
        self.__delete_orphaned_tables(formatted_results)
        logging.info('finished cleanup process of orphaned backups')

    def __delete_orphaned_tables(self, formatted_results):
        [self.big_query.delete_table(table_ref) for table_ref in formatted_results]

    def __format_query_results(self, results):
        formatted_results = [TableReference(result['f'][2]['v'],
                                            result['f'][1]['v'],
                                            result['f'][0]['v']
                                            ) for result in results]
        return formatted_results
