from datetime import datetime

from google.appengine.ext import ndb

from src.backup.datastore.Table import Table
from tests.utils import backup_utils


# @refactor order the method order according to our convention
def create_multiple_table_entities(quantity, project_id, partition_id, dataset_id='example-dataset-name'):
    tables = []
    for i in range(1, quantity + 1):
        table = Table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='example-table-name-{}'.format(i),
            partition_id=partition_id,
            last_checked=datetime(2017, 12, 5)
        )
        tables.append(table)
    ndb.put_multi(tables)


def create_and_insert_table_with_one_backup(project_id, dataset_id, table_id,
                                            date, partition_id=None):
    table = Table(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        partition_id=partition_id,
        last_checked=date
    )

    table.put()
    backup_utils.create_backup(date, table,
                               table_id + date.strftime('%Y%m%d')).put()

    return table
