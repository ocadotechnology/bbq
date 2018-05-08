import random
from datetime import datetime, timedelta

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table


def create_backup_daily_sequence(count, table=Table(),
                                 start_date=datetime(2017, 2, 1, 16, 30)):
    backups = []
    for _ in range(count):
        backup = create_backup(start_date, table)
        backups.append(backup)
        start_date += timedelta(days=1)

    return Backup.sort_backups_by_create_time_desc(backups)


def create_backup(created_datetime, table=Table(), table_id=None):
    if not table_id:
        table_id = 'targetTable' + str(random.randint(1, 1000000))
    backup_size = random.randint(1, 1000)
    backup = Backup(
        parent=table.key,
        created=created_datetime,
        dataset_id='targetDataset',
        table_id=table_id,
        numBytes=backup_size
    )
    return backup
