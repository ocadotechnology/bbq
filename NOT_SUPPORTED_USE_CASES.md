# Rare cases in which backups are not supported by BBQ

In case of some rare circumstances BBQ is not able to verify backup last update time. Such situation results in not creating most up to date backup or creating wrong backup.

## `lastModifiedTime` changed to past value when chunk of data is deleted 

When part of BigQuery table/partition is deleted and deleted part was whole chunk of BigQuery internal storeage, then `lastModifiedTime` is max value taken from other chunks which might be earlier than `lastModifiedTime` of deleted chunk.

#### Prerequisites
* Part of data needs to be deleted
* Deleted part is whole BigQuery internal chunk of data
* There is no further changes to the partition

#### Result
* New backup is not created. BBQ stores previous version which includes deleted data

## Data stored in [__UNPARTITIONED__ partition](https://cloud.google.com/bigquery/docs/querying-partitioned-tables#ingestion-time_partitioned_tables_unpartitioned_partition) for a long time

When data is slowly streamed to BigQuery partitioned table to partitions outside of the 10 day window (7 day past and 3 day ahead), then that data might be moved from `__UNPARTITIONED__` partition into correct one after several hours. `lastModifiedTime` is set to time of streaming (which might be several hours ago), not when data was moved between partitions. This results in not backing up new part of data, because BBQ looks at time of last backup.

#### Prerequisites
* Ingestion-time partitioned table
* Data is streamed to the BigQuery without specifying `partitionId` or outside of the 10 day window (7 day past and 3 day ahead)
* Ingestion is very slow so that `__UNPARTITIONED__` partition store data for more than 24 hours
* There is no further changes to the partition

#### Result
* Part of new data is not backed up

## Backing up empty partition just after expiration

Due to asynchronous nature of scheduling backup for partition it is possible that: 
1. Backup scheduler queries for partition will get given partition. 
2. Partition is removed.
3. Backup of partition task is run because it is scheduled before partition removal. 
4. Google returns information 200 during `table.get`, but partition is empty (0 bytes) - if `lastModifiedTime` is newer than lastBackup (very rare case as `lastModifiedTime` of empty partition is currently time of last modification whole table metadata(eg. description etc.)) then backup will be performed.
5. The newest version of backup is empty, the second newest has proper data.

#### Prerequisites
* Partitioned table with partition expiration set
* Partition expires between listing partitions and running task for backup (this takes up to 7 hours)
* Whole table (metadata, other partitions) are updated in meantime

#### Result
* Backup for given partition (if exists) will be deleted after 7 months, as only the most recent empty backup will be retained 
