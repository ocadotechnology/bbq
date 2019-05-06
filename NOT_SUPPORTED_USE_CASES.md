# Rare cases in which backups are not supported by BBQ

In case of some rare circumstances BBQ is not able to verify backup last update time. Such situation results in not creating most up to date backup or creating wrong backup.

## `lastModifiedTime` changed to past value when chunk of data is deleted 

When part of BigQuery table/partition is deleted and deleted part was whole chunk of BigQuery internal storage, then `lastModifiedTime` is max value taken from other chunks which might be earlier than `lastModifiedTime` of deleted chunk.

#### Prerequisites
* Part of data needs to be deleted
* Deleted part is whole BigQuery internal chunk of data
* There is no further changes to the partition

#### Result
* New backup is not created. BBQ stores previous version which includes deleted data

## Data stored in [__UNPARTITIONED__ partition](https://cloud.google.com/bigquery/docs/querying-partitioned-tables#ingestion-time_partitioned_tables_unpartitioned_partition) for a long time

When data is slowly streamed to BigQuery partitioned table, then that data might be moved from `__UNPARTITIONED__` partition into correct one after several hours. `lastModifiedTime` is set to time of streaming (which might be several hours ago), not when data was moved between partitions. This results in not backing up new part of data, because BBQ looks at time of last backup.

#### Prerequisites
* Ingestion-time partitioned table
* Data is streamed to the BigQuery without specifying `partitionId`
* Ingestion is very slow so that `__UNPARTITIONED__` partition store data for more than 24 hours
* There is no further changes to the partition

#### Result
* Part of new data is not backed up

## Backing up empty partition

Due to asynchronous nature of scheduling backup for table/partition it is possible that: 
1. Source data is modified and backup is scheduled for given table/partition
1. Between scheduling copy-job task and this task execution, the data is deleted manually or by partition expiration
1. The newest version of backup is empty, the second newest has proper data.

#### Prerequisites
* Backup is scheduled for given table/partition, i.e. `lastModifiedTime` is modified
* Between scheduling copy-job task and this task execution, the data is deleted manually or by partition expiration

#### Result
* Backup for given table/partition (if exists) will be deleted after 7 months, as only the most recent empty backup will be retained 
