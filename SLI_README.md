# SLI (Service Level Indicators)


### Number of tables which have no backups after X days since last change.  

This metric is measuring number of tables which are not backed up since X days from last observed change. 

To measure it, it is required to have [GCP Census](https://github.com/ocadotechnology/gcp-census) application that collects metadata about tables. 
Also, we need data collected by BBQ in datastore to be exported into BigQuery. That's why we need [cloud datastore exports](https://github.com/ocadotechnology/bbq/blob/master/SETUP.md#cloud-datastore-export) turn on. 

Metric is implemented as a BigQuery query, which uses data about modifications of tables (collected by Census) and data about existing backups. 

All required views to implement that metric are given below, from most low level ones to SLI query at the end.
 
**Note that:**
  * creating proper datasets 
  * change *\<your-project-id-for-BBQ-project\>* to your BBQ project id in each query
  * change *\<your-project-id-for-GCP-Census-project\>* to your GCP Census project id project

**is required.** 

#### \<your-project-id-for-BBQ-project\>:datastore_export_views_legacy.last_table
View shows *Table* entities from newest Datastore backup.
```sql
#legacySQL
SELECT project_id, dataset_id, table_id, partition_id, last_checked, __key__.id AS id 
FROM TABLE_QUERY(
  [<your-project-id-for-BBQ-project>:datastore_export], 
  'table_id=(SELECT MAX(table_id) FROM [<your-project-id-for-BBQ-project>:datastore_export.__TABLES__] WHERE LEFT(table_id, 6) = "Table_")'
)
```

#### \<your-project-id-for-BBQ-project\>:datastore_export_views_legacy.last_backup
View shows *Backup* entities from newest Datastore backup.
```sql
#legacySQL
SELECT created, deleted, last_modified, numBytes, table_id, dataset_id , key, INTEGER(parent_id) as parent_id
FROM(
  SELECT created, deleted, last_modified, numBytes, 
    table_id.text as table_id, 
    dataset_id.text as dataset_id,
    NTH(2, SPLIT(__key__.path, ',')) AS parent_id, 
    TO_BASE64(BYTES(__key__.path)) AS key
  FROM TABLE_QUERY(
    [<your-project-id-for-BBQ-project>:datastore_export], 
    'table_id=(SELECT MAX(table_id) FROM [<your-project-id-for-BBQ-project>:datastore_export.__TABLES__] WHERE LEFT(table_id, 7) = "Backup_")'
  )
)
```

#### \<your-project-id-for-BBQ-project\>:datastore_export_views_legacy.all_backups
View shows joined *Table* and *Backup* pairs from newest Datastore backup.
```sql
#legacySQL
SELECT 
  t.project_id AS source_project_id,
  t.dataset_id AS source_dataset_id,
  t.table_id AS source_table_id,
  t.partition_id AS source_partition_id,
  t.last_checked AS source_table_last_checked,
  t.id AS table_entity_id,
  b.created AS backup_created,
  b.deleted AS backup_deleted,
  b.last_modified as backup_last_modified,
  b.numBytes as backup_num_bytes,
  b.table_id as backup_table_id,
  b.dataset_id as backup_dataset_id,
  b.key AS backupBqKey
FROM [datastore_export_views_legacy.last_backup] AS b
JOIN [datastore_export_views_legacy.last_table] AS t 
ON b.parent_id = t.id
```

#### \<your-project-id-for-BBQ-project\>:datastore_export_views_legacy.last_available_backup_for_every_table_entity
View shows joined *Table* and *Backup* pairs from newest Datastore backup, but only newest backup for each table. 

```sql
#legacySQL
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY table_entity_id order by backup_created DESC) as rownum
  FROM [datastore_export_views_legacy.all_backups]
  WHERE backup_deleted IS NULL
)
WHERE rownum=1
```

#### \<your-project-id-for-BBQ-project\>:SLO_views_legacy.census_data_3_days_ago
View shows all tables and partitions data from GCP Census, in state seen 3 days ago.

```sql 
#legacySQL
  -- Shows all tables and partitions seen by census 3 days ago
SELECT * FROM (
  SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime
  FROM (
    SELECT 
      projectId, datasetId, tableId, creationTime, lastModifiedTime, 'null' AS partitionId,
      ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
    FROM 
      [<your-project-id-for-GCP-Census-project>.bigquery.table_metadata_v1_0]
    WHERE
      _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -6, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -3, "DAY"))
      AND timePartitioning.type IS NULL AND type='TABLE'
  )
  WHERE rownum = 1
), (
  SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime
  FROM (
    SELECT
      projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime,
        ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId, partitionId ORDER BY snapshotTime DESC) AS rownum
      FROM
        [<your-project-id-for-GCP-Census-project>.bigquery.partition_metadata_v1_0]
      WHERE
        _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -6, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -3, "DAY"))
  )
  WHERE rownum = 1
)
```

#### \<your-project-id-for-BBQ-project\>:SLO_views_legacy.SLI_3_days

View shows all tables which still have not had backups, although 3 days ago there was a new, not backed up modification of the table.

```sql
#legacySQL
SELECT 
  census.projectId as projectId, 
  census.datasetId as datasetId, 
  census.tableId as tableId, 
  census.partitionId as partitionId, 
  census.creationTime as creationTime, 
  census.lastModifiedTime as lastModifiedTime,
  IFNULL(last_backups.backup_created, MSEC_TO_TIMESTAMP(0)) as backup_created,
  IFNULL(last_backups.backup_last_modified, MSEC_TO_TIMESTAMP(0)) as backup_last_modified
FROM
  [<your-project-id-for-BBQ-project>.SLO_views_legacy.census_data_3_days_ago] AS census
LEFT JOIN (
  SELECT
    backup_created, backup_last_modified, source_project_id, source_dataset_id, source_table_id, source_partition_id
  FROM
    [<your-project-id-for-BBQ-project>.datastore_export_views_legacy.last_available_backup_for_every_table_entity]
) AS last_backups
ON 
  census.projectId=last_backups.source_project_id AND 
  census.datasetId=last_backups.source_dataset_id AND 
  census.tableId=last_backups.source_table_id AND 
  census.partitionId=last_backups.source_partition_id
WHERE
  projectId != "\<your-project-id-for-BBQ-project\>"
  AND backup_created < TIMESTAMP(DATE_ADD(CURRENT_TIMESTAMP(), -3 , "DAY"))
  AND backup_created < lastModifiedTime
```

To implement above metric another 'X' days, there is only need to implement census_data_X_days_ago and SLI_X_days views. It could be done by simple change parameters in WHERE clause. 