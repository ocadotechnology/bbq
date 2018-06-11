[![Build Status](https://travis-ci.org/ocadotechnology/bbq.svg?branch=master)](https://travis-ci.org/ocadotechnology/bbq)
[![Coverage Status](https://coveralls.io/repos/github/ocadotechnology/bbq/badge.svg?branch=master)](https://coveralls.io/github/ocadotechnology/bbq?branch=master)

# Backup Big Query (BBQ)

BBQ (read: barbecue) is a python app that runs on Google App Engine (GAE) and creates daily backups of BigQuery tables.

# Motivation

[Google BigQuery](https://cloud.google.com/bigquery/) is fast, highly scalable, cost-effective and fully-managed enterprise data warehouse for analytics at any scale. BigQuery automatically replicates data and keeps a 7-day history of changes.

Restoring data from existing table can be done using [snapshot decorators](https://cloud.google.com/bigquery/table-decorators#snapshot_decorators).
However when tables are deleted there are [some limitations](https://cloud.google.com/bigquery/docs/managing-tables#undeletetable): 
> It's possible to restore a table within 2 days of deletion. By leveraging snapshot decorator functionality, one may be able reference a table prior to the deletion event and then copy it. However, there are two primary caveats to creating a reference in this fashion:
> * You cannot reference a deleted table if a table bearing the same ID in the dataset was created after the deletion time.
> * You cannot reference a deleted table if the encapsulating dataset was also deleted/recreated since the table deletion event.

It is common that data streaming solutions require destination resource to be always present. If such resource (dataset or table entity) is deleted, intentionally or not, then default would be to re-create the same, but empty entity.
In such scenario we're not able to restore data using BigQuery build-in features.

### Our motivation for building BBQ was to:
* protect crucial data against application bug, user error or malicious attack,
* be able to restore to multiple versions of our data,
* restore data at scale (i.e. thousands of tables at the same time) which can be part of Disaster Recovery plan.

# Features

### Main BBQ features include:
* highly scalable daily backup of BigQuery tables (hundreds of thousands backups),
  * both single and partitioned tables are supported,
* simple, access-based whitelisting strategy. BBQ will backup tables it has access to via service account,
* retention - automatic deletion of old backups based on age and/or number of versions,
* restore - BBQ can restore:
  * whole datasets,
  * selected tables/partitions/versions.

### BBQ will not backup:
* [external data sources](https://cloud.google.com/bigquery/external-data-sources),
* Views (you can use [GCP Census](https://github.com/ocadotechnology/gcp-census) for that),
* Dataset/table labels as they are not copied by BigQuery copy job (again, you can use [GCP Census](https://github.com/ocadotechnology/gcp-census) for that)

### Known caveats
* Modification of table metadata (including table description) qualifies table to be backed up at the next cycle. It can be a problem for partitioned tables, where such change updates last modified time in every partition. Then BBQ will backup all partitions again, even though there was no actually change in partition data
* There's 10,000 [copy jobs per project per day limit](https://cloud.google.com/bigquery/quotas#copy_jobs), which you may hit on the first day. This limit can be increased by Google Support
* Data in table streaming buffer will be backed up on the next run, once the buffer is flushed. BBQ uses [copy-job](https://cloud.google.com/bigquery/docs/managing-tables#copy-table) for creating backups and *"Records in the streaming buffer are not considered when a copy or extract job runs"* (check [Life of a BigQuery streaming insert](https://cloud.google.com/blog/big-data/2017/06/life-of-a-bigquery-streaming-insert) for more details). 

# High level architecture

![Architecture diagram](docs/images/bbq-architecture-diagram.png)

BBQ consists of:
- multiple source projects - data of those projects will be backed up,
- BBQ project - main project where GAE runs and backups are stored,
- restoration project - into which data is restored.

BBQ allows to perform 3 operations:
- backups - create backup tables of source tables,
- restore - copies selected backup data into restore project. 
- retention - prunes backups based on selected rules,

BBQ is using Datastore as the main database to store all metadata about backups.

## Backup process


BBQ initially creates backups for all source tables, to which it has access. When source table is modified, BBQ will create a backup within 36 hours. 
Backups are created using [copy-job](https://cloud.google.com/bigquery/docs/managing-tables#copy-table) in the same [location/region](https://cloud.google.com/bigquery/docs/dataset-locations) as source data.

### Example of single table backup
![Single table backup process](docs/images/bbq_single_table_backup_process.gif)

BBQ can hold multiple versions of the same source table.
Every partition in partitioned table is treated as separate table (i.e. BBQ copies only modified partitions). If source table has expiration time set, the backup table will not preserve this property (so that backup never expires).

### Example of partitioned table backup
![Partitioned tabke backup process](docs/images/bbq_partitioned_table_backup_process.gif)

## Restore process

Backups are restored in a separate GCP project. BBQ doesn't restore data in the source table for 2 reasons:
* security - BBQ only reads other projects data. It shouldn't have write access to source data, because then single app/user would have write access both to source data and backups. Such situation only increases the risk of losing all data,
* consistency - BBQ doesn't know if restored data should append or replace source data. It's up to the user to finish restoration based on his specific needs.

There are few ways in which you may restore data:
* restoring whole dataset by specifying project and dataset name. All latest versions of tables in this dataset will be restored in restoration project,
* restoring list of backups by specifying which source tables and which versions should be restored.

Restored data will automatically expire in 7 days (target dataset is created with table default expiration).

### Example of restoring selected partitions from partitioned table 
![Restore process](docs/images/bbq_restore_process.gif)

#### Copy job limit

There's 10,000 [copy jobs per project per day limit](https://cloud.google.com/bigquery/quotas#copy_jobs), which you may hit during the restoration. This limit can be increased by Google Support.

Please remember that partitions from partitioned tables are stored as single tables. So if you want to restore dataset, where there was 10 tables with 500 partitions, this results in creating 5000 copy jobs, which is half of total daily quota.

## Retention process  

Every day retention process scans all backups to find and delete backups matching specific criteria within given source table/partition:
* if there are multiple backups per day, the most recent is retained. Multiple backups per day are created in rare cases (e.g. when task queue task is executed more than one time),
* if there are more than 10 backups for given table/partition, the 10 most recent are retained,
* if all backups are older than 7 months, then most recent is retained,
* if there are backups younger than 7 months, then all others are deleted, 
* if source table is deleted, then the last backup is deleted after 7 months.
 
### Example of 10 versions retention
![Retention process](docs/images/bbq_retention_process_10_versions.gif)

### Example of 7 months old backup deletion and source deletion grace period 
![Retention process](docs/images/bbq_retention_process_7_months.gif)

# Setup
To install BBQ in GCP, click button below or follow [Setup.md](./SETUP.md) doc.

<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/ocadotechnology/bbq&page=editor&open_in_editor=SETUP.md">
<img alt="Open in Cloud Shell" src ="http://gstatic.com/cloudssh/images/open-btn.png"></a>

# Usage

## How to run backups?
Backup process is scheduled periodically for all specified projects (check [config/prd/config.yaml](./config/prd/config.yaml) to specify which projects to backup and [config/cron.yaml](./config/cron.yaml) to configure schedule time).

However, you may also invoke backup process manually from [cron jobs](https://console.cloud.google.com/appengine/taskqueues/cron).

It's worth to underline that:
* Backups for partitions are scheduled randomly within the range of time specified in [config.yaml](./config/prd/config.yaml),
* It is possible to check the progress via [Task Queues](https://console.cloud.google.com/appengine/taskqueues).

## How to list already created backups?
In order to find where is stored backup __Y__ for table __X__:
1. In Cloud Console visit [Datastore](https://console.cloud.google.com/datastore),
1. Check __Key literal__ for table _X_:
    * Select __Table__ kind,
    * Filter entities equal to _X.project_id_, _X.dataset_id_, _X.table_id_ or _X.partition_id_,
    * Find table _X_ from results and copy _Key literal_,
1. Query backup _Y_:
    * Select __Backup__ kind,
    * Filter entities by _Key_ that __has ancestor__ _X.Key literal_.

To check the content for given backup __Y__ in Big Query:  
1. Open [Big Query](https://console.cloud.google.com/bigquery),
1. Filter tables by _Y.dataset_id_ or _Y.table_id_ in search bar,
1. Select table and check _Schema_, _Details_ or _Preview_ tab.