[![Build Status](https://travis-ci.org/ocadotechnology/bbq.svg?branch=master)](https://travis-ci.org/ocadotechnology/bbq)
[![Coverage Status](https://coveralls.io/repos/github/ocadotechnology/bbq/badge.svg?branch=master)](https://coveralls.io/github/ocadotechnology/bbq?branch=master)

# Backup Big Query (BBQ)

BBQ (read: barbecue) is a python app that runs on GAE and creates daily backups of BigQuery tables.

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
* Data in table streaming buffer will be backed up on the next run, once the buffer is flushed. BBQ uses [copy-job](https://cloud.google.com/bigquery/docs/managing-tables#copy-table) for creating backups and *"Records in the streaming buffer are not considered when a copy or extract job runs"* ([Life of a BigQuery streaming insert](https://cloud.google.com/blog/big-data/2017/06/life-of-a-bigquery-streaming-insert)). 

# High level architecture

![Architecture diagram](docs/images/bbq-architecture-diagram.png)

BBQ consists of:
- multiple source projects - BBQ backups data from them,
- BBQ project - main project where GAE runs and backups are stored,
- Restoration project - into which data is restored.

BBQ allows to perform 3 operations:
- backups - create backup tables of source tables,
- restore - copies selected backup data into restore project. 
- retention - prunes backups based on selected rules,

BBQ is using Datastore as the main database.

## Backup process

![Backup process](docs/images/bbq_backup_process.gif)

BBQ initially creates backups for all source tables, to which it has access. If source table will be modified, BBQ will create a backup within 36 hours. 
Backups are created using [copy-job](https://cloud.google.com/bigquery/docs/managing-tables#copy-table) in the same location as source data. 

BBQ can hold multiple versions of the same source table.
Every partitioned table is treated as a separate table (i.e. BBQ copies only modified partitions). When source table has expiration time set, it's cleared from the backup (so that backup won't expire automatically).

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

## How to run backups
Backup process is scheduled periodically for all specified projects.
 * Check [config.yaml](./config/config.yaml) to specify which projects to backup and [cron.yaml](cron.yaml) to configure schedule time.

However, you may also invoke backup process manually in several ways:
* Enforce cron start **GET** *\<**your-project-id**\>.appspot.com/cron/backup*
* Execute task to backup specific dataset **POST** *\<**your-project-id**\>.appsport.com/tasks/backups/dataset?projectId=<**project-id**>&datasetId=<**dataset-id**>*
