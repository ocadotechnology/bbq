resource "google_bigquery_table" "census_data_4_days_ago_view" {
  project = "${var.SLI_views_destination_project}"
  dataset_id = "${var.SLI_views_legacy_dataset}"
  table_id = "census_data_4_days_ago"

  view {
    query = <<EOF
            #legacySQL
              -- Shows all tables and partitions seen by census 4 days ago
            SELECT * FROM (
              SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime
              FROM (
                SELECT
                  projectId, datasetId, tableId, creationTime, lastModifiedTime, 'null' AS partitionId,
                  ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
                FROM
                  [${var.census_project}.bigquery.table_metadata_v1_0]
                WHERE
                  _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -7, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -4, "DAY"))
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
                    [${var.census_project}.bigquery.partition_metadata_v1_0]
                  WHERE
                    _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -7, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -4, "DAY"))
              )
              WHERE rownum = 1
            )
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_dataset.SLI_views_legacy_dataset_dataset"]
}


resource "google_bigquery_table" "SLI_4_days_view" {
  project = "${var.SLI_views_destination_project}"
  dataset_id = "${var.SLI_views_legacy_dataset}"
  table_id = "SLI_4_days"

  view {
    query = <<EOF
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
            [${var.SLI_views_destination_project}.${var.SLI_views_legacy_dataset}.census_data_4_days_ago] AS census
          LEFT JOIN (
            SELECT
              backup_created, backup_last_modified, source_project_id, source_dataset_id, source_table_id, source_partition_id
            FROM
              [${var.datastore_export_project}.${var.datastore_export_views_dataset}.last_available_backup_for_every_table_entity]
          ) AS last_backups
          ON
            census.projectId=last_backups.source_project_id AND
            census.datasetId=last_backups.source_dataset_id AND
            census.tableId=last_backups.source_table_id AND
            census.partitionId=last_backups.source_partition_id
          WHERE
            projectId != "${var.bbq_project}"
            AND backup_created < TIMESTAMP(DATE_ADD(CURRENT_TIMESTAMP(), -4 , "DAY"))
            AND backup_last_modified < lastModifiedTime
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.census_data_4_days_ago_view", "google_bigquery_table.last_available_backup_for_every_table_entity_view"]
}