resource "google_bigquery_table" "census_data_5_days_ago_view" {
  project = "${var.slos_views_destination_project}"
  dataset_id = "${var.SLO_views_legacy}"
  table_id = "census_data_5_days_ago"

  view {
    query = <<EOF
            #legacySQL
              -- Shows all tables and partitions seen by census 5 days ago
            SELECT * FROM (
              SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime
              FROM (
                SELECT
                  projectId, datasetId, tableId, creationTime, lastModifiedTime, 'null' AS partitionId,
                  ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
                FROM
                  [${var.census_project}.bigquery.table_metadata_v1_0]
                WHERE
                  _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -8, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -5, "DAY"))
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
                    _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(CURRENT_DATE(), -8, "DAY")) AND TIMESTAMP(DATE_ADD(CURRENT_DATE(), -5, "DAY"))
              )
              WHERE rownum = 1
            )
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_dataset.SLO_views_legacy_dataset"]
}


resource "google_bigquery_table" "SLI_5_days_view" {
  project = "${var.slos_views_destination_project}"
  dataset_id = "${var.SLO_views_legacy}"
  table_id = "SLI_5_days"

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
            [${var.slos_views_destination_project}.${var.SLO_views_legacy}.census_data_5_days_ago] AS census
          LEFT JOIN (
            SELECT
              backup_created, backup_last_modified, source_project_id, source_dataset_id, source_table_id, source_partition_id
            FROM
              [${var.datastore_export_project}.${var.datastore_export_views_legacy}.last_available_backup_for_every_table_entity]
          ) AS last_backups
          ON
            census.projectId=last_backups.source_project_id AND
            census.datasetId=last_backups.source_dataset_id AND
            census.tableId=last_backups.source_table_id AND
            census.partitionId=last_backups.source_partition_id
          WHERE
            projectId != "${var.bbq_project}"
            AND backup_created < TIMESTAMP(DATE_ADD(CURRENT_TIMESTAMP(), -5 , "DAY"))
            AND backup_last_modified < lastModifiedTime
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.census_data_5_days_ago_view", "google_bigquery_table.last_available_backup_for_every_table_entity_view"]
}