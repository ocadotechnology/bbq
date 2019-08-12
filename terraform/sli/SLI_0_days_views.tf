resource "google_bigquery_table" "data_modified_0_days_ago_view" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_backup_creation_latency_views_dataset.dataset_id
  table_id = "data_modified_0_days_ago"
  description = "All tables which have modifications before 0 days ago"

  view {
    query = <<EOF
            #legacySQL
              --  Shows all tables which have modifications before 0 days ago
            SELECT * FROM (
              SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, numRows
              FROM (
                SELECT
                  projectId, datasetId, tableId, creationTime, lastModifiedTime, 'None' AS partitionId, numRows,
                  ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
                FROM
                  [${var.gcp_census_project}.bigquery.table_metadata_v1_0]
                WHERE
                  _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(NOW(), -2, "DAY")) AND NOW()
                  AND lastModifiedTime <= NOW()
                  AND timePartitioning.type IS NULL AND type='TABLE'
              )
              WHERE rownum = 1
            ), (
              SELECT projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, numRows
              FROM (
                SELECT
                  projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, numRows,
                    ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId, partitionId ORDER BY snapshotTime DESC) AS rownum
                  FROM
                    [${var.gcp_census_project}.bigquery.partition_metadata_v1_0]
                  WHERE
                  _PARTITIONTIME BETWEEN TIMESTAMP(DATE_ADD(NOW(), -2, "DAY")) AND NOW()
                  AND lastModifiedTime <= NOW()
              )
              WHERE rownum = 1
            )
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "SLI_0_days_view" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_backup_creation_latency_views_dataset.dataset_id
  table_id = "SLI_0_days"
  description = "All tables and partitions which backups potentially violate 0 days latency"

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
            [${google_bigquery_table.data_modified_0_days_ago_view.id}] as census
          LEFT JOIN (
            SELECT
              backup_created, backup_last_modified, source_project_id, source_dataset_id, source_table_id, source_partition_id
            FROM
              [${google_bigquery_table.last_available_backup_for_every_table_entity_view.id}]
          ) AS last_backups
          ON
            census.projectId=last_backups.source_project_id AND
            census.datasetId=last_backups.source_dataset_id AND
            census.tableId=last_backups.source_table_id AND
            census.partitionId=last_backups.source_partition_id
          WHERE
            projectId != "${var.bbq_project}"
            AND projectId != "${var.bbq_restoration_project}"
            AND partitionId != "__UNPARTITIONED__"
            AND census.numRows != 0
            AND IFNULL(last_backups.backup_created, MSEC_TO_TIMESTAMP(0)) < CURRENT_TIMESTAMP()
            AND IFNULL(last_backups.backup_last_modified, MSEC_TO_TIMESTAMP(0)) < lastModifiedTime
        EOF
    use_legacy_sql = true
  }
}