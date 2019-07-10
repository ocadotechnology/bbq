resource "google_bigquery_table" "tables_not_modified_since_3_days" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_backup_quality_views_dataset.dataset_id
  table_id = "tables_not_modified_since_3_days"

  view {
    query = <<EOF
            #legacySQL
            SELECT projectId, datasetId, tableId, partitionId, lastModifiedTime, numBytes, numRows FROM (
              SELECT * FROM (
                SELECT
                  projectId, datasetId, tableId, 'None' AS partitionId, lastModifiedTime, numBytes, numRows,
                  ROW_NUMBER() OVER (PARTITION BY projectId, datasetId, tableId ORDER BY snapshotTime DESC) AS rownum
                FROM [${var.gcp_census_project}.bigquery.table_metadata_v1_0]
                WHERE
                  _PARTITIONTIME BETWEEN TIMESTAMP(UTC_USEC_TO_DAY(NOW() - 3 * 24 * 60 * 60 * 1000000)) AND TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))
                  AND timePartitioning.type IS NULL AND type='TABLE'
              )
              WHERE
                rownum=1 AND
                DATEDIFF(CURRENT_TIMESTAMP(), lastModifiedTime) >= 3 AND
                projectId != "${var.bbq_project}" AND
                projectId != "${var.bbq_restoration_project}"
            ), (
              SELECT projectId, datasetId, tableId, partitionId, lastModifiedTime, numBytes, numRows
              FROM [${var.gcp_census_project}.bigquery_views_legacy_sql.partition_metadata_v1_0]
              WHERE DATEDIFF(CURRENT_TIMESTAMP(), lastModifiedTime) >= 3 AND projectId != "${var.bbq_project}"
            )
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "last_backup_in_census" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_backup_quality_views_dataset.dataset_id
  table_id = "last_backup_in_census"

  view {
    query = <<EOF
            #legacySQL
              -- Return last available backup in GCP Census for every table entity from Datastore
            SELECT
              last_backup.source_project_id AS source_project_id,
              last_backup.source_dataset_id AS source_dataset_id,
              last_backup.source_table_id AS source_table_id,
              last_backup.source_partition_id AS source_partition_id,
              last_backup.backup_last_modified AS backup_entity_last_modified_time,
              last_backup.backup_num_bytes AS backup_entity_num_bytes,
              last_backup.backup_dataset_id AS backup_dataset_id,
              last_backup.backup_table_id AS backup_table_id,
              census.lastModifiedTime as backup_last_modified,
              census.numBytes AS backup_num_bytes,
              census.numRows AS backup_num_rows
            FROM
              [${google_bigquery_table.last_available_backup_for_every_table_entity_view.id}]
            AS last_backup
            LEFT OUTER JOIN (
              SELECT datasetId, tableId, lastModifiedTime, numBytes, numRows
              FROM [${var.gcp_census_project}.bigquery_views_legacy_sql.table_metadata_v1_0]
              WHERE projectId = "${var.bbq_project}"
            ) AS census
            ON census.datasetId=last_backup.backup_dataset_id AND census.tableId=last_backup.backup_table_id
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "SLI_quality" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_backup_quality_views_dataset.dataset_id
  table_id = "SLI_quality"

  view {
    query = <<EOF
            #legacySQL
              -- Shows all tables which last backup differs in numRows or numBytes
            SELECT
              source_table.projectId AS source_project_id,
              source_table.datasetId AS source_dataset_id,
              source_table.tableId AS source_table_id,
              source_table.partitionId AS source_partition_id,
              last_backup_in_census.backup_dataset_id AS backup_dataset_id,
              last_backup_in_census.backup_table_id AS backup_table_id,
              source_table.lastModifiedTime AS source_last_modified,
              last_backup_in_census.backup_last_modified AS backup_last_modified,
              last_backup_in_census.backup_entity_last_modified_time AS backup_entity_last_modified,
              source_table.numBytes AS source_num_bytes,
              last_backup_in_census.backup_num_bytes AS backup_num_bytes,
              last_backup_in_census.backup_entity_num_bytes AS backup_entity_num_bytes,
              source_table.numRows AS source_num_rows,
              last_backup_in_census.backup_num_rows AS backup_num_rows
            FROM
              [${google_bigquery_table.tables_not_modified_since_3_days.id}] AS source_table
            LEFT JOIN (
              SELECT
                source_project_id,
                source_dataset_id,
                source_table_id,
                source_partition_id,
                backup_dataset_id,
                backup_table_id,
                backup_last_modified,
                backup_entity_last_modified_time,
                backup_num_bytes,
                backup_entity_num_bytes,
                backup_num_rows
              FROM
                [${google_bigquery_table.last_backup_in_census.id}] ) AS last_backup_in_census
            ON
              source_table.projectId=last_backup_in_census.source_project_id
              AND source_table.datasetId=last_backup_in_census.source_dataset_id
              AND source_table.tableId=last_backup_in_census.source_table_id
              AND source_table.partitionId=last_backup_in_census.source_partition_id
            WHERE
              DATEDIFF(CURRENT_TIMESTAMP(), backup_entity_last_modified_time)>=3
              AND last_backup_in_census.backup_table_id IS NOT NULL
              AND source_partition_id != "__UNPARTITIONED__"
              AND (source_table.numBytes != last_backup_in_census.backup_num_bytes
                   OR source_table.numRows != last_backup_in_census.backup_num_rows
                   OR last_backup_in_census.backup_num_bytes IS NULL)
              AND source_table.numRows != 0
        EOF
    use_legacy_sql = true
  }
}