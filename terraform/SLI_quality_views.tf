resource "google_bigquery_table" "tables_modified_more_than_3_days_ago" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_backup_quality_views_dataset}"
  table_id = "tables_modified_more_than_3_days_ago"

  view {
    query = <<EOF
            #legacySQL
            SELECT projectId, datasetId, tableId, partitionId, lastModifiedTime, numBytes, numRows FROM (
              SELECT projectId, datasetId, tableId, 'null' AS partitionId, lastModifiedTime, numBytes, numRows
              FROM [${var.gcp_census_project}.bigquery_views_legacy_sql.table_metadata_v1_0]
              WHERE DATEDIFF(CURRENT_TIMESTAMP(), lastModifiedTime) >= 3 AND projectId != "${var.bbq_project}"
              ), (
              SELECT projectId, datasetId, tableId, partitionId, lastModifiedTime, numBytes, numRows
              FROM [${var.gcp_census_project}.bigquery_views_legacy_sql.partition_metadata_v1_0]
              WHERE DATEDIFF(CURRENT_TIMESTAMP(), lastModifiedTime) >= 3 AND projectId != "${var.bbq_project}"
            )
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_dataset.SLI_backup_quality_views_dataset"]
}

resource "google_bigquery_table" "last_backup_in_census" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_backup_quality_views_dataset}"
  table_id = "last_backup_in_census"

  view {
    query = <<EOF
            #legacySQL
            SELECT
              last_backup.source_project_id AS source_project_id,
              last_backup.source_dataset_id AS source_dataset_id,
              last_backup.source_table_id AS source_table_id,
              last_backup.source_partition_id AS source_partition_id,
              census.datasetId AS backup_dataset_id,
              census.tableId AS backup_table_id,
              census.lastModifiedTime as backup_last_modified,
              census.numBytes AS backup_num_bytes,
              census.numRows AS backup_num_rows
            FROM
              [${var.bbq_project}.datastore_export_views_legacy.last_available_backup_for_every_table_entity]
            AS last_backup
            LEFT OUTER JOIN (
              SELECT datasetId, tableId, lastModifiedTime, numBytes, numRows
              FROM [${var.gcp_census_project}.bigquery_views_legacy_sql.table_metadata_v1_0]
              WHERE projectId = "${var.bbq_project}" AND DATEDIFF(CURRENT_TIMESTAMP(), lastModifiedTime) >= 3
            ) AS census
            ON census.datasetId=last_backup.backup_dataset_id AND census.tableId=last_backup.backup_table_id
            WHERE DATEDIFF(CURRENT_TIMESTAMP(), last_backup.backup_last_modified) >= 3
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_dataset.SLI_backup_quality_views_dataset"]
}

resource "google_bigquery_table" "SLI_quality" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_backup_quality_views_dataset}"
  table_id = "SLI_quality"

  view {
    query = <<EOF
            #legacySQL
            SELECT
              source_table.projectId AS source_project_id,
              source_table.datasetId AS source_dataset_id,
              source_table.tableId AS source_table_id,
              source_table.partitionId AS source_partition_id,
              last_backup_in_census.backup_dataset_id AS backup_dataset_id,
              last_backup_in_census.backup_table_id AS backup_table_id,
              source_table.lastModifiedTime AS source_last_modified,
              last_backup_in_census.backup_last_modified AS backup_last_modified,
              source_table.numBytes AS source_num_bytes,
              last_backup_in_census.backup_num_bytes AS backup_num_bytes,
              source_table.numRows AS source_num_rows,
              last_backup_in_census.backup_num_rows AS backup_num_rows
            FROM
              [${local.SLI_views_destination_project}.${var.SLI_backup_quality_views_dataset}.tables_modified_more_than_3_days_ago]
            AS source_table
            LEFT JOIN (
              SELECT
                source_project_id, source_dataset_id, source_table_id, source_partition_id,
                backup_dataset_id, backup_table_id, backup_last_modified, backup_num_bytes, backup_num_rows
              FROM [${local.SLI_views_destination_project}.${var.SLI_backup_quality_views_dataset}.last_backup_in_census]
            ) AS last_backup_in_census
            ON source_table.projectId=last_backup_in_census.source_project_id AND
               source_table.datasetId=last_backup_in_census.source_dataset_id AND
               source_table.tableId=last_backup_in_census.source_table_id AND
               source_table.partitionId=last_backup_in_census.source_partition_id
            WHERE (source_table.numBytes-last_backup_in_census.backup_num_bytes) != 0
               OR (source_table.numRows-last_backup_in_census.backup_num_rows) != 0
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_dataset.SLI_backup_quality_views_dataset"]
}