resource "google_bigquery_dataset" "SLI_history_dataset" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  location = "${var.SLI_views_location}"

  labels {"bbq_metadata"=""}

  access {
    role   = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role   = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role   = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_table" "SLI_backup_creation_latency" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_creation_latency"

  time_partitioning {
    type = "DAY"
  }

  schema= "${file("SLI_backup_creation_latency_filtered_table_schema.json")}"

  depends_on = ["google_bigquery_dataset.SLI_history_dataset"]
}

resource "google_bigquery_table" "SLI_backup_creation_latency_view" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_creation_latency_view"

  view {
 query = <<EOF
          #legacySQL
          SELECT snapshotTime, projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, backupCreated, backupLastModified, xDays
          FROM (
            SELECT snapshotTime, projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, backupCreated, backupLastModified, xDays,
            DENSE_RANK() OVER (PARTITION BY xDays ORDER BY snapshotTime DESC ) AS newestSnapshotRank
            FROM [${local.SLI_views_destination_project}:${var.SLI_history_dataset}.SLI_backup_creation_latency]
            WHERE _PARTITIONTIME=TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))
          ) WHERE newestSnapshotRank=1 and projectId!='SNAPSHOT_MARKER'
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_backup_creation_latency"]
}

resource "google_bigquery_table" "SLI_backup_creation_latency_by_count_view" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_creation_latency_by_count_view"

  view {
    query = "SELECT INTEGER(xDays) as xDays, count(*) as count FROM [${var.SLI_history_dataset}.SLI_backup_creation_latency_view] GROUP BY xDays"
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_backup_creation_latency_view"]
}

resource "google_bigquery_table" "SLI_backup_quality" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_quality"

  time_partitioning {
    type = "DAY"
  }

  schema= "${file("SLI_backup_quality_filtered_table_schema.json")}"

  depends_on = ["google_bigquery_dataset.SLI_history_dataset"]
}

resource "google_bigquery_table" "SLI_backup_quality_view" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_quality_view"

  view {
 query = <<EOF
          #legacySQL
          SELECT snapshotTime, projectId, datasetId, tableId, partitionId, backupDatasetId, backupTableId, lastModifiedTime, backupLastModifiedTime, backupEntityLastModifiedTime, numBytes, backupNumBytes, backupEntityNumBytes, numRows, backupNumRows
          FROM (
            SELECT snapshotTime, projectId, datasetId, tableId, partitionId, backupDatasetId, backupTableId, lastModifiedTime, backupLastModifiedTime, backupEntityLastModifiedTime, numBytes, backupNumBytes, backupEntityNumBytes, numRows, backupNumRows,
            DENSE_RANK() OVER (ORDER BY snapshotTime DESC ) AS newestSnapshotRank
            FROM [${local.SLI_views_destination_project}:${var.SLI_history_dataset}.SLI_backup_quality]
            WHERE _PARTITIONTIME=TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))
          ) WHERE newestSnapshotRank=1 and projectId!='SNAPSHOT_MARKER'
        EOF
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_backup_quality"]
}