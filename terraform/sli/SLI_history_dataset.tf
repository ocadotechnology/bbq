resource "google_bigquery_dataset" "SLI_history_dataset" {
  project = local.SLI_views_destination_project
  dataset_id = var.SLI_history_dataset
  location = var.SLI_views_location

  labels = {
    "bbq_metadata" = ""
  }

  access {
    role = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_table" "SLI_backup_creation_latency" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_history_dataset.dataset_id
  table_id = "SLI_backup_creation_latency"

  time_partitioning {
    type = "DAY"
    expiration_ms = local.one_year_in_ms
  }

  schema = file("${path.module}/SLI_backup_creation_latency_filtered_table_schema.json", )
}

resource "google_bigquery_table" "SLI_backup_creation_latency_view" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_history_dataset.dataset_id
  table_id = "SLI_backup_creation_latency_view"

  view {
    query = <<EOF
          #legacySQL
          SELECT snapshotTime, projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, backupCreated, backupLastModified, xDays
          FROM (
            SELECT snapshotTime, projectId, datasetId, tableId, partitionId, creationTime, lastModifiedTime, backupCreated, backupLastModified, xDays,
            DENSE_RANK() OVER (PARTITION BY xDays ORDER BY snapshotTime DESC ) AS newestSnapshotRank
            FROM [${google_bigquery_table.SLI_backup_creation_latency.id}]
            WHERE _PARTITIONTIME=TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))
          ) WHERE newestSnapshotRank=1 and projectId!='SNAPSHOT_MARKER'
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "SLI_backup_creation_latency_by_count_view" {
  project = local.SLI_views_destination_project
  dataset_id = var.SLI_history_dataset
  table_id = "SLI_backup_creation_latency_by_count_view"

  view {
    query = "SELECT INTEGER(xDays) as xDays, count(*) as count FROM [${google_bigquery_table.SLI_backup_creation_latency_view.id}] GROUP BY xDays"
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "SLI_backup_quality" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_history_dataset.dataset_id
  table_id = "SLI_backup_quality"

  time_partitioning {
    type = "DAY"
    expiration_ms = local.one_year_in_ms
  }

  schema = file(
  "${path.module}/SLI_backup_quality_filtered_table_schema.json",
  )
}

resource "google_bigquery_table" "SLI_backup_quality_view" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_history_dataset.dataset_id
  table_id = "SLI_backup_quality_view"

  view {
    query = <<EOF
          #legacySQL
          SELECT snapshotTime, projectId, datasetId, tableId, partitionId, backupDatasetId, backupTableId, lastModifiedTime, backupLastModifiedTime, backupEntityLastModifiedTime, numBytes, backupNumBytes, backupEntityNumBytes, numRows, backupNumRows
          FROM (
            SELECT snapshotTime, projectId, datasetId, tableId, partitionId, backupDatasetId, backupTableId, lastModifiedTime, backupLastModifiedTime, backupEntityLastModifiedTime, numBytes, backupNumBytes, backupEntityNumBytes, numRows, backupNumRows,
            DENSE_RANK() OVER (ORDER BY snapshotTime DESC ) AS newestSnapshotRank
            FROM [${google_bigquery_table.SLI_backup_quality.id}]
            WHERE _PARTITIONTIME=TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))
          ) WHERE newestSnapshotRank=1 and projectId!='SNAPSHOT_MARKER'
        EOF
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "SLI_backup_latency_last_snapshot_time_view" {
  project = local.SLI_views_destination_project
  dataset_id = google_bigquery_dataset.SLI_history_dataset.dataset_id
  table_id = "SLI_backup_latency_last_snapshot_time"

  view {
    query = "SELECT MAX(snapshotTime) as snapshotTime FROM [${google_bigquery_table.SLI_backup_creation_latency.id}] WHERE _PARTITIONTIME=TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))"
    use_legacy_sql = true
  }
}

resource "google_bigquery_table" "SLI_backup_quality_last_snapshot_time_view" {
  project = local.SLI_views_destination_project
  dataset_id = var.SLI_history_dataset
  table_id = "SLI_backup_quality_last_snapshot_time"

  view {
    query = "SELECT MAX(snapshotTime) as snapshotTime FROM [${google_bigquery_table.SLI_backup_quality.id}] WHERE _PARTITIONTIME=TIMESTAMP(UTC_USEC_TO_DAY(CURRENT_TIMESTAMP()))"
    use_legacy_sql = true
  }
}