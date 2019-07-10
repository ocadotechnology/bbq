resource "google_bigquery_dataset" "orphaned_backups_dataset" {
  project = var.bbq_metadata_project
  dataset_id = var.orhpaned_backups_views_dataset
  location = var.SLI_views_location

  labels = {
    "bbq_metadata" = ""
  }

  access {
    role = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role = "WRITER"
    special_group = "projectReaders"
  }
  access {
    role = "OWNER"
    special_group = "projectOwners"
  }
}

resource "google_bigquery_table" "orphaned_backups" {
  project = var.bbq_metadata_project
  dataset_id = var.orhpaned_backups_views_dataset
  table_id = "orphaned_backups_view"

  view {
    query = <<EOF

          SELECT
            tableId,
            datasetId,
            projectId
          FROM (
            SELECT
              tableId,
              datasetId,
              projectId
            FROM
              [${var.gcp_census_project}:bigquery_view.table_metadata_deduplicated_aggregated]
            WHERE
              projectId = "${var.bbq_project}"
              AND tableId LIKE "20%"
              AND lastModifiedTime < DATE_ADD(CURRENT_DATE(), -7, "DAY")
              AND tableId IN (
              SELECT
                backup_table_id
              FROM
                [${google_bigquery_table.all_backups_view.id}]
              WHERE
                backup_deleted IS NOT NULL
                AND backup_deleted < DATE_ADD(CURRENT_DATE(), -7, "DAY"))),
            (
            SELECT
              tableId,
              datasetId,
              projectId
            FROM
              [${var.gcp_census_project}:bigquery_view.table_metadata_deduplicated_aggregated]
            WHERE
              projectId = "${var.bbq_project}"
              AND tableId LIKE "20%"
              AND lastModifiedTime < DATE_ADD(CURRENT_DATE(), -7, "DAY")
              AND tableId NOT IN (
              SELECT
                backup_table_id
              FROM
                [${google_bigquery_table.all_backups_view.id}]))
        EOF
    use_legacy_sql = true
  }
}
