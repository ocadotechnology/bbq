resource "google_bigquery_dataset" "SLI_history_dataset" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  location = "${var.SLI_views_location}"

  labels {"bbq_metadata"=""}
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
    query = "SELECT * FROM [${var.SLI_views_destination_project}:${var.SLI_history_dataset}.SLI_backup_creation_latency]"
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_backup_creation_latency"]
}

resource "google_bigquery_table" "SLI_backup_creation_latency_by_count_view" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_creation_latency_by_count_view"

  view {
    query = "SELECT xDays, count(*) as count FROM [${var.SLI_history_dataset}.SLI_backup_creation_latency_view] GROUP BY xDays"
    use_legacy_sql = true
  }

  depends_on = ["google_bigquery_table.SLI_backup_creation_latency_view"]
}