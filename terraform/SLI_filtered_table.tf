resource "google_bigquery_table" "SLI_filtered_table" {
  project = "${var.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  table_id = "SLI_backup_creation_latency"

  time_partitioning {
    type = "DAY"
  }

  schema= "${file("SLI_filtered_table_schema.json")}"

  depends_on = ["google_bigquery_dataset.SLI_history_dataset"]
}
