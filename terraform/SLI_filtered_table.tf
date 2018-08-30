resource "google_bigquery_table" "SLI_filtered_table" {
  project = "${var.slos_views_destination_project}"
  dataset_id = "${var.SLO_views_legacy_dataset}"
  table_id = "SLI_filtered"

  time_partitioning {
    type = "DAY"
  }

  schema= "${file("SLI_filtered_table_schema.json")}"
}
