resource "google_bigquery_dataset" "SLI_history_legacy_dataset" {
  project = "${var.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_legacy_dataset}"
  location = "${var.SLI_views_location}"
}