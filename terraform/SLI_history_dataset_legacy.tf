resource "google_bigquery_dataset" "SLI_history_legacy_dataset" {
  project = "${var.slos_views_destination_project}"
  dataset_id = "${var.SLI_history_legacy_dataset}"
  location = "${var.SLO_views_location}"
}