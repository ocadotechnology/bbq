resource "google_bigquery_dataset" "SLI_history_dataset" {
  project = "${local.SLI_views_destination_project}"
  dataset_id = "${var.SLI_history_dataset}"
  location = "${var.SLI_views_location}"

  labels {"bbq_metadata"=""}
}