resource "google_bigquery_dataset" "SLI_views_legacy_dataset" {
  dataset_id = "${var.SLI_views_legacy_dataset}"
  project = "${var.SLI_views_destination_project}"
  location = "${var.SLI_views_location}"

  labels {"bbq_metadata"=""}
}