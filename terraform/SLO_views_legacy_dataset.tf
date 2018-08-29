resource "google_bigquery_dataset" "SLO_views_legacy_dataset" {
  dataset_id = "${var.SLO_views_legacy}"
  project = "${var.slos_views_destination_project}"
  location = "EU"
}