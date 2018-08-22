resource "google_bigquery_dataset" "SLO_views_legacy_dataset" {
  dataset_id = "SLO_views_legacy"
  project = "${var.bbq_project}"
  location = "EU"
}