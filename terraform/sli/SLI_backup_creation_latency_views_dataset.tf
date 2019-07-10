resource "google_bigquery_dataset" "SLI_backup_creation_latency_views_dataset" {
  dataset_id = var.SLI_backup_creation_latency_views_dataset
  project = local.SLI_views_destination_project
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