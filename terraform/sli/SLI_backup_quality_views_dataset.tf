resource "google_bigquery_dataset" "SLI_backup_quality_views_dataset" {
  dataset_id = var.SLI_backup_quality_views_dataset
  project = local.SLI_views_destination_project
  location = var.SLI_views_location
  description = "Contain views to calculate Backup Quality SLI that shows all tables which last backup differs in numRows or numBytes."

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