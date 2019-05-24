resource "google_bigquery_dataset" "datastore_export_dataset" {
  dataset_id = "${var.datastore_export_dataset}"
  project = "${local.datastore_export_project}"
  location = "${var.SLI_views_location}"

  labels {"bbq_metadata"=""}

  access {
    role   = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role   = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role   = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_table" "datastore_export_backup_kind_table" {
  project = "${local.datastore_export_project}"
  dataset_id = "${google_bigquery_dataset.datastore_export_dataset.dataset_id}"
  table_id = "Backup_"
  schema= "${file("${path.module}/datastore_export_backup_kind_table_schema.json")}"
}

resource "google_bigquery_table" "datastore_export_table_kind_table" {
  project = "${local.datastore_export_project}"
  dataset_id = "${google_bigquery_dataset.datastore_export_dataset.dataset_id}"
  table_id = "Table_"
  schema= "${file("${path.module}/datastore_export_table_kind_table_schema.json")}"
}