provider "google" {
  version = "1.16"
}

variable "bbq_project" {
  description = "it is needed to filter out backups from SLI"
}

variable "SLI_views_destination_project" {
  description = "all SLI views will be created in this project"
}

variable "census_project" {
  description = "project where GCP Census data resides. More specifically we need bigquery.table_metadata_v1_0 and bigquery.partition_metadata_v1_0 table from that project"
}

variable "datastore_export_project" {
  description = "project where datastore export tables can be get from (also datastore export views will be stored here)"
}

variable "datastore_export_dataset" {
  description = "dataset in project {var.datastore_export_project} where datastore export tables can be get from"
  default = "datastore_export"
}

variable "datastore_export_views_dataset" {
  description = "datastore export views will be stored here"
  default = "datastore_export_views_legacy"
}

variable "SLI_views_legacy_dataset" {
  default = "SLI_views_legacy"
}

variable "SLI_history_legacy_dataset" {
  default = "SLI_history_legacy"
}

variable "SLI_views_location" {
  default = "EU"
}