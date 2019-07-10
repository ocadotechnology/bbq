variable "bbq_project" {
  description = "it is needed to filter out backups from SLI"
}

variable "bbq_restoration_project" {
  description = "it is needed to filter out restored backups from SLI"
}

variable "bbq_metadata_project" {
  description = "storage project for bbq metadata"
}

variable "gcp_census_project" {
  description = "project where GCP Census data resides. More specifically we need bigquery.table_metadata_v1_0 and bigquery.partition_metadata_v1_0 table from that project"
}

variable "SLI_views_destination_project" {
  description = "all SLI views will be created in this project"
  default     = ""
}

variable "datastore_export_project" {
  description = "project where datastore export tables can be get from (also datastore export views will be stored here)"
  default     = ""
}

locals {
  datastore_export_project      = var.datastore_export_project != "" ? var.datastore_export_project : var.bbq_metadata_project
  SLI_views_destination_project = var.SLI_views_destination_project != "" ? var.SLI_views_destination_project : var.bbq_metadata_project
  one_year_in_ms                = 31536000000
}

variable "datastore_export_dataset" {
  description = "dataset in project {local.datastore_export_project} where datastore export tables can be get from"
  default     = "datastore_export"
}

variable "datastore_export_views_dataset" {
  description = "datastore export views will be stored here"
  default     = "datastore_export_views_legacy"
}

variable "SLI_backup_creation_latency_views_dataset" {
  default = "SLI_backup_creation_latency_views"
}

variable "SLI_backup_quality_views_dataset" {
  default = "SLI_backup_quality_views"
}

variable "SLI_history_dataset" {
  default = "SLI_history"
}

variable "SLI_views_location" {
  default = "EU"
}

variable "orhpaned_backups_views_dataset" {
  default = "orphaned_backups_views"
}