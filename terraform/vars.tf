variable "bbq_project" {description = "it is needed to filter out backups from SLI"}
variable "slos_views_destination_project" {description = "all SLO views will be created in this view"}
variable "census_project" {description = "project where census data resides. More specifically we need bigquery.table_metadata_v1_0 and bigquery.partition_metadata_v1_0 table from that project"}
variable "datastore_export_project" {description = "project where datastore export tables can be get from (also datastore export views will be stored here)"}
variable "datastore_export_dataset" {description = "dataset in project {var.datastore_export_project} where datastore export tables can be get from (also datastore export views will be stored here)"}
variable "datastore_export_views_legacy" {default = "datastore_export_views_legacy"}
variable "SLO_views_legacy" {default = "SLO_views_legacy"}
variable "SLO_views_location" {default = "EU"}