variable "bbq_project" {
  description = "it is needed to filter out backups from SLI"
}

variable "bbq_metadata_project" {
  description = "it is needed to export datastore backups from GCS to BQ"
}

variable "bbq_restoration_project" {
  description = "it is needed to filter out restored backups from SLI"
}

variable "gcp_census_project" {
  description = "project where GCP Census data resides. More specifically we need bigquery.table_metadata_v1_0 and bigquery.partition_metadata_v1_0 table from that project"
}