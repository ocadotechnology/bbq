terraform {
  required_version = "~> 0.11"

  backend "gcs" {
    bucket = {}
    prefix = "/terraform-bbq.tfstate"
  }
}


provider "google" {
  version = "~> 2"
}

variable "bbq_project" {
  description = "it is needed to filter out backups from SLI"
}
