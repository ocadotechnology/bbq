terraform {
  required_version = "> 0.11"

  backend "gcs" {
    bucket = {}
    prefix = "/terraform-bbq-sli.tfstate"
  }
}

provider "google" {
  version = ">= 1.18"
}

variable "bbq_project" {
  description = "it is needed to filter out backups from SLI"
}
