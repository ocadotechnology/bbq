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