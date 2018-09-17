# SLI (Service Level Indicators)


### Number of tables which have no backups after X days since last change.  

This metric is measuring number of tables which are not backed up since X days from last observed change. 

Metric is implemented as a BigQuery query, which uses data about modifications of tables (collected by [GCP Census](https://github.com/ocadotechnology/gcp-census)) and data about existing backups. 

### Number of tables not modified since 3 days and which have no equal backup

This metric is measuring number of tables that were not modified since 3 days and which last available backup does not contain the same metadata as source.

Metric is implemented as a BigQuery query, which list all tables where last modification was over 3 days ago and joins them with last available Datastore backups metadata using [Cloud Datastore export](./SETUP.md#cloud-datastore-export) and then compare the number of bytes and rows between source table and its backup (collected by [GCP Census](https://github.com/ocadotechnology/gcp-census)). 

## SLI setup

To measure SLI, please follow all the steps below:
1. Install [GCP Census](https://github.com/ocadotechnology/gcp-census) application that periodical collects metadata about BigQuery tables. 
1. Configure [Cloud Datastore export](./SETUP.md#cloud-datastore-export), 
which periodically exports backup metadata and stores it in BigQuery,
1. Install [Terraform](https://www.terraform.io/) if it's not available,
1. Create [Terraform backend](https://www.terraform.io/docs/backends/) GCS bucket, which will be used to store TF state:
      ```bash
      export TERRAFORM_STATE_BUCKET_ID="<your-bucket-id-to-store-terraform-state>"
      gsutil mb -p ${BBQ_PROJECT_ID} gs://${TERRAFORM_STATE_BUCKET_ID}/
      ```
1. Export BBQ, BBQ restoration project and GCP Census project ids: 
      ```bash
      export TF_VAR_bbq_project=${BBQ_PROJECT_ID}
      export TF_VAR_bbq_restoration_project=${BBQ_RESTORATION_PROJECT_ID}
      export TF_VAR_gcp_census_project=${GCP_CENSUS_PROJECT_ID}
      ```
1. Create a bucket to store remotely infrastructure state and then create all views/tables using [Terraform](https://www.terraform.io/) by running the following commands:
      ```bash
      terraform init -backend-config="bucket=${TERRAFORM_STATE_BUCKET_ID}"
      terraform apply
      ```
1. Deploy SLO service (it's a separate [GAE service](https://cloud.google.com/appengine/docs/standard/python/an-overview-of-app-engine#services)):
      ```bash
      gcloud app deploy --project ${BBQ_PROJECT_ID} slo-service.yaml
      ```
1. Configure periodical SLI export by adding the following [cron entry](./config/cron.yaml)
      ```yaml
      - description: SLI X days calculation
        url: /cron/slo/calculate
        schedule:  every 6 hours from 00:25 to 23:59
        retry_parameters:
          job_retry_limit: 5
        target: slo-service
      ```
1. Deploy all cron entries:
      ```bash
      gcloud app deploy --project ${BBQ_PROJECT_ID} config/cron.yaml
      ```

## SLI results

All SLI results can be queried from ``SLI_history.SLI_backup_creation_latency_view`` and ``SLI_backup_quality_views.SLI_quality`` views.