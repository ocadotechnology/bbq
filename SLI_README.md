# SLI (Service Level Indicators)


### Number of tables which have no backups after X days since last change.  

This metric is measuring number of tables which are not backed up since X days from last observed change. 

Metric is implemented as a BigQuery query, which uses data about modifications of tables (collected by Census) and data about existing backups. 

## SLI setup

To measure SLI, please follow all the steps below:
1. Install [GCP Census](https://github.com/ocadotechnology/gcp-census) application that periodical collects metadata about BigQuery tables. 
1. Configure [Cloud Datastore export](./SETUP.md#cloud-datastore-export), 
which periodically exports backup metadata and stores it in BigQuery, 
1. Create all views/tables using [Terraform](https://www.terraform.io/),
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
