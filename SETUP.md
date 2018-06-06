### Prerequisites
  * Ownership of the GCP project with assigned billing and enabled BigQuery (backups will be stored in that project).
    * see [creating a project and enabling the bigquery api](https://cloud.google.com/bigquery/docs/enable-transfer-service#creating_a_project_and_enabling_the_bigquery_api)

### Configuration
* Open [config.yaml](./config/prd/config.yaml) and replace project ids where:
    * custom_project_list - list of projects to backup (or empty to include all)
    * projects_to_skip - list of projects to skip always (it's recommended to skip backup project itself)
    * backup_project_id - your project id (bbq will store backups here)
    * restoration_project_id - storage destination for restored backups

* Install dependency requirements
```bash
pip install -t lib -r requirements.txt
```

* Deploy app
```bash
cd ..
gcloud app deploy --project "your-project-id" bbq/app.yaml bbq/config/cron.yaml bbq/config/prd/queue.yaml bbq/config/index.yaml
```

* Grant IAM role 'BigQuery Data Viewer' for App Engine default service account ('your-project-id@appsport.gserviceaccount.com') to each project which should be backed up
    * BBQ backups all projects in accordance with custom_project_list parameter in config.yaml and for each of them skips if Service Account don't have appropriate permission 
    * The easiest way is to grant permission at organization or folder level

