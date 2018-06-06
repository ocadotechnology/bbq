### Prerequisites
  * Ownership of the GCP project with assigned billing and enabled BigQuery (backups will be stored in that project).
    * see [creating a project and enabling the bigquery api](https://cloud.google.com/bigquery/docs/enable-transfer-service#creating_a_project_and_enabling_the_bigquery_api)

### Instalation steps
* Clone BBQ repository
* Open [config.yaml](./config/prd/config.yaml) and paste properly filled template: 
  ```
  copy_jobs:
    copy_job_result_check_countdown_in_sec: 60  # ONE MINUTE
  
  backup_settings:
    backup_worker_max_countdown_in_sec: 25200  # SEVEN_HOURS_IN_SECONDS
    custom_project_list: []
    projects_to_skip: ['<your-project-name>']
  
  project_settings:
    backup_project_id: '<your-project-name>'
    restoration_project_id: '<your-project-name>'
    authorized_requestor_service_accounts: [] 
  ```
* Install dependency requirements
  ```bash
  pip install -t lib -r requirements.txt
  ```
*  Deploy app
  ```bash
  cd ..
  gcloud app deploy --project "<your-project-id>" bbq/app.yaml bbq/config/cron.yaml bbq/config/prd/queue.yaml bbq/config/index.yaml
  ```
  
  Note: If it is your first App Engine deploy, App Engine instance needs to be created and you will need to choose preferred localisation. 
* Grant IAM role **BigQuery Data Viewer** for App Engine default service account (*<your-project-id>@appsport.gserviceaccount.com*) to each project which should be backed up
  * The easiest and fastest way is to grant permission at organization or folder level (but you can still control what should be backed up on project level)



### Advanced setup
  * It is possible to manage what projects will be backed up using project IAMs and also using config.yaml file.
      * **custom_project_list** - list of projects to backup (or empty to include all), but still **BigQuery Data Viewer** role for AE service account needs to be given for each of mentioned project
      * **projects_to_skip** - list of projects to skip always (it's recommended to skip BBQ project itself)
      * **backup_project_id** - project id where backups will be stored (it's recommended to be BBQ project itself)
      * **restoration_project_id** - project id where data after restoration will be stored
      




    
