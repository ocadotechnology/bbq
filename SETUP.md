### Prerequisites
  * Ownership of the GCP project with assigned billing and enabled BigQuery (backups will be stored in that project).
    * see [creating a project and enabling the bigquery api](https://cloud.google.com/bigquery/docs/enable-transfer-service#creating_a_project_and_enabling_the_bigquery_api)

### Instalation steps
The easiest way is to use Google Cloud Shell - click button. It opens GCShell and clones this repository.
 
<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/ocadotechnology/bbq&page=editor&open_in_editor=SETUP.md">
<img alt="Open in Cloud Shell" src ="http://gstatic.com/cloudssh/images/open-btn.png"></a>

  * Note: It is possible to do it from local environment. But it requires installing Google Cloud SDK for Python (see at [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

Then you could follow below steps:
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
*  Deploy App Engine application
  ```bash
  cd ..
  gcloud app deploy --project "<your-project-id>" bbq/app.yaml bbq/config/cron.yaml bbq/config/prd/queue.yaml bbq/config/index.yaml
  ```
  
  Note: If it is your first App Engine deploy, App Engine instance needs to be created and you will need to choose preferred localisation. 
* Grant IAM role **BigQuery Data Viewer** for App Engine default service account (*<your-project-id>@appsport.gserviceaccount.com*) to each project which should be backed up
  * The easiest and fastest way is to grant permission at organization or folder level (but you can still control what should be backed up on project level)

* Congratulations! BBQ is running now. The backup process will start on time defined in *cron.yaml* file. 
To enforce start now, GET *<your-project-id>.appspot.com/cron/backup*

### Advanced setup
  * It is possible to manage what projects will be backed up using project IAMs and also using config.yaml file.
      * **custom_project_list** - list of projects to backup (or empty to include all), but still **BigQuery Data Viewer** role for AE service account needs to be given for each of mentioned project
      * **projects_to_skip** - list of projects to skip always (it's recommended to skip BBQ project itself)
      * **backup_project_id** - project id where backups will be stored (it's recommended to be BBQ project itself)
      * **restoration_project_id** - project id where data after restoration will be stored
      


### Local environment setup

Note: App Engine SDK has useful feature which allows to run App Engine application on your local computer. 
Unfortunately, in BBQ application apart of AppEngine, BigQuery is used, which cannot be emulated on local. 
That's why there is a need to have GCP project with enabled BigQuery.

#### Steps

* Follow first steps from installation guide. Exception: edit *./config/**local**/config.yaml*

* The BBQ will use your personal google account (see at [gcloud auth](https://cloud.google.com/sdk/gcloud/reference/auth/)), so grant yourself BigQuery Data Viewer IAM role in project that you will backup and Editor role on main backup project where backups will be stored.

* Copy (or make link) ./config/local/queue.yaml , ./config/cron.yaml and ./config/index.yaml to main application folder (due to lack of possibility to pass full path to dev_appserver.py)

* Run command 
  ```bash
  dev_appserver.py app.yaml
  ```
  
* Local instance of App Engine application (with own queues, datastore) should be run. Check http://0.0.0.0:8000


#### Running unit tests



* Clone repository
* Install dependency requirements
  ```bash
     pip install -t lib -r requirements.txt
     pip install -r requirements_test.txt
  ```
* Run command
