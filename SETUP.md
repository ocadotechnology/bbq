### Prerequisites
Ownership of the GCP project with assigned billing (backups will be stored in that project).
 * see [creating a project in GCP](https://support.google.com/cloud/answer/6251787?hl=en#) doc

### Installation steps

The easiest way is to use Google Cloud Shell - click button below. It opens GCShell and clones the repository. 

<a href="https://console.cloud.google.com/cloudshell/open?git_repo=https://github.com/ocadotechnology/bbq&page=editor&open_in_editor=SETUP.md">
<img alt="Open in Cloud Shell" src ="http://gstatic.com/cloudssh/images/open-btn.png"></a>

<br>
  
  * Note: It is possible to do it from local environment. But it requires installing Google Cloud SDK for Python (see at [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

Then you could follow below steps:
1. Export your project id:
      ```bash
      export PROJECT_ID="<your-project-id>"
      ```

1. Change all **\<your-project-id\>** to your previously created project in [./config/prd/config.yaml](./config/prd/config.yaml) config file.
      ```bash
      sed -i -e "s/<your-project-id>/${PROJECT_ID}/g" config/prd/config.yaml
      ```

1. Install dependency requirements
      ```bash
      pip install -t lib -r requirements.txt
      ```
1.  Deploy App Engine application
       ```bash
       gcloud app deploy --project ${PROJECT_ID} app.yaml config/cron.yaml config/prd/queue.yaml config/index.yaml
       ```
  
    Note: If it is your first App Engine deploy, App Engine needs to be initialised and you will need to choose [region/location](https://cloud.google.com/appengine/docs/locations).
1. Grant IAM role **BigQuery Data Viewer** for App Engine default service account (*\<your-project-id\>@appspot.gserviceaccount.com*) to each project which should be backed up, e.g.:

       ```bash
       gcloud projects add-iam-policy-binding <project-id-to-be-backed-up> --member='serviceAccount:'${PROJECT_ID}'@appspot.gserviceaccount.com' --role='roles/bigquery.dataViewer'
       ```
      * You can also grant this permission for the whole folder or organisation. It will be inherited by all of the projects underneath.

1. Congratulations! BBQ is running now. The backup process will start on time defined in [cron.yaml](./config/cron.yaml) file.
You can also trigger it manually, for more details look at [Usage section](README.md#usage).

### Advanced setup
  It is possible to manage what projects will be backed up using project IAMs and also using config.yaml file.
  * **custom_project_list** - list of projects to backup. If empty, BBQ will backup everything it has read (**BigQuery Data Viewer**) access to. If list is provided you still need to grant **BigQuery Data Viewer** role for BBQ service account for each mentioned projects.
  * **projects_to_skip** - list of projects to skip (it's recommended to skip BBQ project itself). It is useful when you grant **BigQuery Data Viewer** for BBQ service account for the whole organization or folder and want to exclude some of the projects.
  * **backup_project_id** - project id where backups will be stored (it can also be the same project on which BBQ runs)
  * **restoration_project_id** - project into which data will be restored by default (you can also define restoration destination directly while executing restoration)


### Local environment setup

Note: App Engine SDK has useful feature which allows to run App Engine application on your local computer. 
Unfortunately, it does not provide any emulator for BigQuery so it is not possible to have BigQuery locally.
Therefore, in order to have application working locally we need to have GCP project with BigQuery enabled.
All backups that was invoked on local application will end up in this project.

#### Steps

1. BBQ requires Python in version 2.7.x. Make sure correct version is set up on your PATH
      ```bash
      python -V
      ```

1. Install Google Cloud SDK (see at [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

1. Run `gcloud init` to set up your account which will be used by BBQ

1. Grant yourself BigQuery Data Viewer role in project that you will backup and Editor role on main backup project where backups will be stored.
      ```bash
      gcloud projects add-iam-policy-binding ${PROJECT_ID} --member='user:<name.surname@example.com>' --role='roles/editor'
      gcloud projects add-iam-policy-binding <project-id-to-be-backed-up> --member='user:<name.surname@example.com>' --role='roles/bigquery.dataViewer'
      ```

1. Clone repository to the location of your choice and change the directory to bbq
      ```bash
      git clone https://github.com/ocadotechnology/bbq
      cd bbq
      ```

1. Export your project id
      ```bash
      export PROJECT_ID="<your-project-id>"
      ```

1. Change all **\<your-project-id\>** to your previously created project id in [./config/local/config.yaml](./config/local/config.yaml).
      ```bash
      sed -i -e "s/<your-project-id>/${PROJECT_ID}/g" config/local/config.yaml
      ```

1. Install dependency requirements
      ```bash
      pip install -t lib -r requirements.txt
      ```

1. Link config files to main application folder (due to lack of possibility to pass full path to dev_appserver.py)
      ```bash
      ln -s config/local/queue.yaml queue.yaml
      ln -s config/cron.yaml cron.yaml
      ln -s config/index.yaml index.yaml
      ```

1. Run command 
      ```bash
      dev_appserver.py app.yaml
      ```
  
1. Local instance of App Engine application (with own queues, datastore) should be running at: http://localhost:8080  You can also view admin server at: http://localhost:8000
1. To run backup process go to: http://localhost:8080/cron/backup and sign in as administrator

#### Running unit tests

1. Install Google Cloud SDK (see at [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

1. Clone repository to the location of your choice and change the directory to bbq
      ```bash
      git clone https://github.com/ocadotechnology/bbq
      cd bbq
      ```

1. Install dependency requirements
      ```bash
      pip install -t lib -r requirements.txt
      pip install -r requirements_tests.txt
      ```

1. Run following Python command (you might need to update Google Cloud SDK path)
      ```bash
      python test_runner.py --test-path tests/ -v --test-pattern "test*.py" ./google-cloud-sdk
      ```
