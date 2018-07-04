### Installation steps

##### The recommended way is to use Google Cloud Shell - click button below. It opens your Google Cloud Shell and clones the repository. 
  [![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/open?git_repo=https%3A%2F%2Fgithub.com%2Focadotechnology%2Fbbq&page=shell&tutorial=SETUP.md)
    
   It is possible to do it from local environment, but it requires installing Google Cloud SDK for Python (see at [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

1. Create and enable billing to two GCP projects 
    * First is the place where BBQ will be deployed and backups will be stored.
    * Second is the place where backups will be temporarily stored after restoration procedure. 
    * see [creating a project in GCP](https://support.google.com/cloud/answer/6251787?hl=en#) doc

1. **Change values in '<>' to id of your projects.** Below commands will export that values, as they are used by commands from next steps:
      ```bash
      export BBQ_PROJECT_ID="<your-project-id-for-BBQ-project>"
      export RESTORATION_PROJECT_ID="<your-project-id-for-restoration-project>"   
      ```

1. Below commands will edit [config.yaml](./config/config.yaml) file. Previosly exported project id values will replace placeholders:
      ```bash
      sed -i -e "s/<your-project-id-for-BBQ-project>/${BBQ_PROJECT_ID}/g" config/config.yaml
      sed -i -e "s/<your-project-id-for-restoration-project>/${RESTORATION_PROJECT_ID}/g" config/config.yaml
      ```

1. Below command installs all required python dependencies:
      ```bash
      pip install -t lib -r requirements.txt
      ```
1.  Below command deploys App Engine application:
      ```bash
      gcloud app deploy --project ${BBQ_PROJECT_ID} app.yaml config/cron.yaml config/queue.yaml config/index.yaml
      ```
      Note: If it is your first App Engine deploy, App Engine needs to be initialised and you will need to choose [region/location](https://cloud.google.com/appengine/docs/locations).

1. Grant IAM role **BigQuery Data Viewer** for App Engine default service account (*\<your-project-id-for-BBQ-project\>@appspot.gserviceaccount.com*) to each project which should be backed up.
   e.g. change */<project-id-to-be-backed-up/>* to your project id which should be backed up and run below command:
      ```bash
      gcloud projects add-iam-policy-binding <project-id-to-be-backed-up> --member='serviceAccount:'${BBQ_PROJECT_ID}'@appspot.gserviceaccount.com' --role='roles/bigquery.dataViewer'
      ```
      * It is also possible to grant this permission through Google Cloud Console in [IAM tab](https://console.cloud.google.com/iam-admin/iam). 
      * You can also grant this permission for the whole folder or organisation. It will be inherited by all of the projects underneath.

1. Congratulations! BBQ is running now. The backup process will start on time defined in [cron.yaml](./config/cron.yaml) file. Note that time is given in UTC timezone. 
You can also trigger it manually, for more details look at [Usage section](README.md#usage).

### Advanced setup
  It is possible to manage in more detailed way what projects will be backed up using project IAMs and [config.yaml](./config/config.yaml) file.

  * **custom_project_list** - list of projects to backup. If empty, BBQ will backup everything it has read (**BigQuery Data Viewer**) access to. If list is provided you still need to grant **BigQuery Data Viewer** role for BBQ service account for each mentioned projects.
  * **projects_to_skip** - list of projects to skip (it's recommended to skip BBQ project itself). It is useful when you grant **BigQuery Data Viewer** for BBQ service account for the whole organization or folder and want to exclude some of the projects.
  * **backup_project_id** - project id where backups will be stored (it usually is the same project on which BBQ runs)
  * **restoration_project_id** - project into which data will be restored during restoration process


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

1. Change all **\<your-project-id\>** to your previously created project id in [config.yaml](./config/config.yaml).
      ```bash
      sed -i -e "s/<your-project-id>/${PROJECT_ID}/g" config/local/config.yaml
      ```

1. Install dependency requirements
      ```bash
      pip install -t lib -r requirements.txt
      ```

1. Link config files to main application folder (due to lack of possibility to pass full path to dev_appserver.py)
      ```bash
      ln -s config/queue.yaml queue.yaml
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
