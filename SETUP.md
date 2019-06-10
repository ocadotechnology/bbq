### Installation steps

##### The recommended way is to use Google Cloud Shell - click button below. It opens your Google Cloud Shell and clones the repository. 
  [![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/open?git_repo=https%3A%2F%2Fgithub.com%2Focadotechnology%2Fbbq&page=shell&tutorial=SETUP.md)
    
   It is possible to do it from local environment, but it requires Google Cloud SDK for Python (see [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

1. Create and enable billing for two GCP projects 
    * First project is the place where BBQ will be deployed and backups will be stored. (**BBQ_PROJECT_ID** variable)
    * Second project is meant to work as a temporary storage into which backups will be restored. (**RESTORATION_STORAGE_PROJECT_ID** variable)
    * see [creating a project in GCP](https://support.google.com/cloud/answer/6251787?hl=en#) doc

1. **Change values in brackets '<>' to project id's created in previous step. Note - changing values in command is needed only in this step.** Below commands will export that values, as they are used by commands from next steps:
      ```bash 
      export BBQ_PROJECT_ID="<your-project-id-for-BBQ-project>"
      ```
      ```bash 
      export RESTORATION_STORAGE_PROJECT_ID="<your-project-id-for-restoration-storage-project>"
      ```
1. Run commands below:
      ```bash
      sed -i -e "s/BBQ-project-id/${BBQ_PROJECT_ID}/g" config/config.yaml
      ```
      ```bash
      sed -i -e "s/restoration-storage-project-id/${RESTORATION_STORAGE_PROJECT_ID}/g" config/config.yaml
      ```
   Those command will edit [config.yaml](./config/config.yaml) file. Previosly exported project id values will replace placeholders.

1. Command below installs all required python dependencies:
      ```bash
      pip install -t lib -r requirements.txt
      ```

1. Command below deploys App Engine application:
      ```bash
      gcloud app deploy --project ${BBQ_PROJECT_ID} app.yaml config/cron.yaml config/queue.yaml config/index.yaml
      ```
      Note: If it is your first App Engine deploy, App Engine needs to be initialised and you will need to choose [region/location](https://cloud.google.com/appengine/docs/locations). It is recommended to pick the same location as where most of your BigQuery data resides.
1.   Secure your application by following given steps.
     * Restrict IAM roles for your GAE service account and Enable firewall for GAE application.
       * GAE default service account Editor permission needs to be removed manually.
         ```bash
         gcloud projects remove-iam-policy-binding ${BBQ_PROJECT_ID} --member='serviceAccount:'${BBQ_PROJECT_ID}'@appspot.gserviceaccount.com' --role='roles/editor’
         ```
       * You should follow PoLP (Principle of least privilege) during IAM's configuration.
         You can do that via [Terraform](TERRAFORM_SETUP.md).
         After setup go to **terraform/bbq** directory and run following command:
         ```bash
         terraform apply
         ```
         
     * By default Terraform configures firewall to block all traffic to your application.
       To get access to BBQ application you need to whitelist your public IP address in firewall rules.
       You can do it via [UI](https://console.cloud.google.com/appengine/firewall) or run following command:
       ```bash
        gcloud app firewall-rules create 100 --action allow --source-range ${MY_PUBLIC_IP} --description 'my public ip address'
       ```
1. Configure IAP (Identity-Aware Proxy), so that only authorised users can access BBQ.
     * Turn on IAP for your GAE application. All steps are described on [GAE IAP setup](https://cloud.google.com/iap/docs/app-engine-quickstart#enabling_iap) docs.
     * Grant IAP-Secured Web App User (`roles/iap.httpsResourceAccessor`) role to users, which will be using BBQ, e.g.:
         ```bash
         gcloud projects add-iam-policy-binding ${BBQ_PROJECT_ID} --member='user:'${BBQ_PROJECT_ID}'@appspot.gserviceaccount.com' --role='roles/iap.httpsResourceAccessor’
         ```     
     
1. BBQ should be deployed and working right now. You could see it at \<your-project-id-for-BBQ-project\>.appspot.com . 
   The backup process will start at the time defined in [cron.yaml](./config/cron.yaml) file. All times are in UTC standard. 
   You can also trigger the backup manually, for more details see [Usage section](README.md#usage).
  
   **Now you need to decide what will be backed up. Please go to *Granting access for BBQ* section**


### Granting access for BBQ

To perform backup, BBQ needs rights to read BigQuery data from the project which should be backed up. To allow read accees, **BigQuery Data Viewer** IAM role needs to be granted to App Engine default service account (*\<your-project-id-for-BBQ-project\>@appspot.gserviceaccount.com*). There are few alternative ways to do that:

* **Replace \<project-id-to-be-backed-up\> to proper project id** and run below command:

    ```bash
    gcloud projects add-iam-policy-binding --member='serviceAccount:'${BBQ_PROJECT_ID}'@appspot.gserviceaccount.com' --role='roles/bigquery.dataViewer' <project-id-to-be-backed-up>
    ```

* Grant this permission through Google Cloud Console in [IAM tab](https://console.cloud.google.com/iam-admin/iam) for project which should be backed up. 
* Grant this permission for the whole folder or organisation. It will be inherited by all of the projects underneath.

### Cloud Datastore export
  BBQ exports data from Datastore to Big Query. It's much easier to query the data in Big Query rather than Datastore. 
  It is possible to configure schedule time and kinds to export in [cron.yaml](./config/cron.yaml) file.

### Security Layers
BBQ has configured multiple layers of security to limit access to your data.
 * **Firewall**
   * A firewall provides identity-agnostic access control for your App Engine app based on network level. Current firewall setup only allows GAE cron requests and GAE task queue requests.
   * You should whitelist all your public IPs (e.g. office IP).
 * **IAP**
   * Cloud [Identity-Aware Proxy](https://cloud.google.com/iap/docs/concepts-overview) (Cloud IAP) lets you manage access to GAE application via IAMs.
   * You should allow all BBQ users to have IAP-Secured Web App User (`roles/iap.httpsResourceAccessor`).
 * **GAE admin endpoints**
   * BBQ internal endpoints are configured, so that only [admin users](https://cloud.google.com/appengine/docs/standard/python/users/adminusers) can access them. In BBQ case, it's only cron tasks and task queues.
   * You should not change them, as those are BBQ internal endpoints. There is no need to access them as the end user.

### Advanced setup
  It is possible to precisely control which projects will be backed up using project IAMs and [config.yaml](./config/config.yaml) file.

  * **custom_project_list** - list of projects to backup. If empty, BBQ will backup everything it has read (**BigQuery Data Viewer**) access to. If list is provided you still need to grant **BigQuery Data Viewer** role to BBQ service account for each mentioned project.
  * **projects_to_skip** - list of projects to skip (it's highly recommended to skip the project where BBQ runs and backups are stored). Common practice is to grant **BigQuery Data Viewer** to BBQ service account for the whole organization or folder and then exclude some of the projects.
  * **backup_project_id** - project id where backups will be stored (it usually is the same project on which BBQ runs)
  * **default_restoration_project_id** - project into which data will be restored by default during the restoration process


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

1. Install Google Cloud SDK (see [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

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

1. Link config files to main application folder (dev_appserver.py expects config files in the root folder)
      ```bash
      ln -s config/queue.yaml queue.yaml
      ln -s config/cron.yaml cron.yaml
      ln -s config/index.yaml index.yaml
      ```

1. Acquire new user credentials to use for application default credentials:
    ```bash
    gcloud auth application-default login
    ```

1. Run command 
      ```bash
      dev_appserver.py app.yaml
      ```
  
1. Local instance of App Engine application (with own queues, datastore) should be running at: http://localhost:8080  You can also view admin server at: http://localhost:8000
1. To run backup process go to: http://localhost:8080/cron/backup and sign in as administrator

#### Running unit tests

1. Install Google Cloud SDK (see [installing Cloud SDK for Python](https://cloud.google.com/appengine/docs/standard/python/download))

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
      python test_runner.py --test-path tests/ -v --test-pattern "test*.py" /PATH-TO/google-cloud-sdk
      ```
