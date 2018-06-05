### Prerequisites
  * Ownership of the GCP project with enabled BigQuery (backups will be stored in that project).

### Configuration
* Open config/prd/config.yaml and replace project ids where:
    * custom_project_list - list of projects to backup (or empty to include all)
    * projects_to_skip - list of projects to skip always
    * backup_project_id - your project id (bbq will store backups here)
    * restoration_project_id - storage destination for restored backups

* Run command from shell
```bash
pip install -t lib -r requirements.txt
```

* Set service accounts and grant permissions
* Deploy
