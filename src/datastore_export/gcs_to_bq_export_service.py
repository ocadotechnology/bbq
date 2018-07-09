import logging
from time import sleep

from src.backup.copy_job_async.copy_job_result import CopyJobResult
from src.big_query.big_query import BigQuery


class GCSToBQExportService(object):

  def __init__(self):
    pass

  def make_export(self):
    load_backup_entities_job = {
      "projectId": 'local-project-bbq',
      "location": "EU",
      "configuration": {
        "load": {
          "sourceFormat": "DATASTORE_BACKUP",
          "sourceUris": [
            "gs://temp_mr_mr/2018-07-06T16:52:52_38464/all_namespaces/kind_Backup/all_namespaces_kind_Backup.export_metadata"
          ],
          "destinationTable": {
            "projectId": 'local-project-bbq',
            "datasetId": "test_mr1",
            "tableId": "Backup"
          }
        }
      }
    }

    load_backups_job_id = BigQuery().insert_job(
        'local-project-bbq', load_backup_entities_job)
    logging.info("Job ID: " + load_backups_job_id)
    print load_backups_job_id

    load_table_entities_job = {
      "projectId": 'local-project-bbq',
      "location": "EU",
      "configuration": {
        "load": {
          "sourceFormat": "DATASTORE_BACKUP",
          "sourceUris": [
            "gs://temp_mr_mr/2018-07-06T16:52:52_38464/all_namespaces/kind_Table/all_namespaces_kind_Table.export_metadata"
          ],
          "destinationTable": {
            "projectId": 'local-project-bbq',
            "datasetId": "test_mr1",
            "tableId": "Tables"
          }
        }
      }
    }

    load_tables_job_id = BigQuery().insert_job(
        'local-project-bbq', load_table_entities_job)
    logging.info("Job ID: " + load_tables_job_id)
    print load_tables_job_id

    is_done=False
    while not is_done:
      sleep(5)
      result = BigQuery().get_job(
          project_id='local-project-bbq',
          job_id=load_backups_job_id
      )

      copy_job_result = CopyJobResult(result)
      if not copy_job_result.is_done():
        print "1 not done"
        continue

      result = BigQuery().get_job(
          project_id='local-project-bbq',
          job_id=load_tables_job_id
      )

      copy_job_result = CopyJobResult(result)
      if not copy_job_result.is_done():

        continue
      print "3 is done"
      is_done=True



