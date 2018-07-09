import logging
from time import sleep

from src.big_query.big_query import BigQuery
from src.configuration import configuration


class ExportGCSToBigQueryException(Exception):
  pass


class GCSToBQExportService(object):

  def export(self, gcs_export_datetime):
    bakckup_load_job = self.create_load_job("Backup", gcs_export_datetime)
    table_load_job = self.create_load_job("Table", gcs_export_datetime)

    load_jobs = [bakckup_load_job, table_load_job]
    load_job_ids = []
    for load_job in load_jobs:
      job_id = BigQuery().insert_job(configuration.backup_project_id, load_job)
      load_job_ids.append(job_id)

    sleep(5)

    for load_job_id in load_job_ids:
      is_done = False
      while not is_done:
        result = BigQuery().get_job(
            project_id=configuration.backup_project_id,
            job_id=load_job_id
        )
        logging.info(result)

        is_done = result['status']['state'] == 'DONE'
        print is_done

        if not is_done:
          sleep(2)
        if 'errors' in result['status']:
          raise ExportGCSToBigQueryException()

  def create_load_job(self, entity, gcs_export_datetime):
    date = gcs_export_datetime.split('T')[0].replace('-', '')

    return {
      "projectId": configuration.backup_project_id,
      "location": "EU",
      "configuration": {
        "load": {
          "sourceFormat": "DATASTORE_BACKUP",
          "writeDisposition": "WRITE_TRUNCATE",
          "sourceUris": [
            "gs://temp_mr_mr/" + gcs_export_datetime +
            "/all_namespaces/kind_" + entity + "/all_namespaces_kind_"
            + entity + ".export_metadata"
          ],
          "destinationTable": {
            "projectId": configuration.backup_project_id,
            "datasetId": "test_mr1",
            "tableId": entity + "_" + date
          }
        }
      }
    }
