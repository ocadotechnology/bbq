import logging
from time import sleep

from src.big_query.big_query import BigQuery
from src.configuration import configuration


DATASET_ID = "test_mr1"


class ExportGCSToBigQueryException(Exception):
    pass


class ExportGCSToBigQueryService(object):

    @classmethod
    def export(cls, output_url_prefix):
        backup_load_job = cls.create_load_job("Backup", output_url_prefix)
        table_load_job = cls.create_load_job("Table", output_url_prefix)

        load_jobs = [backup_load_job, table_load_job]
        load_job_ids = []
        for load_job in load_jobs:
            job_id = BigQuery().insert_job(configuration.backup_project_id,
                                           load_job)
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

    @classmethod
    def create_load_job(cls, entity, output_url_prefix):
        date = output_url_prefix[5:].split("/")[1].replace("-", "_")
        return {
            "projectId": configuration.backup_project_id,
            "location": "EU",
            "configuration": {
                "load": {
                    "sourceFormat": "DATASTORE_BACKUP",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "sourceUris": [
                        "{}/all_namespaces/kind_{}/"
                        "all_namespaces_kind_{}.export_metadata".format(
                            output_url_prefix, entity, entity)
                    ],
                    "destinationTable": {
                        "projectId": configuration.backup_project_id,
                        "datasetId": DATASET_ID,
                        "tableId": entity + "_" + date
                    }
                }
            }
        }
