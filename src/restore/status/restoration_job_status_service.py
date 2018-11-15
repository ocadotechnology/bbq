import json
import logging
from collections import Counter

from src.commons import camel_case_converter
from src.commons.exceptions import NotFoundException
from src.commons.config.environment import Environment
from src.commons.config.configuration import configuration
from src.restore.datastore.restoration_job import RestorationJob
from src.restore.datastore.restore_item import RestoreItem


class RestorationJobStatusService(object):
    def get_restoration_job(self, restoration_job_id, warnings_only=False):
        restoration_job = self.__get_restoration_job_entity(restoration_job_id)

        restoration_items = list(self.__get_restoration_items(restoration_job))
        status = self.__create_status(restoration_items,
                                      restoration_job.items_count)
        item_results = self.__create_item_results(restoration_items,
                                                  restoration_job.items_count)

        if warnings_only:
            restoration_items = self.__filter_out_done_and_in_progress(
                restoration_items)

        return {"restorationJobId": restoration_job_id,
                "status": status,
                "itemResults": item_results,
                "itemsCount": restoration_job.items_count,
                "createDisposition": restoration_job.create_disposition,
                "writeDisposition": restoration_job.write_disposition,
                "restorationItems": restoration_items}

    @staticmethod
    def get_status_endpoint(restoration_job_id):
        domain = Environment.get_domain(configuration.backup_project_id)
        return "{}/restore/jobs/{}".format(domain,
                                                         restoration_job_id)

    @staticmethod
    def get_warnings_only_status_endpoint(restoration_job_id):
        domain = Environment.get_domain(configuration.backup_project_id)
        return "{}/restore/jobs/{}?warningsOnly".format(domain,
                                                         restoration_job_id)

    @staticmethod
    def __get_restoration_job_entity(restoration_job_id):
        entity = RestorationJob.get_by_id(restoration_job_id)
        if entity:
            return entity
        else:
            raise NotFoundException("Restoration job with id: '{}' doesn't "
                                    "exists".format(restoration_job_id))

    def __get_restoration_items(self, restoration_job):
        for item in RestoreItem.get_restoration_items(restoration_job.key):
            completed = item.completed.isoformat() if item.completed else None
            yield {
                "status": item.status,
                "statusMessage": item.status_message,
                "completed": completed,
                "customParameters": self.__parse_custom_parameters(
                    item.custom_parameters),
                "sourceTable": item.source_table_reference.__str__(),
                "targetTable": item.target_table_reference.__str__()
            }

    @staticmethod
    def __create_status(restoration_items, items_count):
        in_progress_count = sum(i["status"] == RestoreItem.STATUS_IN_PROGRESS
                                for i in restoration_items)
        done_count = sum(i["status"] == RestoreItem.STATUS_DONE
                         for i in restoration_items)
        is_finished = in_progress_count == 0 and \
                      len(restoration_items) == items_count
        status = {"state": "Done" if is_finished else "In progress"}
        if is_finished:
            is_success = items_count == done_count
            status["result"] = "Success" if is_success else "Failed"
        return status

    @staticmethod
    def __create_item_results(restoration_items, items_count):
        counter = Counter([item["status"] for item in restoration_items])
        result = dict(counter)

        unknown_items = items_count - len(restoration_items)
        if unknown_items > 0:
            result['unknown'] = unknown_items

        return camel_case_converter.dict_to_camel_case(result)

    @staticmethod
    def __parse_custom_parameters(custom_parameters):
        if not custom_parameters:
            return None
        try:
            return json.loads(custom_parameters)
        except ValueError:
            logging.exception("Unable to parse custom parameters: {}"
                              .format(custom_parameters))
            return custom_parameters

    @staticmethod
    def __filter_out_done_and_in_progress(item_results):
        return [item_result for item_result in item_results if
                item_result["status"] not in [RestoreItem.STATUS_DONE,
                                              RestoreItem.STATUS_IN_PROGRESS]]
