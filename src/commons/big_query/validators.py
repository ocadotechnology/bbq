import re

from src.commons.exceptions import ParameterValidationException


class WrongDatasetNameException(Exception):
    pass


class WrongProjectNameException(Exception):
    pass


class WrongWriteDispositionException(Exception):
    pass


class WrongCreateDispositionException(Exception):
    pass


project_id_pattern = re.compile("^[a-zA-Z0-9-]+$")
dataset_id_pattern = re.compile("^[a-zA-Z0-9_]+$")
available_write_dispositions = ["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"]
available_create_dispositions = ["CREATE_IF_NEEDED", "CREATE_NEVER"]


def validate_dataset_restore_params(source_project_id, source_dataset_id,
     target_project_id, target_dataset_id, create_disposition, write_disposition):
    __validate_restore_request_params(source_project_id=source_project_id,
                                      source_dataset_id=source_dataset_id,
                                      target_project_id=target_project_id,
                                      target_dataset_id=target_dataset_id,
                                      create_disposition=create_disposition,
                                      write_disposition=write_disposition)

def validate_list_restore_params(target_project_id, target_dataset_id,
      create_disposition, write_disposition):
    __validate_restore_request_params(target_project_id=target_project_id,
                                      target_dataset_id=target_dataset_id,
                                      create_disposition=create_disposition,
                                      write_disposition=write_disposition)


def __validate_restore_request_params(
        source_project_id=None, source_dataset_id=None, target_project_id=None,
        target_dataset_id=None, create_disposition=None, write_disposition=None,):
    try:
        if source_project_id:
            __validate_project_id(source_project_id)
        if source_dataset_id:
            __validate_dataset_id(source_dataset_id)
        if target_project_id:
            __validate_project_id(target_project_id)
        if target_dataset_id:
            __validate_dataset_id(target_dataset_id)
        if write_disposition:
            __validate_write_disposition(write_disposition)
        if create_disposition:
            __validate_create_disposition(create_disposition)

    except (WrongDatasetNameException,
            WrongProjectNameException,
            WrongWriteDispositionException,
            WrongCreateDispositionException), e:

        raise ParameterValidationException(e.message)


def __validate_project_id(project_id):
    if not project_id or not project_id_pattern.match(project_id):
        error_message = "Invalid project value: '{}'. Project IDs may " \
                        "contain letters, numbers, and " \
                        "dash".format(project_id)
        raise WrongProjectNameException(error_message)


def __validate_dataset_id(dataset_id):
    if not dataset_id or not dataset_id_pattern.match(dataset_id):
        error_message = "Invalid dataset value: '{}'. Dataset IDs may " \
                        "contain letters, numbers, and " \
                        "underscores".format(dataset_id)
        raise WrongDatasetNameException(error_message)


def __validate_write_disposition(write_disposition):
    if write_disposition not in available_write_dispositions:
        error_message = "Invalid write disposition: '{}'. " \
                        "The following values are supported: {}."\
            .format(write_disposition, ', '.join(available_write_dispositions))
        raise WrongWriteDispositionException(error_message)


def __validate_create_disposition(create_disposition):
    if create_disposition not in available_create_dispositions:
        error_message = "Invalid create disposition: '{}'. " \
                        "The following values are supported: {}." \
            .format(create_disposition, ', '.join(available_create_dispositions))
        raise WrongCreateDispositionException(error_message)
