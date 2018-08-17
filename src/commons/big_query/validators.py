import re


class WrongDatasetNameException(Exception):
    pass


dataset_id_pattern = re.compile("^[a-zA-Z0-9_]+$")


def validate_dataset_id(dataset_id):
    if not dataset_id or not dataset_id_pattern.match(dataset_id):
        error_message = "Invalid dataset value: '{}'. Dataset IDs may " \
                        "contain letters, numbers, and " \
                        "underscores".format(dataset_id)
        raise WrongDatasetNameException(error_message)
