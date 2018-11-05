class JobResultExample(object):
    IN_PROGRESS = {
        "status": {
            "state": "IN PROGRESS"
        },
        "configuration": {
            "copy": {
                "sourceTable": {
                    "projectId": "source_project_id",
                    "tableId": "source_table_id$123",
                    "datasetId": "source_dataset_id"
                },
                "destinationTable": {
                    "projectId": "target_project_id",
                    "tableId": "target_table_id",
                    "datasetId": "target_dataset_id"
                }
            }
        }
    }
    DONE = {
        "status": {
            "state": "DONE"
        },
        "statistics": {
            "startTime": "1511356638992",
            "endTime": "1511356648992"
        },
        "configuration": {
            "copy": {
                "sourceTable": {
                    "projectId": "source_project_id",
                    "tableId": "source_table_id$123",
                    "datasetId": "source_dataset_id"
                },
                "destinationTable": {
                    "projectId": "target_project_id",
                    "tableId": "target_table_id",
                    "datasetId": "target_dataset_id"
                }
            }
        }
    }
    DONE_WITH_RETRY_ERRORS = {
        "status": {
            "state": "DONE",
            "errors": [
                {
                    "reason": "invalid",
                    "message": "Cannot read a table without a schema"
                },
                {
                    "reason": "backendError",
                    "message": "Backend error"
                }
            ],
            "errorResult": {
                "reason": "backendError",
                "message": "Backend error"
            }
        },
        "statistics": {
            "startTime": "1511356638992",
            "endTime": "1511356648992"
        },
        "configuration": {
            "copy": {
                "sourceTable": {
                    "projectId": "source_project_id",
                    "tableId": "source_table_id$123",
                    "datasetId": "source_dataset_id"
                },
                "destinationTable": {
                    "projectId": "target_project_id",
                    "tableId": "target_table_id",
                    "datasetId": "target_dataset_id"
                }
            }
        }
    }
    DONE_WITH_NOT_REPETITIVE_ERRORS = {
        "status": {
            "state": "DONE",
            "errors": [
                {
                    "reason": "invalid",
                    "message": "Cannot read a table without a schema"
                },
                {
                    "reason": "backendError",
                    "message": "Backend error"
                }
            ],
            "errorResult": {
                "reason": "invalid",
                "message": "Cannot read a table without a schema"
            }
        },
        "statistics": {
            "startTime": "1511356638992",
            "endTime": "1511356648992"
        },
        "configuration": {
            "copy": {
                "sourceTable": {
                    "projectId": "source_project_id",
                    "tableId": "source_table_id$123",
                    "datasetId": "source_dataset_id"
                },
                "destinationTable": {
                    "projectId": "target_project_id",
                    "tableId": "target_table_id",
                    "datasetId": "target_dataset_id"
                }
            }
        }
    }
