import logging


class DatastoreRestClient(object):
    def __init__(self, datastore_service, datastore_project_id):
        self.datastore_service = datastore_service
        self.datastore_project_id = datastore_project_id

    @staticmethod
    def get_path(backup):
        return backup['entity']['key']['path']

    def get_table(self, table_reference):
        tables = self.query_table(table_reference)
        if tables:
            if len(tables) == 1:
                return tables[0]
            else:
                raise Exception("Expected single result. Returned %s tables: %s"
                                .format(len(tables), tables))
        else:
            return None

    def get_table_entities(self, table_reference):
        return self.query_table(table_reference)

    def get_table_backups_sorted(self, table_entity):
        if table_entity is None:
            return {}

        table_key_path = table_entity['entity']['key']['path']
        backups = self.query_backups_with_ancestor_ordered_desc(table_key_path)
        return backups

    def query_table(self, table_reference):
        body = {
            "query": {
                "kind": [
                    {
                        "name": "Table"
                    }
                ],
                "filter": {
                    "compositeFilter": {
                        "filters": [
                            {
                                "propertyFilter": {
                                    "value": {
                                        "stringValue": table_reference.get_project_id()
                                    },
                                    "property": {
                                        "name": "project_id"
                                    },
                                    "op": "EQUAL"
                                }
                            },
                            {
                                "propertyFilter": {
                                    "value": {
                                        "stringValue": table_reference.get_dataset_id()
                                    },
                                    "property": {
                                        "name": "dataset_id"
                                    },
                                    "op": "EQUAL"
                                }
                            },
                            {
                                "propertyFilter": {
                                    "value": {
                                        "stringValue": table_reference.get_table_id()
                                    },
                                    "property": {
                                        "name": "table_id"
                                    },
                                    "op": "EQUAL"
                                }
                            }
                        ],
                        "op": "AND"
                    }
                }
            }
        }

        if table_reference.is_partition():
            body['query']['filter']['compositeFilter']['filters'].append({
                "propertyFilter": {
                    "value": {
                        "stringValue": table_reference.get_partition_id()
                    },
                    "property": {
                        "name": "partition_id"
                    },
                    "op": "EQUAL"
                }
            })

        response = self.datastore_service.projects() \
            .runQuery(projectId=self.datastore_project_id, body=body).execute()

        results = response['batch'].get('entityResults', [])
        logging.info("Returning %s elements querying table '%s'", len(results), table_reference)
        return results

    # ancestor_path example
    # "path": [
    #     {
    #         "id": "5719378301550592",
    #         "kind": "Table"
    #     }
    # ]
    def query_backups_with_ancestor_ordered_desc(self, parent_key_path):
        body = {
            "query": {
                "kind": [
                    {
                        "name": "Backup"
                    }
                ],
                "filter": {
                    "propertyFilter": {
                        "value": {
                            "keyValue": {
                                "path": parent_key_path
                            }
                        },
                        "op": "HAS_ANCESTOR",
                        "property": {
                            "name": "__key__"
                        }
                    }
                },
                "order": {
                    "direction": "DESCENDING",
                    "property": {
                        "name": "created"}
                }
            }
        }
        response = self.datastore_service.projects() \
            .runQuery(projectId=self.datastore_project_id, body=body).execute()
        results = response['batch'].get('entityResults', [])
        logging.info(
            "Returning %s backups for ancestor parent_key_path:'%s'",
            len(results), parent_key_path
        )
        return results

    # entity_path example
    # "path": [
    #     {
    #         "kind": "Table",
    #         "id": "5719378301550592"
    #     },
    #     {
    #         "kind": "Backup",
    #         "id": "5629499534213120"
    #     }
    # ]
    def delete_entity(self, entity_key_path):
        body = {
            "mutations": [
                {
                    "delete": {
                        "path": entity_key_path
                    }
                }
            ],
            "mode": "NON_TRANSACTIONAL"
        }
        logging.info("Deleting entity with key_path:'%s'", entity_key_path)
        self.datastore_service.projects() \
            .commit(projectId=self.datastore_project_id, body=body).execute()

