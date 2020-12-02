class BigQueryTable(object):
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id if isinstance(table_id, unicode) else str(table_id).decode('utf8')

    def get_project_id(self):
        return self.project_id

    def get_dataset_id(self):
        return self.dataset_id

    def get_table_id(self):
        return self.table_id

    def __unicode__(self):
        return u'{0}:{1}.{2}'.format(self.project_id, self.dataset_id,
                                     self.table_id)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __repr__(self):
        return self.__str__()

    def __eq__(self, o):
        return type(o) is BigQueryTable \
               and self.project_id == o.project_id \
               and self.dataset_id == o.dataset_id \
               and self.table_id == o.table_id

    def __ne__(self, other):
        return not (self == other)

    def to_json(self):
        return dict(project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_id=self.table_id)

    @classmethod
    def from_json(cls, json):
        return BigQueryTable(project_id=json["project_id"],
                             dataset_id=json["dataset_id"],
                             table_id=json["table_id"])

    @staticmethod
    def split_table_and_partition_id(table_id):
        table_id = table_id
        partition_id = None
        if '$' in table_id:
            table_id, partition_id = table_id.split('$')

        return table_id, partition_id
