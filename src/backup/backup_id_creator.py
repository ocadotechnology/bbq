from src.commons.exceptions import ParameterValidationException


class BackupIdCreator(object):
    @classmethod
    def create(cls, project_id, dataset_id, table_id, timestamp,
               partition_id=None):
        """
        :return:
            Backup id for specified project, dataset, table, timestamp
                and partition(optional).
            If created id exceeds number of 1024 chars,
                it replace last 24 signs with '-' and 18-20 length hash
            If project, dataset, table or timestamp are not specified
                throws ParameterValidationException.
        """
        if project_id is None:
            raise ParameterValidationException(
                'No project specified, attribute is mandatory.')
        if dataset_id is None:
            raise ParameterValidationException(
                'No dataset specified, attribute is mandatory.')
        if table_id is None:
            raise ParameterValidationException(
                'No table specified, attribute is mandatory.')
        if timestamp is None:
            raise ParameterValidationException(
                'No timestamp specified, attribute is mandatory.')

        name = '_'.join(
            (timestamp.strftime("%Y%m%d_%H%M%S"),
             project_id.replace('-', '_'),
             dataset_id,
             table_id)
        ) + ('' if partition_id is None else '_partition_'+str(partition_id))
        if len(name) > 1024:
            # checksum returns long int with a sign, 18-20 characters long
            checksum = str(hash(name)).replace('-', '_')
            return '_'.join((name[:1000], checksum))[:1024]
        else:
            return name
