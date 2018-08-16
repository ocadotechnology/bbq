import logging
import cloudstorage as gcs

from src.commons.decorators.retry import retry

my_default_retry_params = gcs.RetryParams(
    initial_delay=0.2,
    max_delay=5.0,
    backoff_factor=2,
    max_retry_period=15
)
gcs.set_default_retry_params(my_default_retry_params)


class GoogleCloudStorageClient(object):
    @classmethod
    @retry(Exception, tries=6, delay=2, backoff=2)
    def get_gcs_file_content(cls, bucket, filename):
        filename = '/{0}/{1}'.format(bucket, filename)

        try:
            with gcs.open(filename) as gcs_file:
                content = gcs_file.read()
        except Exception as e:
            logging.warning('Can\'t read GCS file gs://%s due to %s',
                            filename, e)
            return None

        return content

    @classmethod
    @retry(Exception, tries=6, delay=2, backoff=2)
    def put_gcs_file_content(
            cls, bucket, filename, data, content_type='text/plain'
    ):
        filename = '/{0}/{1}'.format(bucket, filename)

        try:
            with gcs.open(
                filename, 'w', content_type=content_type
            ) as gcs_file:
                content = gcs_file.write(data)
        except Exception as e:
            logging.warning('Can\'t write GCS file gs://%s due to %s',
                            filename, e)
            return None

        return content
