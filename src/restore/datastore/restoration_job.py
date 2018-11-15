import logging

from google.appengine.ext import ndb


class DuplicatedRestorationJobIdException(Exception):
    pass


class RestorationJob(ndb.Model):
    created = ndb.DateTimeProperty(indexed=True, auto_now_add=True)
    items_count = ndb.IntegerProperty(indexed=True)
    create_disposition = ndb.StringProperty(indexed=True)
    write_disposition = ndb.StringProperty(indexed=True)

    @classmethod
    @ndb.transactional
    def create(cls, restoration_job_id, create_disposition, write_disposition):
        already_exist = RestorationJob.get_by_id(restoration_job_id)

        if already_exist:
            raise DuplicatedRestorationJobIdException()

        restoration_job = RestorationJob(
            id=restoration_job_id,
            items_count=0,
            create_disposition=create_disposition,
            write_disposition=write_disposition
        )
        return restoration_job.put()

    @ndb.transactional
    def increment_count_by(self, count):
        restoration_job = self.key.get()
        items_count_before_update = restoration_job.items_count
        restoration_job.items_count += count
        logging.info("Restore items count before update %s, after update: %s",
                     items_count_before_update, restoration_job.items_count)
        restoration_job.put()
