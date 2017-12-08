from rq.job import Job


class DelayJob(Job):

    def __init__(self, id=None, connection=None):
        super(DelayJob, self).__init__(id=id, connection=connection)

    @classmethod
    def create(cls, *args,  **kwargs):
        job = super(DelayJob, cls).create(*args, **kwargs)
        return job
