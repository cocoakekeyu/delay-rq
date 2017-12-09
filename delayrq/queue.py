import time
import logging

from redis import WatchError
from rq.queue import Queue
from rq.utils import parse_timeout
from rq.job import JobStatus
from .exceptions import InvalidJobDependency



class DelayQueue(Queue):
    redis_delay_queue_namespace_prefix = 'rq:delay_queue:'
    redis_delay_queues_keys = 'rq:deley_queues'

    def __init__(self, name='default', default_timeout=None, connection=None,
                 async=True, job_class=None):
        super(DelayQueue, self).__init__(name, default_timeout, connection,
                                         async, job_class)
        prefix = self.redis_delay_queue_namespace_prefix
        self.delay_key = '{0}{1}'.format(prefix, name)

    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, description=None, delay=0,
                     depends_on=None, job_id=None, at_front=False, meta=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """
        timeout = parse_timeout(timeout) or self._default_timeout
        result_ttl = parse_timeout(result_ttl)
        ttl = parse_timeout(ttl)
        ts = time.time() + delay

        job = self.job_class.create(
            func, args=args, kwargs=kwargs, connection=self.connection,
            result_ttl=result_ttl, ttl=ttl, status=JobStatus.QUEUED,
            description=description, depends_on=depends_on, delay=delay,
            timeout=timeout, id=job_id, origin=self.name, meta=meta)

        # If job depends on an unfinished job, register itself on it's
        # parent's dependents instead of enqueueing it.
        # If WatchError is raised in the process, that means something else is
        # modifying the dependency. In this case we simply retry
        if depends_on is not None:
            if not isinstance(depends_on, self.job_class):
                depends_on = self.job_class(id=depends_on,
                                            connection=self.connection)
            with self.connection._pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(depends_on.key)

                        # If the dependency does not exist, raise an
                        # exception to avoid creating an orphaned job.
                        if not self.job_class.exists(depends_on.id,
                                                     self.connection):
                            raise InvalidJobDependency(
                                'Job {0} does not exist'.format(depends_on.id))

                        if depends_on.get_status() != JobStatus.FINISHED:
                            pipe.multi()
                            job.set_status(JobStatus.DEFERRED)
                            job.register_dependency(pipeline=pipe)
                            job.save(pipeline=pipe)
                            job.cleanup(ttl=job.ttl, pipeline=pipe)
                            pipe.execute()
                            return job
                        break
                    except WatchError:
                        continue

        if delay > 0:
            return self.enqueue_delay_job(job, ts)

        job = self.enqueue_job(job, at_front=at_front)

        return job

    def enqueue_delay_job(self, job, ts):
        """Enqueue a job into delay queue"""
        conn = self.connection
        conn.zadd(self.delay_key, **{job.id: ts})
        return job
