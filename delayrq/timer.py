# -*- coding: utf-8 -*-
import time

from rq.job import Job
from rq.worker import Worker, WorkerStatus, StopRequested
from rq.compat import string_types
from rq.connections import get_current_connection
from rq.utils import backend_class, ensure_list
from rq.logutils import setup_loghandlers
from rq.defaults import DEFAULT_WORKER_TTL
from rq.exceptions import DequeueTimeout

from .queue import DelayQueue


class Timer(Worker):
    redis_timer_namespace_prefix = 'rq:timer:'
    redis_timers_keys = 'rq:timers'

    def __init__(self, queues, name=None, default_result_ttl=None,
                 connection=None, exc_handler=None, exception_handlers=None,
                 default_worker_ttl=None, job_class=None, queue_class=None):
        queue_class = queue_class or DelayQueue
        super(Timer, self).__init__(
            queues, name=name, default_result_ttl=default_result_ttl,
            connection=connection, exc_handler=exc_handler,
            exception_handlers=exception_handlers,
            default_worker_ttl=default_worker_ttl, job_class=job_class,
            queue_class=queue_class)

    def work(self):
        setup_loghandlers("INFO")
        self._install_signal_handlers()
        self.register_birth()
        self.set_state(WorkerStatus.STARTED)

        self.log.info('Timer started.')
        qnames = ', '.join(self.queue_names())
        self.log.info('Listening on {}..'.format(qnames))
        try:
            while True:
                try:
                    self.dequeue_delay_job_and_enqueue()
                except StopRequested:
                    break
        finally:
            self.register_death()

    def dequeue_delay_job_and_enqueue(self):
        conn = self.connection
        while True:
            for q in self.queues:
                item = conn.zrange(q.delay_key, 0, 0, withscores=True)
                if not item or item[0][1] > time.time():
                    continue
                break
            if not item or item[0][1] > time.time():
                time.sleep(.01)
                continue
            job_id = item[0][0].decode('utf-8')
            job = self.job_class.fetch(job_id)
            # TODO: add lock
            if conn.zrem(q.delay_key, job_id):
                q.enqueue_job(job)
                self.log.info('Enqueue delay job: {}'.format(job.id))
