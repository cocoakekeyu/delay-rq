# -*- coding: utf-8 -*-
import logging

from rq.compat import string_types
from rq.connections import get_current_connection
from rq.utils import backend_class, ensure_list
from rq.default import DEFAULT_WORKER_TTL
from rq.exceptions import DequeueTimeout

from .queue import DelayQueue


class Timer(object):
    redis_timer_namespace_prefix = 'rq:timer:'
    redis_timers_keys = 'rq:timers'

    def __init__(self, queues, name=None, default_result_ttl=None,
                 connection=None, exc_handler=None, exception_handlers=None,
                 default_timer_ttl=None, job_class=None, queue_class=None):
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        self.default_timer_ttl = default_timer_ttl or DEFAULT_WORKER_TTL

        self.job_class = backend_class(self, 'job_class', override=job_class)
        self.queue_class = queue_class or DelayQueue

        queues = [self.queue_class(name=q,
                                   connection=connection,
                                   job_class=self.job_class)
                  if isinstance(q, string_types) else q
                  for q in ensure_list(queues)]

        self._name = name
        self.queues = queues

    def queue_names(self):
        return list(map(lambda q: q.name, self.queues))

    def work(self):
        logging.info('Timer started.')
        logging.info('Listening on {}..'.format(self.queue_names))
        while True:
            timeout = max(1, self.default_timer_ttl - 60)
            self.dequeue_delay_job_and_enqueue(timeout)

    def dequeue_delay_job_and_enqueue(self, timeout):
        conn = self.connection
        queue_keys = [q.key for q in self.queues]
        end_time = time.time() + timeout
        while time.time() < end_time:
            for queue_key in queue_keys:
                item = connection.zrange(queue_key, 0, 0, withscores=True)
                if not item or item[0][1] > time.time():
                    continue
            if not item or item[0][1] > time.time():
                time.sleep(.01)
                continue
            job_id = item[0][0]
            job = self.job_class.fetch_job(job_id)
            self.queue_class.enqueue_job(job)
            logging.info('Enqueue delay job: {}'.format(job.id))
