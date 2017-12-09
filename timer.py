# -*- coding: utf-8 -*-
from rq.compat import string_types
from rq.connections import get_current_connection
from rq.utils import backend_class, ensure_list
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

        self.job_class = backend_class(self, 'job_class', override=job_class)
        self.queue_class = queue_class or DelayQueue

        queues = [self.queue_class(name=q,
                                   connection=connection,
                                   job_class=self.job_class)
                  if isinstance(q, string_types) else q
                  for q in ensure_list(queues)]

        self._name = name
        self.queues = queues

    def work(self):
        while True:
            job = self.dequeue_job(60)
            self.queue_class.enqueue_job(job)

    def dequeue_job(self, timeout):
        while True:
            try:
                result = self.queue_class.dequeue_any(self.queues, timeout,
                                                      connection=self.connection,
                                                      job_class=self.job_class)
                break
            except DequeueTimeout:
                pass
        return result
