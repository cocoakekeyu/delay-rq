# -*- coding: utf-8 -*-
import time

from rq.worker import Worker, WorkerStatus, StopRequested
from rq.utils import make_colorizer
from rq.logutils import setup_loghandlers

from .queue import DelayQueue
from .lock import SimpleLock


green = make_colorizer('darkgreen')
blue = make_colorizer('darkblue')


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
        qnames = green(', '.join(self.queue_names()))
        self.log.info('Listening on {}..'.format(qnames))
        try:
            while True:
                try:
                    result = self.dequeue_delay_job()
                    if not result:
                        time.sleep(.01)
                        continue
                    queue, job = result
                    self.process_enqueue(queue, job)
                except StopRequested:
                    break
        finally:
            self.register_death()

    def dequeue_delay_job(self):
        conn = self.connection
        for q in self.queues:
            item = conn.zrange(q.delay_key, 0, 0, withscores=True)
            if item and item[0][1] < time.time():
                job_id = item[0][0].decode('utf-8')
                job = self.job_class.fetch(job_id)
                return q, job
        else:
            return None

    def process_enqueue(self, queue, job):
        conn = self.connection
        with SimpleLock(conn, job.id):
            if conn.zrem(queue.delay_key, job.id):
                queue.enqueue_job(job)
                self.log.info('Enqueue delay job: {}'.format(blue(job.id)))
