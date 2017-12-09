# -*- coding: utf-8 -*-
from tests import RQTestCase
from tests.fixtures import say_hello
from delayrq.queue import DelayQueue


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = DelayQueue('my-queue')
        self.assertEqual(q.name, 'my-queue')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = DelayQueue()
        self.assertEqual(q.name, 'default')

    def test_enqueue_with_delay(self):
        """Enqueueing job onto queues."""
        q = DelayQueue()
        self.assertEqual(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar', delay=30)
        job_id = job.id
        self.assertEqual(job.origin, q.name)

        # Inspect data inside Redis
        q_key = 'rq:delay_queue:default'
        self.assertEqual(self.testconn.zcard(q_key), 1)
        self.assertEqual(
            self.testconn.zrange(q_key, 0, 0, withscores=True)[0][0].decode('ascii'),
            job_id)

    def test_enqueue_with_no_delay(self):
        """Enqueueing job onto queues."""
        q = DelayQueue()
        self.assertEqual(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar')
        job_id = job.id
        self.assertEqual(job.origin, q.name)

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEqual(self.testconn.llen(q_key), 1)
        self.assertEqual(
            self.testconn.lrange(q_key, 0, -1)[0].decode('ascii'),
            job_id)
