# -*- coding: utf-8 -*-
import time
from tests import RQTestCase
from tests.fixtures import say_hello
from delayrq.queue import DelayQueue
from delayrq.timer import Timer


class TestTimer(RQTestCase):
    def test_dequeue_delay_job_and_enqueue(self):
        q = DelayQueue()
        job = q.enqueue(say_hello, 'Nick', foo='bar', delay=.02)
        timer = Timer(q.name)
        timer.dequeue_delay_job_and_enqueue(0.5)

        self.assertEqual(self.testconn.zcard(q.delay_key), 0)


