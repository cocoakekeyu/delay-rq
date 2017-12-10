# -*- coding: utf-8 -*-
import uuid

from delayrq.lock import SimpleLock, NoLock
from tests import RQTestCase


class TestLock(RQTestCase):

    def test_lock(self):
        name = str(uuid.uuid4())
        conn = self.testconn
        with SimpleLock(conn, name) as lock:
            identifier = lock.identifier
            self.assertEqual(self.testconn.get(lock.lockname).decode('utf-8'),
                             identifier)

        self.assertTrue(self.testconn.get("lock:"+name) is None)

    def test_lock_failed(self):
        name = str(uuid.uuid4())
        conn = self.testconn
        conn.set("lock:" + name, "aaaa")
        try:
            with SimpleLock(conn, name, timeout=0.1):
                self.assertTrue(False)
        except NoLock:
            self.assertTrue(True)
