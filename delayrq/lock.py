# -*- coding: utf-8 -*-
import uuid
import time


class NoLock(Exception):
    "Raised when a lock cannot be acquired"


class SimpleLock(object):
    """This a simple redis lock"""

    def __init__(self, connection, lockname, timeout=10, lock_timeout=10):
        self.connection = connection
        self.lockname = "lock:" + lockname
        self.timeout = timeout
        self.lock_timeout = lock_timeout
        self.identifier = str(uuid.uuid4())

    def __enter__(self):
        conn = self.connection
        lockname = self.lockname
        lock_timeout = self.lock_timeout
        identifier = self.identifier
        end_time = time.time() + self.timeout

        while time.time() < end_time:
            if conn.setnx(lockname, identifier):
                conn.expire(lockname, lock_timeout)
                return self
            elif not conn.ttl(lockname):
                conn.expire(lockname, lock_timeout)

            time.sleep(.001)

        raise NoLock()

    def __exit__(self, type, value, traceback):
        conn = self.connection
        lockname = self.lockname

        item = conn.get(lockname)
        if item and item.decode('utf-8') == self.identifier:
            conn.delete(lockname)
