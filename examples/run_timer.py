# -*- coding: utf-8 -*-
from rq import Connection
from delayrq import Timer
from delayrq import DelayQueue


if __name__ == '__main__':
    with Connection():
        q = DelayQueue()
        Timer(q).work()
