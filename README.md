# Delay-rq
## Introduction
实现了一个基于RQ (https://github.com/nvie/rq) 的延迟队列。

新增一个DelayQueue用于存放延迟任务，一个Timer不间断轮询DelayQueue将到期执行的任务放入RQ的任务队列。

## Install
`pip install delayrq`

## Usage

### DelayQueue

```python
from rq import Connection
from delayrq import DelayQueue

with Connection():
    q = DelayQueue()
    # New add delay param
    job = q.enqueue(some_func, delay=30) # delay 30s enqueue

```

### Timer

```python
# run_timer.py
from rq import Connection
from delayrq import Timer
from delayrq import DelayQueue

with Connection():
    q = DelayQueue()
    Timer(q).work()

# python run_timer.py
```

## Examples

See examples

## TODO

- [ ] Timer add poll strategy
