#!/usr/bin/env python

import sys

sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import mesos_pb2
import unittest

from mock import MagicMock
from task_stealing_scheduler import *

class TestChunkScheduler(unittest.TestCase):

    def setUp(self):
        self.mSchedulerDriver = MagicMock(spec=TaskStealingSchedulerDriver)
        self.mScheduler = MagicMock(spec=mesos.Scheduler)
        self.stealingScheduler = TaskChunkScheduler(self.mScheduler)

    def test_resourceOffers(self):
        pass

if __name__ == '__main__':
    unittest.main()
