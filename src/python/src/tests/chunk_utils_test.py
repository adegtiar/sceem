#!/usr/bin/env python2.7

import sys
sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import mesos
import mesos_pb2
import unittest

class TestTaskChunks(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
