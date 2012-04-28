#!/usr/bin/env python2.7

# Add the cwd to the lookup path for modules.
import sys
sys.path.append(".")

# Replace the mesos module with the stub implementation.
import stub_mesos
sys.modules["mesos"] = stub_mesos

import chunk_utils
import mesos_pb2
import unittest

from mock import MagicMock

class TestTaskChunks(unittest.TestCase):

    def setUp(self):
        self.chunk = chunk_utils.newTaskChunk()
        self.subTask = mesos_pb2.TaskInfo()

    def test_isTaskChunk_true(self):
        self.assertTrue(chunk_utils.isTaskChunk(self.chunk))

    def test_isTaskChunk_false(self):
        self.assertFalse(chunk_utils.isTaskChunk(self.subTask))

    def test_numSubTasks(self):
        self.assertEqual(0, chunk_utils.numSubTasks(self.chunk))

    def test_addSubTask(self):
        chunk_utils.addSubTask(self.chunk, self.subTask)
        self.assertEqual(1, chunk_utils.numSubTasks(self.chunk))

    def test_nextSubTask(self):
        chunk_utils.addSubTask(self.chunk, self.subTask)
        self.assertEqual(self.subTask, chunk_utils.nextSubTask(self.chunk))

    def test_nextSubTask_error(self):
        with self.assertRaises(ValueError):
            chunk_utils.nextSubTask(self.chunk)

    def test_removeSubTask(self):
        chunk_utils.addSubTask(self.chunk, self.subTask)
        chunk_utils.removeSubTask(self.chunk, self.subTask.task_id)
        self.assertEqual(0, chunk_utils.numSubTasks(self.chunk))
        with self.assertRaises(ValueError):
            chunk_utils.nextSubTask(self.chunk)

if __name__ == '__main__':
    unittest.main()
