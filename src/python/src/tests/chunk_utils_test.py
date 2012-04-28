#!/usr/bin/env python2.7

# Add the cwd to the lookup path for modules.
import sys
sys.path.append(".")

# Replace the mesos module with the stub implementation.
import stub_mesos
sys.modules["mesos"] = stub_mesos

from chunk_utils import *
import mesos_pb2
import unittest


class TestTaskChunks(unittest.TestCase):
    """
    Tests for the base task chunk utilities.
    """

    def setUp(self):
        self.chunk = newTaskChunk()
        self.subTask = mesos_pb2.TaskInfo()

    def test_isTaskChunk_true(self):
        self.assertTrue(isTaskChunk(self.chunk))

    def test_isTaskChunk_false(self):
        self.assertFalse(isTaskChunk(self.subTask))

    def test_numSubTasks(self):
        self.assertEqual(0, numSubTasks(self.chunk))

    def test_addSubTask(self):
        addSubTask(self.chunk, self.subTask)
        self.assertEqual(1, numSubTasks(self.chunk))

    def test_nextSubTask(self):
        addSubTask(self.chunk, self.subTask)
        self.assertEqual(self.subTask, nextSubTask(self.chunk))

    def test_nextSubTask_error(self):
        with self.assertRaises(ValueError):
            nextSubTask(self.chunk)

    def test_removeSubTask(self):
        addSubTask(self.chunk, self.subTask)
        removeSubTask(self.chunk, self.subTask.task_id)
        self.assertEqual(0, numSubTasks(self.chunk))
        with self.assertRaises(ValueError):
            nextSubTask(self.chunk)

    def test_subTaskIterator(self):
        subTasks = []
        for i in range(5):
            subTask = mesos_pb2.TaskInfo()
            subTask.task_id.value = "{0}".format(i)
            subTasks.append(subTask)
            addSubTask(self.chunk, subTask)
        extracted = [task for task in subTaskIterator(self.chunk)]
        self.assertEqual(subTasks, extracted)


class TestTaskTable(unittest.TestCase):
    """
    Tests for the TaskTable in chunk_utils.
    """

    def setUp(self):
        self.table = TaskTable()


if __name__ == '__main__':
    unittest.main()
