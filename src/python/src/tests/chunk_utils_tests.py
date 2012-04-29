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
        self.subTask.task_id.value = "subtask_1"

    def test_isTaskChunk_true(self):
        self.assertTrue(isTaskChunk(self.chunk))

    def test_isTaskChunk_false(self):
        self.assertFalse(isTaskChunk(self.subTask))

    def test_numSubTasks(self):
        self.assertEqual(0, numSubTasks(self.chunk))

    def test_addSubTask(self):
        addSubTask(self.chunk, self.subTask)
        addSubTask(self.chunk, self.subTask)
        self.assertEqual(2, numSubTasks(self.chunk))

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
            subTask.task_id.value = "id{0}".format(i)
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

    def new_tasks(self, num):
        tasks = []
        for i in range(num):
            task = mesos_pb2.TaskInfo()
            task.task_id.value = "id{0}".format(i)
            tasks.append(task)
        return tasks

    def new_task_chunk(self, subTasksPerChunk):
        taskChunk = newTaskChunk(self.new_tasks(subTasksPerChunk))
        taskChunk.task_id.value = "chunk_id"
        return taskChunk

    def test_len(self):
        self.assertEqual(0, len(self.table))

    def test_addTask(self):
        for task in self.new_tasks(2):
            self.table.addTask(task)

        self.assertEqual(2, len(self.table))

    def test_addTask_error(self):
        with self.assertRaises(ValueError):
            self.table.addTask(mesos_pb2.TaskInfo())

    def test_addTask_task_chunk(self):
        self.table.addTask(self.new_task_chunk(2))
        self.assertEqual(3, len(self.table))

    def test_addTask_deep_task_chunk(self):
        innerTaskChunk = self.new_task_chunk(2)

        outerTaskChunk = newTaskChunk((innerTaskChunk,))
        outerTaskChunk.task_id.value = "chunk_id_outer"

        self.table.addTask(outerTaskChunk)
        self.assertEqual(4, len(self.table))

    def test_addTask_same(self):
        for task in self.new_tasks(2):
            self.table.addTask(task)
        for task in self.new_tasks(2):
            self.table.addTask(task)

        self.assertEqual(2, len(self.table))

    def test_contains(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        self.assertTrue(taskChunk.task_id in self.table)
        for task in tasks:
            self.assertTrue(task.task_id in self.table)

        someId = mesos_pb2.TaskID(value="foo")
        self.assertFalse(someId in self.table)

    def test_get(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        self.assertEqual(taskChunk, self.table[taskChunk.task_id])
        for task in tasks:
            self.assertEqual(task, self.table[task.task_id])

    def test_del_sub_task(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        del self.table[tasks[0].task_id]
        self.assertFalse(tasks[0].task_id in self.table)
        self.assertEqual(2, len(self.table))

    def test_del_task_chunk(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        del self.table[taskChunk.task_id]
        self.assertFalse(taskChunk.task_id in self.table)
        for task in tasks:
            self.assertFalse(task.task_id in self.table)
        self.assertEqual(0, len(self.table))

    def test_iter(self):
        all_tasks = []
        innerTaskChunk = self.new_task_chunk(2)

        all_tasks.append(innerTaskChunk)
        for subTask in subTaskIterator(innerTaskChunk):
            all_tasks.append(subTask)

        outerTaskChunk = newTaskChunk((innerTaskChunk,))
        outerTaskChunk.task_id.value = "chunk_id_outer"

        all_tasks.append(outerTaskChunk)

        self.table.addTask(outerTaskChunk)

        num_iter_tasks = 0
        for task in self.table:
            self.assertTrue(task in all_tasks)
            num_iter_tasks += 1
        self.assertEqual(len(all_tasks), num_iter_tasks)

    def test_active(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        # Tasks are initially inactive.
        self.assertFalse(self.table.isActive(taskChunk.task_id))
        for task in tasks:
            self.assertFalse(self.table.isActive(task.task_id))

        self.table.setActive(tasks[0].task_id)
        self.assertTrue(self.table.isActive(tasks[0].task_id))

    def test_getParent(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(tasks)
        taskChunk.task_id.value = "chunk_id"

        outerTaskChunk = newTaskChunk((taskChunk,))
        outerTaskChunk.task_id.value = "chunk_id_outer"

        self.table.addTask(outerTaskChunk)

        self.assertEqual(self.table.rootTask, self.table.getParent(outerTaskChunk.task_id))
        self.assertEqual(outerTaskChunk, self.table.getParent(taskChunk.task_id))
        for task in tasks:
            self.assertEqual(taskChunk, self.table.getParent(task.task_id))

    def test_isSubTask(self):
        tasks = self.new_tasks(4)
        taskChunk = newTaskChunk(tasks[:2])
        taskChunk.task_id.value = "chunk_id"

        outerTaskChunk = newTaskChunk((taskChunk,))
        outerTaskChunk.task_id.value = "chunk_id_outer"

        self.table.addTask(outerTaskChunk)

        self.assertFalse(self.table.isSubTask(outerTaskChunk.task_id))
        self.assertTrue(self.table.isSubTask(taskChunk.task_id))
        for task in tasks[:2]:
            self.assertTrue(self.table.isSubTask(task.task_id))
        for task in tasks[2:]:
            self.assertFalse(self.table.isSubTask(task.task_id))


class TestSubTaskMessage(unittest.TestCase):
    """
    Tests for SubTaskMessage in chunk_utils.
    """

    def test_invalidMessage(self):
        invalidMessage = SubTaskMessage(valid = False)

        self.assertFalse(invalidMessage.isValid())

        with self.assertRaises(NotImplementedError):
            invalidMessage.toString()

        with self.assertRaises(ValueError):
            invalidMessage.getPayload()

        with self.assertRaises(ValueError):
            invalidMessage.getType()



if __name__ == '__main__':
    unittest.main()
