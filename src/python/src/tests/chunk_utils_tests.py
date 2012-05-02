#!/usr/bin/env python

# Add the cwd to the lookup path for modules.
import sys
sys.path.append(".")

# Replace the mesos module with the stub implementation.
import stub_mesos
sys.modules["mesos"] = stub_mesos

from chunk_utils import *
import mesos_pb2
import unittest
from mock import Mock

isPython27 = (sys.version_info >= (2,7))

class TestTaskChunks(unittest.TestCase):
    """
    Tests for the base task chunk utilities.
    """

    def setUp(self):
        self.slave_id = Mock()
        self.slave_id.value = "slave_id"
        self.chunk = newTaskChunk(self.slave_id)
        self.subTask = mesos_pb2.TaskInfo()
        self.subTask.task_id.value = "subtask_1"

    def test_withUtils(self):
        if isPython27:
            self.nextSubTask_error()
            self.removeSubTask()

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

    def nextSubTask_error(self):
        with self.assertRaises(ValueError):
            nextSubTask(self.chunk)

    def removeSubTask(self):
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
    Tests for the TaskTable.
    """

    def setUp(self):
        self.table = TaskTable()
        self.slave_id = Mock()
        self.slave_id.value = "slave_id"

    def new_tasks(self, num):
        tasks = []
        for i in range(num):
            task = mesos_pb2.TaskInfo()
            task.task_id.value = "id{0}".format(i)
            tasks.append(task)
        return tasks

    def new_task_chunk(self, subTasksPerChunk):
        taskChunk = newTaskChunk(self.slave_id,
                                 subTasks=self.new_tasks(subTasksPerChunk))
        taskChunk.task_id.value = "chunk_id"
        return taskChunk

    def add_resources(self, taskChunk, sizeRes, numRes,
                      dictRes=None, operator=None):
        for i in xrange(numRes):
            resource = taskChunk.resources.add()
            resource.name = str(i)
            resource.type = mesos_pb2.Value.SCALAR
            resource.scalar.value = sizeRes
            if dictRes!=None:
                dictRes[resource.name] = operator(dictRes[resource.name], sizeRes)

    def test_with(self):
        if isPython27:
            self.addTask_error()


    def test_len(self):
        self.assertEqual(0, len(self.table))

    def test_incrementResources(self):
        dictRes = defaultdict(int)
        numResPerTask = 5

        taskChunk = newTaskChunk(self.slave_id)
        self.add_resources(taskChunk, 5, numResPerTask, dictRes,operator.add)

        taskChunk2 = newTaskChunk(self.slave_id)
        self.add_resources(taskChunk2, 10, numResPerTask, dictRes, operator.add)

        incrementResources(taskChunk, taskChunk2)
        for resource in taskChunk.resources:
            self.assertTrue(dictRes[resource.name] == resource.scalar.value)

    def test_decrementResources(self):
        dictRes = defaultdict(int)
        numResPerTask = 5

        taskChunk = newTaskChunk(self.slave_id)
        self.add_resources(taskChunk, 5, numResPerTask, dictRes, operator.add)

        taskChunk2 = newTaskChunk(self.slave_id)
        self.add_resources(taskChunk2, 10, numResPerTask, dictRes, operator.sub)

        decrementResources(taskChunk, taskChunk2)
        for resource in taskChunk.resources:
            self.assertTrue(dictRes[resource.name] == resource.scalar.value)


    def test_maxResources(self):
        dictRes = defaultdict(int)
        numResPerTask = 5

        taskChunk = newTaskChunk(self.slave_id)
        self.add_resources(taskChunk, 5, numResPerTask, dictRes, max)

        taskChunk2 = newTaskChunk(self.slave_id)
        self.add_resources(taskChunk2, 10, numResPerTask, dictRes, max)

        maxResources(taskChunk, taskChunk2)
        for resource in taskChunk.resources:
            self.assertTrue(dictRes[resource.name] == resource.scalar.value)

    def test_isOfferValid(self):
        offer = mesos_pb2.Offer()
        self.add_resources(offer, -1, 1)
        self.assertFalse(isOfferValid(offer))

        offer = mesos_pb2.Offer()
        self.add_resources(offer, 2, 3)
        self.assertTrue(isOfferValid(offer))

        self.add_resources(offer, -2, 1)
        self.assertFalse(isOfferValid(offer))

    def test_isOfferEmpty(self):
        offer = mesos_pb2.Offer()
        self.add_resources(offer, 5, 5)
        self.assertFalse(isOfferEmpty(offer))

        self.add_resources(offer, 0, 1)
        self.assertFalse(isOfferEmpty(offer))

        offer = mesos_pb2.Offer()
        self.add_resources(offer, 0, 5)
        self.assertTrue(isOfferEmpty(offer))

    def test_addTask(self):
        for task in self.new_tasks(2):
            self.table.addTask(task)

        self.assertEqual(2, len(self.table))

    def addTask_error(self):
        with self.assertRaises(ValueError):
            self.table.addTask(mesos_pb2.TaskInfo())

    def test_addTask_task_chunk(self):
        self.table.addTask(self.new_task_chunk(2))
        self.assertEqual(3, len(self.table))

    def test_addTask_deep_task_chunk(self):
        innerTaskChunk = self.new_task_chunk(2)

        outerTaskChunk = newTaskChunk(self.slave_id,subTasks=(innerTaskChunk,))
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
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        self.assertTrue(taskChunk.task_id in self.table)
        for task in tasks:
            self.assertTrue(task.task_id in self.table)

        someId = mesos_pb2.TaskID(value="foo")
        self.assertFalse(someId in self.table)

    def test_get(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        self.assertEqual(taskChunk, self.table[taskChunk.task_id])
        for task in tasks:
            self.assertEqual(task, self.table[task.task_id])

    def test_del_sub_task(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        del self.table[tasks[0].task_id]
        self.assertFalse(tasks[0].task_id in self.table)
        self.assertEqual(2, len(self.table))

    def test_del_task_chunk(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks)
        taskChunk.task_id.value = "chunk_id"

        self.table.addTask(taskChunk)

        del self.table[taskChunk.task_id]
        self.assertFalse(taskChunk.task_id in self.table)
        for task in tasks:
            self.assertFalse(task.task_id in self.table)
        self.assertEqual(0, len(self.table))

    # TODO: make this test better
    def test_iter(self):
        all_tasks = []
        innerTaskChunk = self.new_task_chunk(2)

        all_tasks.append(innerTaskChunk)
        for subTask in subTaskIterator(innerTaskChunk):
            all_tasks.append(subTask)

        outerTaskChunk = newTaskChunk(self.slave_id, subTasks=(innerTaskChunk,))
        outerTaskChunk.task_id.value = "chunk_id_outer"

        all_tasks.append(outerTaskChunk)

        self.table.addTask(outerTaskChunk)

        num_iter_tasks = 0
        for task in self.table:
            self.assertTrue(task in all_tasks)
            num_iter_tasks += 1
        self.assertEqual(1, num_iter_tasks)

    def test_active(self):
        tasks = self.new_tasks(2)
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks)
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
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks)
        taskChunk.task_id.value = "chunk_id"

        outerTaskChunk = newTaskChunk(self.slave_id, subTasks=(taskChunk,))
        outerTaskChunk.task_id.value = "chunk_id_outer"

        self.table.addTask(outerTaskChunk)

        self.assertEqual(self.table.rootTask,
                self.table.getParent(outerTaskChunk.task_id))
        self.assertEqual(outerTaskChunk,
                self.table.getParent(taskChunk.task_id))
        for task in tasks:
            self.assertEqual(taskChunk, self.table.getParent(task.task_id))

    def test_isSubTask(self):
        tasks = self.new_tasks(4)
        taskChunk = newTaskChunk(self.slave_id, subTasks=tasks[:2])
        taskChunk.task_id.value = "chunk_id"

        outerTaskChunk = newTaskChunk(self.slave_id, subTasks=(taskChunk,))
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
    Tests for SubTaskMessage.
    """

    def setUp(self):
        self.payload = 513948
        self.messageType = 3
        self.message = SubTaskMessage(self.messageType, self.payload)

    def test_with(self):
        if isPython27:
            self.invalidMessage()

    def invalidMessage(self):
        invalidMessage = SubTaskMessage(valid = False)

        self.assertFalse(invalidMessage.isValid())

        with self.assertRaises(ValueError):
            invalidMessage.toString()

        with self.assertRaises(ValueError):
            invalidMessage.getPayload()

        with self.assertRaises(ValueError):
            invalidMessage.getType()

    def test_parseUnknownMessageType(self):
        serializedMessage = pickle.dumps((123, "foo"))
        unknownMessage = SubTaskMessage.fromString(serializedMessage)

        self.assertFalse(unknownMessage.isValid())

    def test_parseInvalidMessage(self):
        invalidMessage = SubTaskMessage.fromString("invalid stuff")
        self.assertFalse(invalidMessage.isValid())

    def test_validMessage(self):
        self.assertTrue(self.message.isValid())

    def test_getPayload(self):
        self.assertEqual(self.payload, self.message.getPayload())

    def test_getType(self):
        self.assertEqual(self.messageType, self.message.getType())

    def test_eq(self):
        payload = self.payload
        messageType = self.messageType
        message = SubTaskMessage(messageType, payload)
        self.assertEqual(self.message, message)


class TestSubTaskSerialization(object):

    def setUp(self):
        """
        Implement this to store self.payload and self.serializationClass.
        """
        raise NotImplemented()

    def test_serialization(self):
        serialized = self.serializationClass.payloadToString(self.payload)
        deserialized = self.serializationClass.payloadFromString(serialized)

        self.assertEqual(self.payload, deserialized)

    def test_endToEnd(self):
        message = self.serializationClass(self.payload)
        serialized = message.toString()
        deserialized = SubTaskMessage.fromString(serialized)

        self.assertEqual(message, deserialized)


class TestSubTaskUpdateMessage(unittest.TestCase, TestSubTaskSerialization):
    """
    Tests for SubTaskUpdateMessage.
    """

    def setUp(self):
        taskStatus = mesos_pb2.TaskStatus()
        taskStatus.task_id.value = "id"
        taskStatus.state = mesos_pb2.TASK_RUNNING
        taskStatus.message = "foo message"
        taskStatus.data = "foo data"
        self.payload = taskStatus
        self.serializationClass = SubTaskUpdateMessage


class TestKillSubTasksMessage(unittest.TestCase, TestSubTaskSerialization):
    """
    Tests for SubTaskUpdateMessage.
    """

    def setUp(self):
        taskIds = [mesos_pb2.TaskID(value="{0}".format(i)) for i in xrange(5)]
        self.payload = taskIds
        self.serializationClass = KillSubTasksMessage


if __name__ == '__main__':
    unittest.main()
