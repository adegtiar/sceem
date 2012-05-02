#!/usr/bin/env python

import sys

sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import chunk_utils
import itertools
import mesos
import mesos_pb2
import unittest

from mock import MagicMock
from task_stealing_scheduler import *

class TestChunkScheduler(unittest.TestCase):

    def setUp(self):
        self.counter = defaultdict(itertools.count)

        self.slave_id = mesos_pb2.SlaveID()
        self.slave_id.value = "slave_id"

        self.executor_id = mesos_pb2.ExecutorID()
        self.executor_id.value = "executor_id"

        self.driver = MagicMock(spec=TaskStealingSchedulerDriver)
        self.scheduler = MagicMock(spec=mesos.Scheduler)
        self.stealingScheduler = TaskStealingScheduler(self.scheduler)

    def nextId(self, offer_type):
        return "{0}_id_{1}".format(offer_type, self.counter[offer_type].next())

    def addResource(self, container, name, amount=1):
        resource = container.resources.add()
        resource.name = name
        resource.type = mesos_pb2.Value.SCALAR
        resource.scalar.value = amount

    def generateOffer(self, resourceAmount=4):
        offer = mesos_pb2.Offer()
        offer.id.value = self.nextId("offer")
        offer.framework_id.value = self.nextId("framework")
        offer.slave_id.value = self.nextId("slave")
        offer.hostname = self.nextId("local")

        self.addResource(offer, "cpus", resourceAmount)
        self.addResource(offer, "mem", resourceAmount)

        return offer

    def newTasks(self, num):
        tasks = []
        for i in range(num):
            task = mesos_pb2.TaskInfo()
            task.task_id.value = self.nextId("task")

            self.addResource(task, "cpus", i+1)
            self.addResource(task, "mem", i+1)

            tasks.append(task)
        return tasks

    def newTaskChunk(self, numSubTasks):
        taskChunk = chunk_utils.newTaskChunk(self.slave_id,
                                 subTasks=self.newTasks(numSubTasks))
        taskChunk.task_id.value = self.nextId("chunk")
        return taskChunk

    # Test modifies a global variable, breaking other tests.
    # TODO: Re-add this when fixed.
    def resourceOffers(self):
        offers = [self.generateOffer()]

        stealing = MagicMock()

        TaskStealingScheduler.resourceOffersStealing = stealing.first
        TaskChunkScheduler.resourceOffers = stealing.second

        self.stealingScheduler.resourceOffers(self.driver, offers)

        # Stealing offer is called first.
        TaskStealingScheduler.resourceOffersStealing.assert_called_once_with(
                self.stealingScheduler, self.driver, offers)
        # Then offers are updated.
        self.driver.updateOffers.assert_called_once_with(offers)
        # Underlying resourceOffers is called second.
        TaskChunkScheduler.resourceOffers.assert_called_once_with(
                self.stealingScheduler, self.driver, offers)

    def newSubTaskUpdate(self):
        self.taskStatus = mesos_pb2.TaskStatus()
        self.taskStatus.task_id.value = "id"
        self.taskStatus.state = mesos_pb2.TASK_RUNNING
        self.taskStatus.message = "foo message"
        self.taskStatus.data = "foo data"

        self.parentId = mesos_pb2.TaskID()
        self.parentId.value = "task_chunk"

        payload = (self.parentId, self.taskStatus)
        return chunk_utils.SubTaskUpdateMessage(payload).toString()

    def test_resourceOffersStealing(self):
        offers = [self.generateOffer()]
        tasks = [self.newTaskChunk(2)]
        self.driver.pendingTasks = MagicMock()

        scheduler = self.stealingScheduler

        scheduler.selectTasksToSteal = MagicMock()
        scheduler.selectTasksToSteal.return_value = {offers[0].id.value: tasks}
        scheduler.stealSubTasks = MagicMock()

        scheduler.resourceOffersStealing(self.driver, offers)

        scheduler.selectTasksToSteal.assert_called_once()
        scheduler.stealSubTasks.assert_called_once_with(self.driver, tasks)
        self.driver.launchTasks.assert_called_once_with(offers[0].id, tasks)

    def test_selectTasksToSteal(self):
        offers = [self.generateOffer()]
        pendingTasks = [self.newTaskChunk(4)]

        self.stealingScheduler.generateTaskId = MagicMock()
        self.stealingScheduler.generateTaskId.return_value = "chunk_id_0"

        stolen = self.stealingScheduler.selectTasksToSteal(self.driver, offers, pendingTasks)

        self.stealingScheduler.generateTaskId.assert_called_once()
        self.assertEqual(1, len(stolen))
        self.assertTrue(offers[0].id.value in stolen)

        taskChunks = stolen[offers[0].id.value]
        self.assertEqual(1, len(taskChunks))

        taskChunk = taskChunks[0]
        self.assertEqual(2, chunk_utils.numSubTasks(taskChunk))

        stolenTasks = [subTask for subTask in chunk_utils.subTaskIterator(taskChunk)]

        self.assertEqual("task_id_2", stolenTasks[0].task_id.value)
        self.assertEqual("task_id_3", stolenTasks[1].task_id.value)

    def test_selectTasksToStealOfferSplitting(self):
        offers = [self.generateOffer(8)]
        pendingTasks = [self.newTaskChunk(4)]

        stolen = self.stealingScheduler.selectTasksToSteal(self.driver, offers, pendingTasks)

        self.assertEqual(1, len(stolen))
        self.assertTrue(offers[0].id.value in stolen)

        taskChunks = stolen[offers[0].id.value]
        self.assertEqual(2, len(taskChunks))

        # First stolen chunk.
        taskChunk = taskChunks[0]
        self.assertEqual(2, chunk_utils.numSubTasks(taskChunk))

        stolenTasks = [subTask for subTask in chunk_utils.subTaskIterator(taskChunk)]

        self.assertEqual("task_id_2", stolenTasks[0].task_id.value)
        self.assertEqual("task_id_3", stolenTasks[1].task_id.value)

        # Second stolen chunk.
        taskChunk = taskChunks[1]
        self.assertEqual(1, chunk_utils.numSubTasks(taskChunk))

        stolenTasks = [subTask for subTask in chunk_utils.subTaskIterator(taskChunk)]

        self.assertEqual("task_id_1", stolenTasks[0].task_id.value)

    def test_frameworkMessageNormal(self):
        oldMethod = TaskChunkScheduler.frameworkMessage

        TaskChunkScheduler.frameworkMessage = MagicMock()

        self.stealingScheduler.frameworkMessage(self.driver, self.executor_id,
                self.slave_id, "not a SubTaskMessage")

        TaskChunkScheduler.frameworkMessage.assert_called_once()

        TaskChunkScheduler.frameworkMessage = oldMethod

    def test_frameworkMessageNotStolen(self):
        data = self.newSubTaskUpdate()

        oldMethod = TaskChunkScheduler.frameworkMessage

        TaskChunkScheduler.frameworkMessage = MagicMock()

        self.stealingScheduler.frameworkMessage(self.driver, self.executor_id,
                self.slave_id, data)

        TaskChunkScheduler.frameworkMessage.assert_called_once_with(
                self.stealingScheduler, self.driver, self.executor_id,
                self.slave_id, data)

        TaskChunkScheduler.frameworkMessage = oldMethod

    def test_frameworkMessageStolen(self):
        data = self.newSubTaskUpdate()
        stolenTaskIdEntry = (self.parentId.value, self.taskStatus.task_id.value)
        self.stealingScheduler.stolenTaskIds = [stolenTaskIdEntry]

        oldMethod = TaskChunkScheduler.frameworkMessage

        TaskChunkScheduler.frameworkMessage = MagicMock()

        self.stealingScheduler.frameworkMessage(self.driver, self.executor_id,
                self.slave_id, data)

        self.assertEqual(0, len(TaskChunkScheduler.frameworkMessage.mock_calls))

        TaskChunkScheduler.frameworkMessage = oldMethod

    def test_frameworkMessageE2E(self):
        data = self.newSubTaskUpdate()

        try:
            self.stealingScheduler.frameworkMessage(self.driver, self.executor_id,
                    self.slave_id, data)
            self.assertTrue(False)
        except AttributeError:
            pass


if __name__ == '__main__':
    unittest.main()
