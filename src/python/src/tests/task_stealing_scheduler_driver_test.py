#!/usr/bin/env python

import sys
import threading
import time

sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import mesos_pb2
import os
import unittest
from collections import defaultdict

from task_stealing_scheduler import *
from chunk_utils import *
from mock import Mock, MagicMock

class TestChunkSchedulerDriver(unittest.TestCase):

    def setUp(self):
        self.tid = 0
        self.launchedTask = 0
        self.numExecutor = 0
        self.currExecutor = 0
        self.slave_id = Mock()
        self.slave_id.value = "slave_id"
        self.taskChunk = newTaskChunk(self.slave_id)
        self.taskChunk.task_id.value = str(self.getTaskId())
        self.task = self.getNewSubTask()
        self.mSchedulerDriver = Mock(spec=mesos.SchedulerDriver())
        self.mScheduler = Mock(spec=mesos.Scheduler)
        self.stealingScheduler = self.getStealScheduler()
        self.stealingSchedulerDriver = self.getStealSchedulerDriver()

    def getTaskId(self):
        self.tid = self.launchedTask
        self.launchedTask +=1
        return self.tid

    def getExecutorId(self):
        self.currExecutor = self.numExecutor
        self.numExecutor +=1
        return self.currExecutor

    def getNewSubTask(self):
        subTask = mesos_pb2.TaskInfo()
        subTask.task_id.value = str(self.getTaskId())
        return subTask

    def getStealScheduler(self):
        chunkScheduler = TaskChunkScheduler(self.mScheduler)
        return chunkScheduler

    def getExecutor(self):
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = str(self.getExecutorId())
        executor.command.value = os.path.abspath("./test-executor")
        return executor

    def getSlaveID(self):
        slave = mesos_pb2.SlaveID()
        slave.value = "01"
        return slave

    def getStealSchedulerDriver(self):
        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "Test Framework (Python)"
        stealSchedulerDriver = TaskStealingSchedulerDriver(self.mScheduler, framework, "local")
        return stealSchedulerDriver

    def add_resources(self, taskChunk, sizeRes, numRes,
                      dictRes=None, operator=None):
        for i in xrange(numRes):
            resource = taskChunk.resources.add()
            resource.name = str(i)
            resource.type = mesos_pb2.Value.SCALAR
            resource.scalar.value = sizeRes
            if dictRes!=None:
                dictRes[resource.name] = operator(dictRes[resource.name], sizeRes)



    def test_launchTasks(self):
        offer = mesos_pb2.Offer()
        offer.id.value = "Offer_1"
        self.add_resources(offer, 10, 3)

        tasks = []
        for i in xrange(5):
            task = self.getNewSubTask()
            self.add_resources(task, 2, 3)
            tasks.append(task)

        self.stealingSchedulerDriver.launchTasks(offer.id, tasks)

        self.assertTrue(chunk_utils.numSubTasks(
            self.stealingSchedulerDriver.pendingTasks.rootTask) == 5)

        consumedResources = self.stealingSchedulerDriver.consumedResources[offer.id.value]
        self.assertTrue(len(consumedResources)==3)
        for resource in consumedResources:
            self.assertTrue(resource.scalar.value == 10)

    def test_updateOffers(self):
        offers = []
        
        for i in xrange(5):
            offer = mesos_pb2.Offer()
            offer.id.value = "Offer_"+str(i)
            self.add_resources(offer, 10, 3)
            consumedResources = self.stealingSchedulerDriver.consumedResources[offer.id.value]
            chunk_utils.incrementResources(consumedResources, offer.resources)
            offers.append(offer)
            
        self.stealingSchedulerDriver.updateOffers(offers)
        for offer in offers:
            self.assertTrue(chunk_utils.isOfferEmpty(offer))

    def test_clearConsumedResources(self):
        offers = []
        for i in xrange(5):
            offer = mesos_pb2.Offer()
            offer.id.value = "Offer_"+str(i)
            self.add_resources(offer, 10, 3)
            consumedResources = self.stealingSchedulerDriver.consumedResources[offer.id.value]
            chunk_utils.incrementResources(consumedResources, offer.resources)
            offers.append(offer)
            
        self.stealingSchedulerDriver.clearConsumedResources(offers)

        self.assertTrue(len(self.stealingSchedulerDriver.consumedResources.items())
                        ==0)

 
    def test_killSubtasks(self):
        subTasks = []
        subTaskIds = []
        parentTaskIds = []
        executor1 = self.getExecutor()
        for i in xrange(4):
            subtask = self.getNewSubTask()
            subtask.slave_id.value = self.getSlaveID().value
            subtask.executor.MergeFrom(executor1)
            subTasks.append(subtask)
            subTaskIds.append(subtask.task_id)
            chunk_utils.addSubTask(self.taskChunk, subtask)
            parentTaskIds.append(self.taskChunk.task_id)

        
        self.stealingSchedulerDriver.pendingTasks.addTask(self.taskChunk)
        
        listExpected = zip(parentTaskIds, subTasks)
        listReturn = self.stealingSchedulerDriver.killSubTasks(subTaskIds)
        self.assertTrue(listExpected == listReturn)

#self.assertTrue(self.chunkSchedulerDriver.sendFrameworkMessage.call_args_list ==

if __name__ == '__main__':
    unittest.main()

