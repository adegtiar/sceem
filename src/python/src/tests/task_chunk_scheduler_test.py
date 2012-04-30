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

from task_chunk_scheduler import *
from chunk_utils import *
from mock import Mock, MagicMock

class TestChunkScheduler(unittest.TestCase):

    def setUp(self):
        self.tid = 0
        self.launchedTask = 0
        self.taskChunk = newTaskChunk()
        self.taskChunk.task_id.value = str(self.getTaskId())
        self.task = self.getNewSubtask()
        self.mSchedulerDriver = Mock(spec=mesos.SchedulerDriver())
        self.mScheduler = Mock(spec=mesos.Scheduler)
        self.chunkScheduler = self.getChunkScheduler()

    def getTaskId(self):
        self.tid = self.launchedTask
        self.launchedTask +=1
        return self.tid

    def getChunkScheduler(self):
        chunkScheduler = TaskChunkScheduler(self.mScheduler)
        return chunkScheduler

    def getNewSubtask(self):
        subTask = mesos_pb2.TaskInfo()
        subTask.task_id.value = str(self.getTaskId())
        return subTask

    def test_frameworkMessageUpdate(self):
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.taskChunk.task_id.value
        update.state = mesos_pb2.TASK_FINISHED
        self.mScheduler.statusUpdate = Mock()
        updateMessage = chunk_utils.SubTaskUpdateMessage(update)
        message = SubTaskMessage.fromString(data)
        self.chunkScheduler.frameworkMessage(self.mSchedulerDriver, updateMessage)
        self.mScheduler.statusUpdate.assert_called_once_with(self.mSchedulerDriver, update)

    def test_frameworkMessage(self):
        self.chunkScheduler.frameworkMessage(self.mSchedulerDriver, "message")
        self.mScheduler.frameworkMessage.assert_called_once_with(self.mSchedulerDriver, "message")

class TestChunkSchedulerDriver(unittest.TestCase):

    def setUp(self):
        self.tid = 0
        self.launchedTask = 0
        self.numExecutor = 0
        self.currExecutor = 0
        self.taskChunk = newTaskChunk()
        self.taskChunk.task_id.value = str(self.getTaskId())
        self.task = self.getNewSubTask()
        self.mSchedulerDriver = Mock(spec=mesos.SchedulerDriver())
        self.mScheduler = Mock(spec=mesos.Scheduler)
        self.chunkScheduler = self.getChunkScheduler()
        self.chunkSchedulerDriver = self.getChunkSchedulerDriver()

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

    def getChunkScheduler(self):
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

    def getChunkSchedulerDriver(self):
        chunkSchedulerDriver = TaskChunkSchedulerDriver(self.mScheduler)
        return chunkSchedulerDriver

    def test_killSubtasks(self):
        subTasks = []
        subTaskIds = []
        executor1 = self.getExecutor()
        expectedCalls = []
        self.chunkSchedulerDriver.sendFrameworkMessage = Mock()
#executor2 = self.getExecutor()
        for i in xrange(4):
            subtask = self.getNewSubTask()
            subtask.slave_id.value = self.getSlaveID().value
            subtask.executor.MergeFrom(executor1)
            subTasks.append(subtask)
            subTaskIds.append(subtask.task_id)

        message = chunk_utils.KillSubTasksMessage(subTaskIds)
        self.chunkSchedulerDriver.killSubtasks(subTasks)
        self.chunkSchedulerDriver.sendFrameworkMessage.assert_called_once_with(executor1.executor_id, subTasks[0].slave_id, message)

#        self.assertTrue(self.chunkSchedulerDriver.sendFrameworkMessage.call_args_list ==

if __name__ == '__main__':
    unittest.main()

