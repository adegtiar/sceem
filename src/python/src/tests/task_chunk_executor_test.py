import sys
import threading
import time

sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import mesos
import mesos_pb2
import unittest
import task_chunk_executor
from mock import Mock

class MockTestExecutor:
    

    def __init__(self):
        self.tid = 0
        self.launchedTask = 0

    def getTaskId(self):
        self.tid = self.launchedTask
        self.launchedTask +=1
        return self.tid
# crete a mock Executor
    def getMockExecutor(self):
        mExecutor = Mock(spec=mesos.Executor)
        mExecutor.registered.assert_called_once_with(driver, executorInfo, frameworkInfo, slaveInfo)
        mExecutor.reregistered.assert_called_once_with(driver, slaveInfo)
        mExecutor.disconnected.assert_called_once_with(driver)
        mExecutor.launchTask.assert_called_once_with(driver, task)
        mExecutor.launchTask.return_value = "Underlying Executor: Launch Task Called"
        mExecutor.killTask.assert_called_once_with(driver, taskId)
        mExecutor.killTask.return_value = "Underlying Executor: KillTask Called"
        mExecutor.frameworkMessage.assert_called_once_with(driver, message)
        mExecutor.frameworkMessage.return_value = "Underlying Executor: FrameworkMessage Called"
        
        mExecutor.shutdown.assert_called_once_with(driver)
        return mExecutor
    
    def getMockExecutorDriver(self):
        
        mExecutorDriver = Mock(spec=mesos.ExecutorDriver)
        mExecutorDriver.sendStatusUpdate.return_value = "StatusUpdateCalled"
        mExecutorDriver.assert_called_once_with(status)
        
        
    def getChunkExecutor(self, executor):
        # Create a TaskChunk Executor
        chunkExecutor = task_chunk_executor.TaskChunkExecutor(mExecutor)
        
        
    def createTaskChunk(self):
        
        TASK_CPUS = 1
        TASK_MEM = 32
        
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(getTaskId())
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % tid
        task.executor.MergeFrom(chunkExecutor)
        
        subTask = task.sub_tasks.add()
        subTask.task_id.value = str(getTaskId())
        launchedTask += 1
        subTask.slave_id.value = offer.slave_id.value
        subTask.name = "task %d" % subtask.task_id.value
        subTask.executor.MergeFrom(self.executor)
        
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
        return task


mockTest = MockTestExecutor()
mExecutor = mockTest.getMockExecutor()
chunkExecutor = mockTest.getChunkExecutor(mExecutor)
mExecutorDriver = mockTest.getMockExecutorDriver()
taskChunk = mockTest.createTaskChunk()
