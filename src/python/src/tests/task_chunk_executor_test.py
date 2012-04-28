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

class MockTestExecutor(unittest.TestCase):
    
    def setUp(self):
        self.tid = 0
        self.launchedTask = 0
        self.mExecutor = self.getMockExecutor()
        self.mExecutorDriver = self.getMockExecutorDriver()
        self.chunkExecutor = self.getChunkExecutor()
        self.taskChunk = self.createTaskChunk()

    def getTaskId(self):
        self.tid = self.launchedTask
        self.launchedTask +=1
        return self.tid

# crete a mock Executor
    def getMockExecutor(self):
        self.mExecutor = Mock(spec=mesos.Executor)
        """
        mExecutor.registered.assert_called_once_with(self.mExecutorDriver, executorInfo, frameworkInfo, slaveInfo)
        mExecutor.reregistered.assert_called_once_with(driver, slaveInfo)
        mExecutor.disconnected.assert_called_once_with(driver)
        

        mExecutor.launchTask.assert_called_once_with(self.mExecutorDriver, self.t)
        """
        self.mExecutor.launchTask = Mock()
        self.mExecutor.launchTask.return_value = "Underlying Executor: Launch Task Called"
        #mExecutor.killTask.assert_called_once_with(driver, taskId)
        self.mExecutor.killTask.return_value = "Underlying Executor: KillTask Called"
        #mExecutor.frameworkMessage.assert_called_once_with(driver, message)
        self.mExecutor.frameworkMessage.return_value = "Underlying Executor: FrameworkMessage Called"
        
        #mExecutor.shutdown.assert_called_once_with(driver)
        return self.mExecutor
    
    def getMockExecutorDriver(self):
        
        mExecutorDriver = Mock(spec=mesos.ExecutorDriver)
        mExecutorDriver.sendStatusUpdate.return_value = "StatusUpdateCalled"
        return mExecutorDriver
        #mExecutorDriver.assert_called_once_with(status)
        
        
    def getChunkExecutor(self):
        # Create a TaskChunk Executor
        chunkExecutor = task_chunk_executor.TaskChunkExecutor(self.mExecutor)
        return chunkExecutor
        
    
    def createTask(self):
        
        TASK_CPUS = 1
        TASK_MEM = 32
        SLAVE_ID = "001"
        
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(self.getTaskId())
        task.slave_id.value = SLAVE_ID
        task.name = "task %d" % self.tid
#        task.executor.MergeFrom(self.chunkExecutor)
        
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
        return task
        
    def createTaskChunk(self):
        
        TASK_CPUS = 1
        TASK_MEM = 32
        SLAVE_ID = "001"
        
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(self.getTaskId())
        task.slave_id.value = SLAVE_ID
        task.name = "task %d" % self.tid
 #       task.executor.MergeFrom(self.chunkExecutor)
        
        subTask = task.sub_tasks.tasks.add()
        subTask.task_id.value = str(self.getTaskId())
        subTask.slave_id.value = SLAVE_ID
        subTask.name = "task %d" % self.tid
  #      subTask.executor.MergeFrom(self.chunkExecutor)
        
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
        return task

    def test_launchTaskChunks(self):
#        self.mExecutor.assert_called_once_with(self.mExecutorDriver, self.taskChunk)
        self.chunkExecutor.runNextSubTask.assert_called_once_with(self.mExecutorDriver, self.task.task_id.value)
        self.chunkExecutor.launchTask(self.mExecutorDriver,self.taskChunk)

    def test_launchTask(self):
        task = self.createTask()
        self.chunkExecutor.launchTask(self.mExecutorDriver, task)
        self.mExecutor.launchTask.assert_called_once_with(self.mExecutorDriver, task)

    def test_killTask(self):
        pass
        #self.mExecutor.launchTask(self.mExecutorDriver, "boo")

        #self.assertFalse(chunk_utils.isTaskChunk(task))
        #self.chunkExecutor.launchTask(self.mExecutorDriver, task)
                            
if __name__ == '__main__':
    unittest.main()

