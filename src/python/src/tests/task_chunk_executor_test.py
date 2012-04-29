import sys
import threading
import time

sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import mesos_pb2
import unittest

from task_chunk_executor import *
from chunk_utils import *
from mock import Mock, MagicMock

SUBTASK_UPDATE, KILL_SUBTASKS = range(2)

class MockTestExecutor(unittest.TestCase):
  

    def setUp(self):
        self.tid = 0
        self.launchedTask = 0
        self.taskChunk = newTaskChunk()
        self.taskChunk.task_id.value = str(self.getTaskId())
        self.task = self.getNewSubtask()
        self.mExecutor = self.getMockExecutor()
        self.mExecutorDriver = self.getMockExecutorDriver()
        self.chunkExecutor = self.getChunkExecutor()
        
    def getTaskId(self):
        self.tid = self.launchedTask
        self.launchedTask +=1
        return self.tid
    
    def getNewSubtask(self):
        subTask = mesos_pb2.TaskInfo()
        subTask.task_id.value = str(self.getTaskId())
        return subTask

    # crete a mock Executor
    def getMockExecutor(self):
        mExecutor = Mock(spec=mesos.Executor)
        mExecutor.launchTask.return_value = "uExecutor: Launch Task Called"
        mExecutor.killTask.return_value = "uExecutor: KillTask Called"
        mExecutor.frameworkMessage.return_value = "uExecutor: FrameMessage Called"
        return mExecutor
    
    def getMockExecutorDriver(self):
        mExecutorDriver = Mock(spec=mesos.ExecutorDriver)
        mExecutorDriver.sendStatusUpdate.return_value = "StatusUpdateCalled"
        mExecutorDriver.getMessage = Mock()
        mExecutorDriver.getMessage.return_value = (KILL_SUBTASKS, 
                                                   [self.task.task_id])
        return mExecutorDriver
        
    def getChunkExecutor(self):
        # Create a TaskChunk Executor
        chunkExecutor = TaskChunkExecutor(self.mExecutor)
        return chunkExecutor
        
    def test_launchTaskChunks(self):
        task = self.getNewSubtask()
        addSubTask(self.taskChunk,task)
        self.chunkExecutor.runNextSubTask = Mock()
        self.chunkExecutor.launchTask(self.mExecutorDriver,self.taskChunk)
        self.chunkExecutor.runNextSubTask.assert_called_once_with(
            self.mExecutorDriver, self.taskChunk.task_id)

    def test_launchTask(self):
        task = self.getNewSubtask()        
        self.chunkExecutor.launchTask(self.mExecutorDriver, task)
        self.mExecutor.launchTask.assert_called_once_with(self.mExecutorDriver, 
                                                          task)

    def test_killTaskChunks(self): #kill currently running Task Chunk
        task = self.getNewSubtask()
        addSubTask(self.taskChunk,task)
        self.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutor.pendingTaskChunks.updateState(self.taskChunk.task_id,
                                                          mesos_pb2.TASK_RUNNING)
        self.chunkExecutor.pendingTaskChunks.updateState(task.task_id,
                                                          mesos_pb2.TASK_RUNNING)
        self.chunkExecutor.killTask(self.mExecutorDriver, self.taskChunk.task_id)
        self.mExecutor.killTask.assert_called_once_with(self.mExecutorDriver,
                                                        task.task_id)

    def test_killTask(self): #Kill currently running Task
        task = self.getNewSubtask()
        #self.chunkExecutor.pendingTaskChunks.addTask(task)
        #self.chunkExecutor.pendingTaskChunks.updateStatus(task.task_id, mesos_pb2.TASK_RUNNING)
        self.chunkExecutor.killTask(self.mExecutorDriver, task.task_id)
        self.mExecutor.killTask.assert_called_once_with(self.mExecutorDriver,
                                                        task.task_id)

    def test_killSubtasks(self):
        subtaskIds = []
        for i in xrange(5):
            task = self.getNewSubtask()
            subtaskIds.append(task.task_id)
            addSubTask(self.taskChunk, task)
        self.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutor.killSubTasks(self.mExecutorDriver, subtaskIds)
        
        for taskid in subtaskIds:
            self.assertFalse(taskid in self.chunkExecutor.pendingTaskChunks)

    def test_killSubtasksRunning(self):
        subtaskIds = []
        for i in xrange(5):
            task = self.getNewSubtask()
            subtaskIds.append(task.task_id)
            addSubTask(self.taskChunk, task)
        self.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutor.pendingTaskChunks.updateState(subtaskIds[0], mesos_pb2.TASK_RUNNING)
        self.chunkExecutor.killSubTasks(self.mExecutorDriver, subtaskIds)
        
        for taskid in subtaskIds:
            self.assertFalse(taskid in self.chunkExecutor.pendingTaskChunks)
        
        self.mExecutor.killTask.assert_called_once_with(self.mExecutorDriver,
                                                        subtaskIds[0])

    def test_runNextSubtask(self):
        subTask = self.getNewSubtask()
        addSubTask(self.taskChunk, subTask)
        self.chunkExecutor.launchTask = Mock()
        self.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutor.runNextSubTask(self.mExecutorDriver, self.taskChunk.taskId)
        self.chunkExecutor.launchTask.assert_called_once_with(self.mExecutorDriver
                                                              ,subTask.task_id)
        
    def test_runNextSubtask(self): #empty subtask
        self.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutor.runNextSubTask(self.mExecutorDriver, self.taskChunk.task_id)
        self.mExecutorDriver.sendStatusUpdate.assert_called_once_with(mesos_pb2.TaskStatus(self.taskChunk.task_id, mesos_pb2.TASK_FINISHED))

    def test_frameworkMessage(self):
        addSubTask(self.taskChunk, self.task)
        self.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutor.frameworkMessage(self.mExecutorDriver, "message")
        self.mExecutorDriver.getMessage.assert_called_once()
        
        self.assertFalse(self.task.task_id in self.chunkExecutor.pendingTaskChunks)
 
    def test_frameworkMessage(self):
        self.mExecutorDriver.getMessage.return_value = ("1", "2")
        self.chunkExecutor.frameworkMessage(self.mExecutorDriver, "message")
        self.mExecutor.frameworkMessage.assert_called_once_with(self.mExecutorDriver, "message")
        
        
        

        
        
        
        


    
if __name__ == '__main__':
    unittest.main()

