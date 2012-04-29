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

class TestChunkExecutor(unittest.TestCase):

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
        self.chunkExecutor.pendingTaskChunks.setActive(self.taskChunk.task_id)
        self.chunkExecutor.pendingTaskChunks.setActive(task.task_id)
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
        self.chunkExecutor.pendingTaskChunks.setActive(self.taskChunk.task_id)
        self.chunkExecutor.pendingTaskChunks.setActive(subtaskIds[0])
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
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.taskChunk.task_id.value
        update.state = mesos_pb2.TASK_FINISHED
        self.mExecutorDriver.sendStatusUpdate.assert_called_once_with(update)

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
        
    

class TestHelperMethod():
    
    def __init__():
        pass

    def getMockExecutor(self):
        mExecutor = Mock(spec=mesos.Executor)
        mExecutor.launchTask.return_value = "uExecutor: Launch Task Called"
        mExecutor.killTask.return_value = "uExecutor: KillTask Called"
        mExecutor.frameworkMessage.return_value = "uExecutor: FrameMessage Called"
        return mExecutor
    
    def getMockExecutorDriver(self):
        mExecutorDriver = Mock(spec=mesos.ExecutorDriver)
        return mExecutorDriver
    
    def getChunkExecutor(self):
        # Create a TaskChunk Executor
        chunkExecutor = TaskChunkExecutor(self.mExecutor)
        return chunkExecutor

    
        
class TestChunkExecutorDriver(unittest.TestCase):

    def setUp(self):
        self.tid = 0
        self.launchedTask = 0
        self.taskChunk = newTaskChunk()
        self.taskChunk.task_id.value = str(self.getTaskId())
        self.task = self.getNewSubtask()
        self.mExecutor = Mock(spec=mesos.Executor)
        self.mExecutorDriver = Mock(spec=mesos.ExecutorDriver)
        self.chunkExecutor = self.getChunkExecutor()
        self.chunkExecutorDriver = self.getChunkExecutorDriver()
        
    def getTaskId(self):
        self.tid = self.launchedTask
        self.launchedTask +=1
        return self.tid
    
    def getNewSubtask(self):
        subTask = mesos_pb2.TaskInfo()
        subTask.task_id.value = str(self.getTaskId())
        return subTask

    def getChunkExecutor(self):
        # Create a TaskChunk Executor
        chunkExecutor = TaskChunkExecutor(self.mExecutor)
        return chunkExecutor
    
    def getChunkExecutorDriver(self):
        chunkDriver = TaskChunkExecutorDriver(self.mExecutor)
        return chunkDriver

    def test_sendStatusUpdateSubtaskTerminal(self):
        addSubTask(self.taskChunk, self.task)
        self.mExecutor.sendStatusUpdate = Mock()
        self.chunkExecutorDriver.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutorDriver.chunkExecutor.runNextSubTask = Mock()
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.task.task_id.value
        update.state = mesos_pb2.TASK_FINISHED
        self.chunkExecutorDriver.sendStatusUpdate(update)
        self.chunkExecutorDriver.chunkExecutor.runNextSubTask.assert_called_once_with(self.chunkExecutorDriver, self.taskChunk.task_id)


    def test_sendStatusUpdateSubtask(self):        
        self.mExecutor.sendStatusUpdate = Mock()
        addSubTask(self.taskChunk, self.task)
        self.chunkExecutorDriver.chunkExecutor.pendingTaskChunks.addTask(self.taskChunk)
        self.chunkExecutorDriver.driver.sendStatusUpdate = Mock()
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.taskChunk.task_id.value
        update.state = mesos_pb2.TASK_FINISHED
        self.chunkExecutorDriver.sendStatusUpdate(update)
        self.chunkExecutorDriver.driver.sendStatusUpdate.assert_called_once_with(update)


    #def test_sendStatusUpdate(self):
        
        
        
        
        


    
if __name__ == '__main__':
    unittest.main()

