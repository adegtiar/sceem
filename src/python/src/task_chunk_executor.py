import mesos
import mesos_pb2
import chunk_utils
from chunk_utils import SubTaskMessage

class TaskChunkExecutor(chunk_utils.ExecutorWrapper):

    def __init__(self, executor):
        """
        Initialize TaskTable and executorWrapper with executor.

        """
        chunk_utils.ExecutorWrapper.__init__(self, executor)
        self.pendingTaskChunks = chunk_utils.TaskTable()
        #super(TaskChunkExecutor, self).__init__(self, executor)

    def launchTask(self, driver, task):
        """
        Logic to launch TaskChunks by running through sub-tasks one at a time.
        """

        if task.task_id in self.pendingTaskChunks:
            self.pendingTaskChunks.setActive(task.task_id)

        if chunk_utils.isTaskChunk(task):
            self.pendingTaskChunks.addTask(task)
            taskChunkId = task.task_id
            if not self.pendingTaskChunks.isActive(taskChunkId):
                #TODO: Send a task started message
                update = mesos_pb2.TaskStatus()
                update.task_id.value = taskChunkId.value
                update.state = mesos_pb2.TASK_RUNNING
                driver.sendStatusUpdate(update)

            self.runNextSubTask(driver, taskChunkId)
        else:
            #super(TaskChunkExecutor, self).launchTask(driver, task)
            chunk_utils.ExecutorWrapper.launchTask(self, driver, task)

    def killTask(self, driver, taskId):
        """
        Kills task specified using taskId.
        taskId can refer to task or TaskChunks.

        """
        if taskId in self.pendingTaskChunks:
            origTaskId = taskId
            task = self.pendingTaskChunks[taskId]
            while chunk_utils.isTaskChunk(task):
                for subTask in chunk_utils.subTaskIterator(task):
                    if self.pendingTaskChunks.isActive(subTask.task_id):
                        taskId = subTask.task_id
                        task = self.pendingTaskChunks[taskId]
                        break
            update = mesos_pb2.TaskStatus()
            update.task_id.value = origTaskId.value
            update.state = mesos_pb2.TASK_KILLED
            driver.sendStatusUpdate(update)
            #del self.pendingTaskChunks[origTaskId]
            

        chunk_utils.ExecutorWrapper.killTask(self, driver, taskId)

    def killSubTasks(self, driver, subTaskIds):
        """
        Kills list of given subTasks given ids.
        SubTasksIds can refer to task/TaskChunks that are either
        pending or running but can't overlap (i.e. top-most parent id should
        be provided).

        """
        taskIdsToRun = set()
        for subTaskId in subTaskIds:
            if self.pendingTaskChunks.isActive(subTaskId):
                parent = self.pendingTaskChunks.getParent(subTaskId)
                self.killTask(driver, subTaskId)
            #    if not parent.task_id.SerializeToString() in taskIdsToRun:
                taskIdsToRun.add(parent.task_id.SerializeToString())
            else:
                update = mesos_pb2.TaskStatus()
                update.task_id.value = subTaskId.value
                update.state = mesos_pb2.TASK_KILLED
                driver.sendStatusUpdate(update)
                del self.pendingTaskChunks[subTaskId]

        for taskChunkId in taskIdsToRun:
            taskID = mesos_pb2.TaskID()
            taskID.ParseFromString(taskChunkId)
            self.runNextSubTask(driver, taskID)

    def runNextSubTask(self, driver, taskChunkId):
        """
        Launches next subtask from current taskChunk.

        """
        taskChunk = self.pendingTaskChunks[taskChunkId]
        if chunk_utils.numSubTasks(taskChunk) > 0:
            nextSubTaskId = chunk_utils.nextSubTask(taskChunk)
            self.launchTask(driver, nextSubTaskId)
        else:
            update = mesos_pb2.TaskStatus()
            update.task_id.value = taskChunkId.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)

    def frameworkMessage(self, driver, message):
        """
        Parses through framework messages to determine if KILL_SUBTASK message
        recieved.

        """
        parsedMessage = SubTaskMessage.fromString(message)
        if parsedMessage.isValid() and parsedMessage.getType() == SubTaskMessage.KILL_SUBTASKS:
            self.killSubTasks(driver, parsedMessage.getPayload())
        else:
            #super(TaskChunkExecutor, self).frameworkMessage(driver, message)
            chunk_utils.ExecutorWrapper.frameworkMessage(self, driver, message)


class TaskChunkExecutorDriver(chunk_utils.ExecutorDriverWrapper):

    def __init__(self, executor):
        """
        Initialize TaskTable and executorWrapper with executor

        """
        self.chunkExecutor = TaskChunkExecutor(executor)
        outerExecutor = chunk_utils.DriverOverridingExecutor(self.chunkExecutor, self)
        driver = mesos.MesosExecutorDriver(outerExecutor)
        chunk_utils.ExecutorDriverWrapper.__init__(self, driver)
        #super(TaskChunkExecutor, self).__init__(self, executor)

    def sendStatusUpdate(self, update):
        pending_tasks = self.chunkExecutor.pendingTaskChunks

        if pending_tasks.isSubTask(update.task_id):
            parentTaskId = pending_tasks.getParent(update.task_id).task_id
            updateMessage = chunk_utils.SubTaskUpdateMessage((parentTaskId, update))
            chunk_utils.ExecutorDriverWrapper.sendFrameworkMessage(self,
                    updateMessage.toString())
            if (chunk_utils.isTerminalUpdate(update) and
                    not update.state == mesos_pb2.TASK_KILLED):
                del pending_tasks[update.task_id]
                self.chunkExecutor.runNextSubTask(self, parentTaskId)
        else:
            if update.task_id in pending_tasks and chunk_utils.isTerminalUpdate(update):
                del pending_tasks[update.task_id]

            chunk_utils.ExecutorDriverWrapper.sendStatusUpdate(self,update)
            #super(TaskChunkExecutorDriver, self).sendStatusUpdate(update)
