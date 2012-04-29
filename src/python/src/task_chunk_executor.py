import mesos
import mesos_pb2
import chunk_utils


SUBTASK_UPDATE, KILL_SUBTASKS = range(2)

class TaskChunkExecutor(chunk_utils.ExecutorWrapper):


    def __init__(self, executor):
        """
        Initialize TaskTable and executorWrapper with executor.

        """
        self.pendingTaskChunks = chunk_utils.TaskTable()
        self.executor = executor
        #super(TaskChunkExecutor, self).__init__(self, executor)
        chunk_utils.ExecutorWrapper.__init__(self, executor)

    def launchTask(self, driver, task):
        """
        Logic to launch TaskChunks by running through sub-tasks one at a time.
        """
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
        if task.task_id in self.pendingTaskChunks:
            self.pendingTaskChunks.setActive(task.task_id)

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
            del self.pendingTaskChunks[origTaskId]

        chunk_utils.ExecutorWrapper.killTask(self, driver, taskId)

    def killSubTasks(self, driver, subTaskIds):
        """
        Kills list of given subTasks given ids.
        SubTasksIds can refer to task/TaskChunks that are either
        pending or running but can't overlap (i.e. top-most parent id should
        be provided).

        """
        taskChunksToRun = set()
        for subTaskId in subTaskIds:
            if self.pendingTaskChunks.isActive(subTaskId):
                self.killTask(driver, subTaskId)
                parent = self.pendingTaskChunks.getParent(subTaskId)
                taskChunksToRun.add(parent.task_id)
            else:
                del self.pendingTaskChunks[subTaskId]

        for taskChunkId in taskChunksToRun:
            self.runNextSubTask(driver, taskChunkId)

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
        parsed_msg = driver.getMessage(message)
        if parsed_msg and parsed_msg[0] == KILL_SUBTASKS:
            self.killSubTasks(driver, parsed_msg[1])
        else:
            #super(TaskChunkExecutor, self).frameworkMessage(driver, message)
            chunk_utils.ExecutorWrapper.frameworkMessage(self, driver, message)


class TaskChunkExecutorDriver(chunk_utils.ExecutorDriverWrapper):

    def __init__(self, executor):
        """
        Initialize TaskTable and executorWrapper with executor

        """
        self.chunkExecutor = TaskChunkExecutor(executor)
        driver =  mesos.MesosExecutorDriver(self.chunkExecutor)
        chunk_utils.ExecutorDriverWrapper.__init__(self,driver)
        #super(TaskChunkExecutor, self).__init__(self, executor)

    def sendStatusUpdate(self,update):
        pending_tasks = self.chunkExecutor.pendingTaskChunks

        if self.chunkExecutor.isSubTask(update.taskId):
            sendFrameworkMessage(serializeSubtaskUpdate(update))

            if isTerminalUpdate(update):
                del pending_tasks[update.taskId]
                self.chunkExecutor.runNextSubTask(self,)
        else:

            if update.task_id in pending_tasks and isTerminalUpdate(update):
                del pending_tasks[update.taskId]

            chunk_utils.ExecutorDriverWrapper.sendStatusUpdate(self,update)
            #super(TaskChunkExecutorDriver, self).sendStatusUpdate(update)

