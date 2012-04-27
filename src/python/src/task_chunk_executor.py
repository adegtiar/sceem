import mesos
import chunk_utils

class TaskChunkExecutor(chunk_utils.ExecutorWrapper):
    
    def __init__(self, executor):
        """
        Initialize TaskTable and executorWrapper with executor

        """
        self.pendingTaskChunks = TaskTable()
        super().__init__(self, executor)


    def launchTask(self, driver, task):
        """
        Logic to launch TaskChunks by running through sub-tasks one at a time.

        """
        if isTaskChunk(task):
            self.pendingTaskChunks.addTask(task)
            runNextSubTask(driver, task.task_id.value)
        else:
            super().launchTask(driver, task)
            
    def killTask(self, driver, taskId):
        """
        Kills task specified using taskId 
        taskId can refer to task or TaskChunks

        """
        if taskId in self.pendingTaskChunks:
            origTaskId = taskId
            task = self.pendingTaskChunks[taskId]:
            while chunk_utils.isTaskChunk(task):
                for subTask in chunk_utils.subTaskIterator(task):
                    if pendingTaskChunks.isRunning(subTask.task_id):
                        taskId = subTask.task_id
                        task = self.pendingTaskChunks[taskId]
                        break
            self.pendingTaskChunks.removeTask(origTaskId)

        super().killTask(driver, taskId)


    def killSubTasks(self, driver, subTaskIds):
        """
        Kills list of given subTasks given ids.
        SubTasksIds can refer to task/TaskChunks that are either 
        pending or running but can't overlap (i.e. top-most parent id should
        be provided)

        """
        taskChunksToRun = set()
        for subTaskId in subTaskIds:
            if self.pendingTaskChunks.isRunning(subTaskId):
                self.killTask(driver, subTaskId)
                parent = self.pendingTaskChunks.getParent(subTaskId)
                taskChunksToRun.add(parent.task_id)
            else: 
                self.pendingTaskChunks.removeTask(subTaskId)
                
        for taskChunkId in taskChunksToRun:
            self.runNextSubTask(driver, taskChunkId)
            

    def runNextSubTask(self, driver, taskChunkId):
        """
        Launches next subtask from current taskChunk

        """
        if self.pendingTaskChunks.hasSubTask(taskChunkId):
            nextSubTaskId = self.pendingTaskChunks.nextSubTask(taskChunkId)
            self.launchTask(driver,nextSubTaskId)
        else:
            driver.sendStatusUpdate(TaskStatus(taskChunkId, FINISHED))
            

    def frameworkMessage(self, driver, message):
        """
        Parses through framework messages to determine if KILL_SUBTASK message 
        recieved
        
        """
        parsed_msg = getMessage(data) #TODO: 
        if parsed_msg and parsed_msg[0] == KILL_SUBTASKS:
            self.killSubTasks(driver, parsed_msg[1])
        else:
            super().frameworkMessage(driver, message)

class TaskChunkExecutorDriver(mesos.MesosExecutorDriver):
    

    sendStatusUpdate(update):
    if chunkingExecutor.isSubTask(update.taskId):
        sendFrameworkMessage(serializeSubtaskUpdate(update))
        if isTerminalUpdate(update):
            chunkingExecutor.runNextSubTask(update.taskId)
        else:
            super.sendStatusUpdate(update)
            pass
