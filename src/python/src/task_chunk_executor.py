import mesos
import chunk_utils

class TaskChunkExecutor(chunk_utils.ExecutorWrapper):
    
    pendingTaskChunks
    def __init__(self, executor):
        

# Handles logic for launching TaskChunks by running sub-tasks one at a time.
    def launchTask(self, driver, task):
        if isTaskChunk(task):
            self.pendingTaskChunks.addTask(task)
            runNextSubTask(driver, task.task_id.value)
        else:
            super.launchTask(driver, task)

    def killTask(self, driver, taskId):
        if taskId in pendingTasks:
            origTaskId = taskId
            task = pendingTasks[taskId]:
            while isTaskChunk(task):
                for subTask in subTaskIterator(task):
                    if pendingTaskChunks.isRunning(subTask.task_id):
                        taskId = subTask.task_id
                        task = pendingTaskChunks[taskId]
                        break
                    pendingTaskChunks.removeTask(origTaskId)

    executor.killTask(driver, taskId)


    def killSubTasks(self, driver, subTaskIds):
        taskChunksToRun = set()
        for subTaskId in subTaskIds:
            if pendingTaskChunks.isRunning(subTaskId):
                killTask(driver, subTaskId)
                parent = pendingTaskChunks.getParent(subTaskId)
                taskChunksToRun.add(parent.task_id)
            else:
                pendingTaskChunks.removeTask(subTaskId)
                
                for taskChunkId in taskChunksToRun:
                    runNextSubTask(driver, taskChunkId)



runNextSubTask(self, driver, taskChunkId):
if pendingTaskChunks.hasSubTask(taskChunkId):
    launchTask(driver, pendingTaskChunks.nextSubTask(taskChunkId))
else:
    driver.sendStatusUpdate(TaskStatus(taskChunkId, FINISHED))


isSubTask(self, taskId):
return pendingTaskChunks.isSubTask(taskId)


frameworkMessage(self, driver, data):
message = getMessage(data)
if message and message[0] == KILL_SUBTASKS:
killSubTasks(driver, message[1])
else:
executor.frameworkMessage(driver, data)
    pass

class TaskChunkExecutorDriver(mesos.MesosExecutorDriver):
    pass
