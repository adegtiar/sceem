import mesos
import chunk_utils

class TaskChunkScheduler(chunk_utils.SchedulerWrapper):

    def frameworkMessage(self, driver, data):
        message = getMessage(data)
        if message and message[0] == SUBTASK_UPDATE:
            super.statusUpdate(driver, message[1])
        else:
            SchedulerWrapper.frameworkMessage(self, driver, data)

class TaskChunkSchedulerDriver(mesos.MesosSchedulerDriver):
    
    def killSubtasks(self, subTasks):
        perExecutorTasks = defaultdict(list)
        for subTask in subTasks:
            perExecutorTasks[subTask.executor.executor_id].append(subTask)
            for executorId, subTasks in perExecutorTasks.iteritems():
                message = serializeKillSubtasks(taskIds)
                SchedulerWrapper.sendFrameworkMessage(executorId, subTasks[0].slave_id, message)
                
