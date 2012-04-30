import mesos
import mesos_pb2
import chunk_utils
from chunk_utils import SubclassMessages
from collections import defaultdict

class TaskChunkScheduler(chunk_utils.SchedulerWrapper):

    
    def __init__(self, scheduler):
        """
        Initialize schedulerWrapper with executor.

        """
        self.currentTaskChunks = chunk_utils.TaskTable()
        #super(TaskChunkExecutor, self).__init__(self, executor)
        chunk_utils.SchedulerWrapper.__init__(self, scheduler)

    def frameworkMessage(self, driver, data):
        message = driver.getMessage(data)
        if message and message[0] == SubclassMessages.SUBTASK_UPDATE:
            chunk_utils.SchedulerWrapper.statusUpdate(self, driver, 
message[1])
        else:
            chunk_utils.SchedulerWrapper.frameworkMessage(self, driver, data)

class TaskChunkSchedulerDriver(chunk_utils.SchedulerDriverWrapper):
    
    def __init__(self, scheduler):
        """
        Initialize scheduler wrapper with framework scheduler
        
        """
        self.chunkScheduler = TaskChunkScheduler(scheduler)
        driver = mesos.MesosSchedulerDriver(self.chunkScheduler)
        chunk_utils.SchedulerDriverWrapper.__init__(self,driver)

    def killSubtasks(self, subTasks):
        perExecutorTasks = defaultdict(list)
        for subTask in subTasks:
            perExecutorTasks[subTask.executor.executor_id.SerializeToString()].append(subTask.task_id)
            for executor, subTasks in perExecutorTasks.iteritems():
                executorId = mesos_pb2.ExecutorID()
                executorId.ParseFromString(executor)
                message = serializeKillSubtasks(subTaskIds)
                chunk_utils.SchedulerDriverWrapper.sendFrameworkMessage(
                    executorId,subTasks[0].slave_id, message)
                
