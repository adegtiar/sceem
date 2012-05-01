import mesos
import mesos_pb2
import chunk_utils
from chunk_utils import SubTaskMessage
from collections import defaultdict

class TaskChunkScheduler(chunk_utils.SchedulerWrapper):


    def __init__(self, scheduler, driver):
        """
        Initialize schedulerWrapper with executor.

        """
        self.currentTaskChunks = chunk_utils.TaskTable()
        #super(TaskChunkExecutor, self).__init__(self, executor)
        chunk_utils.SchedulerWrapper.__init__(self, scheduler, driver)

    def frameworkMessage(self, driver, executorId, slaveId, data):
        message = SubTaskMessage.fromString(data)
        if message.isValid() and message.getType() == SubTaskMessage.SUBTASK_UPDATE:
            self.statusUpdate(driver, message.getPayload())
        else:
            chunk_utils.SchedulerWrapper.frameworkMessage(self, driver,
                    executorId, slaveId, data)

class TaskChunkSchedulerDriver(chunk_utils.SchedulerDriverWrapper):

    def __init__(self, scheduler, framework, master):
        """
        Initialize scheduler wrapper with framework scheduler

        """
        self.chunkScheduler = TaskChunkScheduler(scheduler, self)
        self.driver = mesos.MesosSchedulerDriver(self.chunkScheduler, framework, master)
        chunk_utils.SchedulerDriverWrapper.__init__(self, self.driver)

    def killSubTasks(self, subTasks):
        perExecutorTasks = defaultdict(list)

        for subTask in subTasks:
            executorIdString = subTask.executor.executor_id.SerializeToString()
            perExecutorTasks[executorIdString].append(subTask.task_id)

        for executorIdString, subTaskIds in perExecutorTasks.iteritems():
            message = chunk_utils.KillSubTasksMessage(subTaskIds)

            executorId = mesos_pb2.ExecutorID()
            executorId.ParseFromString(executorIdString)

            self.sendFrameworkMessage(executorId, subTasks[0].slave_id,
                    message.toString())
