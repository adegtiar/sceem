import mesos
import mesos_pb2
import chunk_utils
from chunk_utils import SubTaskMessage
from collections import defaultdict


class TaskChunkScheduler(chunk_utils.SchedulerWrapper):

    def __init__(self, scheduler):
        """
        Initialize schedulerWrapper with executor.
        """
        chunk_utils.SchedulerWrapper.__init__(self, scheduler)
        self.currentTaskChunks = chunk_utils.TaskTable()

    def frameworkMessage(self, driver, executorId, slaveId, data):
        message = SubTaskMessage.fromString(data)
        if message.isValid() and message.getType() == SubTaskMessage.SUBTASK_UPDATE:
            self.statusUpdate(driver, message.getPayload()[1])
        else:
            chunk_utils.SchedulerWrapper.frameworkMessage(self, driver,
                    executorId, slaveId, data)


class TaskChunkSchedulerDriver(chunk_utils.SchedulerDriverWrapper):

    def __init__(self, scheduler, framework, master, outerScheduler=None):
        """
        Initialize scheduler wrapper with framework scheduler
        """
        if not isinstance(scheduler, TaskChunkScheduler):
            scheduler = TaskChunkScheduler(scheduler)
        # Wrap the outer scheduler with the driver overriding wrapper.
        if not outerScheduler:
            outerScheduler = chunk_utils.DriverOverridingScheduler(scheduler, self)

        # Construct the mesos driver at the lowest level.
        mesos_driver = mesos.MesosSchedulerDriver(outerScheduler, framework, master)
        chunk_utils.SchedulerDriverWrapper.__init__(self, mesos_driver)

    def killSubTasks(self, subTasks):
        perSlaveExecTasks = defaultdict(list)

        for subTask in subTasks:
            executorIdString = subTask.executor.executor_id.SerializeToString()
            slaveIdString = subTask.slave_id.SerializeToString()
            perSlaveExecTasks[(slaveIdString, executorIdString)].append(subTask.task_id)

        for slaveExecId, subTaskIds in perSlaveExecTasks.iteritems():
            slaveIdString, executorIdString = slaveExecId

            executorId = mesos_pb2.ExecutorID()
            executorId.ParseFromString(executorIdString)

            slaveId = mesos_pb2.SlaveID()
            slaveId.ParseFromString(slaveIdString)

            message = chunk_utils.KillSubTasksMessage(subTaskIds)

            self.sendFrameworkMessage(executorId, slaveId, message.toString())
