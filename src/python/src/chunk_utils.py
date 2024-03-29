import mesos
import mesos_pb2
import pdb
import pickle
import operator
from collections import defaultdict


# TaskStates that indicate the task is done and can be cleaned up.
TERMINAL_STATES = (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED,
            mesos_pb2.TASK_KILLED, mesos_pb2.TASK_LOST)


def isTerminalUpdate(statusUpdate):
    """
    Checks whether the given TaskStatus is for a terminal state.
    """
    taskState = statusUpdate.state
    return taskState in TERMINAL_STATES


class SubTaskMessage(object):
    """
    A message corresponding to a task chunk's sub task.
    """
    SUBTASK_UPDATE, KILL_SUBTASKS = range(2)
    messageClasses = {}

    def __init__(self, messageType = None, payload = None, valid = True):
        self.__type = messageType
        self.__payload = payload
        self.__valid = valid

    def __eq__(self, other):
        return (isinstance(other, SubTaskMessage) and
                self.__type == other.__type and
                self.__payload == other.__payload and
                self.__valid == other.__valid)


    @staticmethod
    def fromString(serialized_message):
        """
        Returns a sub task message with a type and a payload.
        If this fails, message.isValid() will be False.
        """
        try:
            messageType, serializedPayload = pickle.loads(serialized_message)

            if messageType not in SubTaskMessage.messageClasses:
                raise ValueError

            message_class = SubTaskMessage.messageClasses[messageType]
            payload = message_class.payloadFromString(serializedPayload)
            return message_class(payload)
        except Exception:
            return SubTaskMessage(valid = False)


    def toString(self):
        """
        Serializes the message into a string.
        """
        if not self.isValid():
            raise ValueError("Cannot serialize an invalid message")
        try:
            payloadString = self.payloadToString(self.getPayload())
        except Exception:
            raise ValueError("Payload is invalid and cannot be serialized")
        return pickle.dumps((self.getType(), payloadString))

    def isValid(self):
        """
        Checks whether or not the message was parsed successfully.
        """
        return self.__valid

    def getPayload(self):
        """
        Retrieves the un-serialized payload of the message.
        """
        if not self.isValid():
            raise ValueError("Cannot retrieve the payload of an invalid message")
        return self.__payload

    def getType(self):
        if not self.isValid():
            raise ValueError("Cannot retrieve the type of an invalid message")
        return self.__type


class SubTaskUpdateMessage(SubTaskMessage):
    """
    A message that holds the TaskStatus for a sub task.
    """

    def __init__(self, parentStatusTuple):
        super(SubTaskUpdateMessage, self).__init__(
                SubTaskMessage.SUBTASK_UPDATE, parentStatusTuple)

    @staticmethod
    def payloadFromString(serializedPayload):
        parentIdString, statusString = pickle.loads(serializedPayload)

        parentId = mesos_pb2.TaskID()
        parentId.ParseFromString(parentIdString)

        taskStatus = mesos_pb2.TaskStatus()
        taskStatus.ParseFromString(statusString)

        return parentId, taskStatus

    @staticmethod
    def payloadToString(payload):
        parentId, status = payload

        parentIdString = parentId.SerializeToString()
        statusString = status.SerializeToString()

        return pickle.dumps((parentIdString, statusString))


class KillSubTasksMessage(SubTaskMessage):
    """
    A message that holds the list of the ids of sub tasks to kill.
    """

    def __init__(self, subTaskIds):
        super(KillSubTasksMessage, self).__init__(
                SubTaskMessage.KILL_SUBTASKS, subTaskIds)

    @staticmethod
    def payloadFromString(serializedPayload):
        subTaskIdStrings = pickle.loads(serializedPayload)
        subTaskIds = []
        for serializedSubTaskId in subTaskIdStrings:
            taskId = mesos_pb2.TaskID()
            taskId.ParseFromString(serializedSubTaskId)
            subTaskIds.append(taskId)
        return subTaskIds

    @staticmethod
    def payloadToString(payload):
        subTaskIdStrings = [subTaskId.SerializeToString() for subTaskId in payload]
        return pickle.dumps(subTaskIdStrings)


# Initialize class list of SubTaskMessage.
SubTaskMessage.messageClasses[SubTaskMessage.SUBTASK_UPDATE] = SubTaskUpdateMessage
SubTaskMessage.messageClasses[SubTaskMessage.KILL_SUBTASKS] = KillSubTasksMessage


def newTaskChunk(slaveId,executor=None, subTasks = ()):
    """
    Creates a new empty chunk of tasks.
    Any given sub tasks are copied into the chunk.
    """
    taskChunk = mesos_pb2.TaskInfo()
    taskChunk.slave_id.value = slaveId.value
    if executor:
        taskChunk.executor.MergeFrom(executor)
    # Initialize the empty sub_tasks field.
    taskChunk.sub_tasks.tasks.extend(())

    for subTask in subTasks:
        addSubTask(taskChunk, subTask)
    return taskChunk


def getResourcesValue(resources):
    dictRes = defaultdict(int)
    for resource in resources:
        dictRes[resource.name] = (resource.scalar.value, resource)
    return dictRes


def incrementResources(arg1, arg2):
    resourcesOperation(arg1, arg2, operator.add)


def decrementResources(arg1, arg2):
    resourcesOperation(arg1, arg2, operator.sub)


def maxResources(arg1, arg2):
    resourcesOperation(arg1, arg2, max)


def resourcesOperation(resArg1, resArg2, func):
    for res_op_2 in resArg2:
        isPresent = False
        for res_op_1 in resArg1:
            if res_op_2.name == res_op_1.name:
                isPresent = True
                if (res_op_2.type == mesos_pb2.Value.SCALAR):
                    res_op_1.scalar.value = func(res_op_1.scalar.value,
                            res_op_2.scalar.value)

        if not isPresent:
            if func in [max, operator.add]:
                resNew = resArg1.add()
                resNew.CopyFrom(res_op_2)
            elif func == operator.sub:
                resNew = resArg1.add()
                resNew.CopyFrom(res_op_2)
                resNew.scalar.value = -res_op_2.scalar.value


def isOfferValid(offer):
    for resource in offer.resources:
        if resource.type == mesos_pb2.Value.SCALAR:
            if resource.scalar.value < 0:
                return False
    return True


def isOfferEmpty(offer):
    for resource in offer.resources:
        if resource.type == mesos_pb2.Value.SCALAR:
            if resource.scalar.value > 0:
                return False
    return True


def addSubTask(taskChunk, subTask, inherit=True):
    """
    Adds a copy of the given sub task to the task chunk.
    """
    if not subTask.task_id.IsInitialized():
      raise ValueError("Sub tasks added to a task chunk must have an id.")

    if inherit:
        subTask.slave_id.value = taskChunk.slave_id.value
        maxResources(taskChunk.resources, subTask.resources)
        if taskChunk.executor.IsInitialized():
            subTask.executor.MergeFrom(taskChunk.executor)

    taskChunk.sub_tasks.tasks.extend((subTask,))


def isTaskChunk(task):
    """
    Checks whether the given TaskInfo represents a chunk of tasks.
    """
    return task.HasField("sub_tasks")


def numSubTasks(taskChunk):
    """
    Returns the number of direct sub tasks in the given task chunk.
    """
    return len(taskChunk.sub_tasks.tasks)


def nextSubTask(taskChunk):
    """
    Gets the next sub task within the given chunk.

    Raises:
        ValueError: The given taskChunk has no sub tasks.
    """
    try:
        return next(subTaskIterator(taskChunk))
    except StopIteration:
        raise ValueError("The given task has no sub tasks")


def removeSubTask(taskChunk, subTaskId):
    """
    Removes the subTask with the given id from the parent.

    Returns:
        The removed subTask.

    Raises:
        KeyError: No subTask with that id is found.
    """
    index = 0
    for subTask in taskChunk.sub_tasks.tasks:
        if subTask.task_id == subTaskId:
            del taskChunk.sub_tasks.tasks[index]
            return subTask
        index += 1
    raise KeyError("subTaskId {0} not found in {1}".format(subTaskId, taskChunk))


def subTaskIterator(taskChunk):
    """
    An iterator over the direct sub tasks within the given task chunk.
    """
    for subTask in taskChunk.sub_tasks.tasks:
        yield subTask


class TransformingDict(dict):
    """
    A dictionary that applies a map function prior to using keys.
    """

    def __init__(self, mapper=lambda x: x):
        super(TransformingDict, self).__init__()
        self.mapper = mapper

    def __getitem__(self, key):
        return dict.__getitem__(self, self.mapper(key))

    def __setitem__(self, key, value):
        return dict.__setitem__(self, self.mapper(key), value)

    def __delitem__(self, key):
        return dict.__delitem__(self, self.mapper(key))

    def __contains__(self, key):
        return dict.__contains__(self, self.mapper(key))


class TaskTable(object):
    """
    A table of all currently running/pending tasks.
    """

    class TaskNode:
        """
        Stores the graph node of a task within the table.
        """

        def __init__(self, parent, task, state = mesos_pb2.TASK_STAGING):
            self.parent = parent
            self.task = task
            self.state = state

    def __init__(self):
        # Maps taskId to TaskNode.
        def task_id_mapper(task_id):
            return task_id.SerializeToString()
        self.all_task_nodes = TransformingDict(task_id_mapper)
        # The root container for all tasks.
        self.rootTask = mesos_pb2.TaskInfo()

    def addTask(self, task, parent=None):
        """
        Adds the given task to the table.

        If no parent is specified, adds it as a top-level task.
        If the task is already in the table, there is no effect.
        """
        if not task.task_id.IsInitialized():
            raise ValueError("Tasks added to the table must have an id.")
        if task.task_id in self:
            return
        if not parent:
            parent = self.rootTask
            addSubTask(parent, task, inherit=False)
        self.all_task_nodes[task.task_id] = TaskTable.TaskNode(parent, task)
        for subTask in subTaskIterator(task):
            self.addTask(subTask, task)

    def __getitem__(self, taskId):
        """
        Retrieves from the table the task with the given id.
        """
        return self.all_task_nodes[taskId].task

    def __delitem__(self, taskId):
        """
        Removes the task with the given task id from the table.
        """
        taskNode = self.all_task_nodes[taskId]
        removeSubTask(taskNode.parent, taskId)
        self.propogateToRoot(taskNode.parent)
        # Generate a list of sub tasks to avoid concurrent modification.
        subTasks = [subTask for subTask in subTaskIterator(taskNode.task)]
        for subTask in subTasks:
            del self[subTask.task_id]
        del self.all_task_nodes[taskId]

    def propogateToRoot(self, task):
        """
        Call this when the given task is update, and changes need to
        propogate to the root.
        """
        # TODO: We probably shouldn't use TaskChunks directly in the
        # table.
        if task is not self.rootTask:
            parent = self.all_task_nodes[task.task_id].parent
            for subTask in subTaskIterator(parent):
                if subTask.task_id == task.task_id:
                    subTask.CopyFrom(task)
                    return self.propogateToRoot(parent)

    def __contains__(self, taskId):
        """
        Checks if the task with the given id is in the table.
        """
        return taskId in self.all_task_nodes

    def __len__(self):
        """
        Returns the number of tasks (including sub tasks) in the table.
        """
        return len(self.all_task_nodes)

    def __iter__(self):
        """
        Returns an iterator over the top-level tasks (sub tasks of the root).
        """
        for subTask in subTaskIterator(self.rootTask):
            if self.isActive(subTask.task_id):
                yield subTask

    def setActive(self, taskId):
        """
        Updates the state of a task in the table.
        """
        self.all_task_nodes[taskId].state = mesos_pb2.TASK_RUNNING

    def getParent(self, subTaskId):
        """
        Returns the parent of the sub task with the given id.
        """
        return self.all_task_nodes[subTaskId].parent

    def isActive(self, taskId):
        """
        Checks if the task with the given id is currently running.
        """
        return self.all_task_nodes[taskId].state == mesos_pb2.TASK_RUNNING

    def isSubTask(self, taskId):
        """
        Checks if the task with the given id is a sub task in the table.
        """
        return taskId in self and self.getParent(taskId) is not self.rootTask


class ExecutorWrapper(mesos.Executor):
    """
    Delegates calls to the underlying executor.
    """

    def __init__(self, executor):
        self.executor = executor

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        self.executor.registered(driver, executorInfo, frameworkInfo, slaveInfo)

    def reregistered(self, driver, slaveInfo):
        self.executor.reregistered(driver, slaveInfo)

    def disconnected(self, driver):
        self.executor.disconnected(driver)

    def launchTask(self, driver, task):
        self.executor.launchTask(driver, task)

    def killTask(self, driver, taskId):
        self.executor.killTask(driver, taskId)

    def frameworkMessage(self, driver, message):
        self.executor.frameworkMessage(driver, message)

    def shutdown(self, driver):
        self.executor.shutdown(driver)

    def error(self, driver, message):
        self.executor.error(driver, message)


class DriverOverridingExecutor(ExecutorWrapper):
    """
    A wrapper that overrides the driver passed in to functions.
    """

    def __init__(self, executor, driver):
        ExecutorWrapper.__init__(self, executor)
        self.driver = driver

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        ExecutorWrapper.registered(self, self.driver, executorInfo, frameworkInfo, slaveInfo)

    def reregistered(self, driver, slaveInfo):
        ExecutorWrapper.reregistered(self, self.driver, slaveInfo)

    def disconnected(self, driver):
        ExecutorWrapper.disconnected(self, self.driver)

    def launchTask(self, driver, task):
        ExecutorWrapper.launchTask(self, self.driver, task)

    def killTask(self, driver, taskId):
        ExecutorWrapper.killTask(self, self.driver, taskId)

    def frameworkMessage(self, driver, message):
        ExecutorWrapper.frameworkMessage(self, self.driver, message)

    def shutdown(self, driver):
        ExecutorWrapper.shutdown(self, self.driver)

    def error(self, driver, message):
        ExecutorWrapper.error(self, self.driver, message)



class SchedulerWrapper(mesos.Scheduler):
    """
    Delegates calls to the underlying scheduler with the passed-in driver.
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def registered(self, driver, frameworkId, masterInfo):
        self.scheduler.registered(driver, frameworkId, masterInfo)

    def reregistered(self, driver, masterInfo):
        self.scheduler.reregistered(driver, masterInfo)

    def disconnected(self, driver):
        self.scheduler.disconnected(driver)

    def resourceOffers(self, driver, offers):
        self.scheduler.resourceOffers(driver, offers)

    def offerRescinded(self, driver, offerId):
        self.scheduler.offerRescinded(driver, offerId)

    def statusUpdate(self, driver, status):
        self.scheduler.statusUpdate(driver, status)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.scheduler.frameworkMessage(driver, executorId, slaveId, message)

    def slaveLost(self, driver, slaveId):
        self.scheduler.slaveLost(driver, slaveId)

    def executorLost(self, driver, executorId, slaveId, status):
        self.scheduler.executorLost(driver, executorId, slaveId, status)

    def error(self, driver, message):
        self.scheduler.error(driver, message)


class DriverOverridingScheduler(SchedulerWrapper):
    """
    A wrapper that overrides the driver passed in to functions.
    """
    def __init__(self, scheduler, driver):
        SchedulerWrapper.__init__(self, scheduler)
        self.driver = driver

    def registered(self, driver, frameworkId, masterInfo):
        SchedulerWrapper.registered(self, self.driver, frameworkId, masterInfo)

    def reregistered(self, driver, masterInfo):
        SchedulerWrapper.reregistered(self, self.driver, masterInfo)

    def disconnected(self, driver):
        SchedulerWrapper.disconnected(self, self.driver)

    def resourceOffers(self, driver, offers):
        SchedulerWrapper.resourceOffers(self, self.driver, offers)

    def offerRescinded(self, driver, offerId):
        SchedulerWrapper.offerRescinded(self, self.driver, offerId)

    def statusUpdate(self, driver, status):
        SchedulerWrapper.statusUpdate(self, self.driver, status)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        SchedulerWrapper.frameworkMessage(self, self.driver, executorId, slaveId, message)

    def slaveLost(self, driver, slaveId):
        SchedulerWrapper.slaveLost(self, self.driver, slaveId)

    def executorLost(self, driver, executorId, slaveId, status):
        SchedulerWrapper.executorLost(self, self.driver, executorId, slaveId, status)

    def error(self, driver, message):
        SchedulerWrapper.error(self, self.driver, message)


class ExecutorDriverWrapper(mesos.ExecutorDriver):
    """
    Delegates calls to the underlying MesosExecutorDriver.
    """
    def __init__(self, driver):
        self.driver = driver

    def start(self):
        self.driver.start()

    def stop(self):
        self.driver.stop()

    def abort(self):
        self.driver.abort()

    def join(self):
        self.driver.join()

    def run(self):
        self.driver.run()

    def sendStatusUpdate(self, status):
        self.driver.sendStatusUpdate(status)

    def sendFrameworkMessage(self, data):
        self.driver.sendFrameworkMessage(data)


class SchedulerDriverWrapper(mesos.SchedulerDriver):
    """
    Delegates calls to the underlying MesosSchedulerDriver.
    """
    def __init__(self, driver):
        self.driver = driver

    def start(self):
        self.driver.start()

    def stop(self, failover=False):
        if failover:
            self.driver.stop(failover)
        else:
            self.driver.stop()

    def abort(self):
        self.driver.abort()

    def join(self):
        self.driver.join()

    def run(self):
        self.driver.run()

    def requestResources(self, requests):
        self.driver.requestResources(requests)

    def launchTasks(self, offerId, tasks, filters=None):
        if filters:
            self.driver.launchTasks(offerId, tasks, filters)
        else:
            self.driver.launchTasks(offerId, tasks)

    def killTask(self, taskId):
        self.driver.killTask(taskId)

    def declineOffer(self, offerId, filters=None):
        if filters:
            self.driver.declineOffer(self, offerId, filters)
        else:
            self.driver.declineOffer(self, offerId)

    def reviveOffers(self):
        self.driver.reviveOffers()

    def sendFrameworkMessage(self, executorId, slaveId, data):
        self.driver.sendFrameworkMessage(executorId, slaveId, data)

