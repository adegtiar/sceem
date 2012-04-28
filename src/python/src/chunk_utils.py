import mesos
import mesos_pb2


class MessageType:
    """
    The type of a subtask message.
    """
    SUBTASK_UPDATE, KILL_SUBTASKS = range(2)


# TaskStates that indicate the task is done and can be cleaned up.
TERMINAL_STATES = (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED,
            mesos_pb2.TASK_KILLED, mesos_pb2.TASK_LOST)


def getMessage(data):
    #TODO: implement
#    partially de-serialize data
#    if matches MessageTypes:
#        return deserialize_data(data)
#    else:
#        return None
    pass


def serializeSubTaskUpdate(taskUpdate):
    #TODO: implement
    pass


def serializeKillSubTasks(subTaskIds):
    #TODO: implement
    pass


def newTaskChunk():
    """
    Creates a new empty chunk of tasks.
    """
    taskChunk = mesos_pb2.TaskInfo()
    # Initialize the empty sub_tasks field.
    taskChunk.sub_tasks.tasks.extend(())
    taskChunk


def addSubTask(taskChunk, subTask):
    """
    Adds a new sub task to a task chunk.
    """
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


def getNextSubTask(taskChunk):
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
    raise KeyError("subTaskId {0} not found in {1}".format(subTaskId, parent))


def subTaskIterator(taskChunk):
    """
    An iterator over the direct sub tasks within the given task chunk.
    """
    for subTask in taskChunk.sub_tasks.tasks:
        yield subTask


def isTerminalUpdate(statusUpdate):
    """
    Checks whether the given TaskStatus is for a terminal state.
    """
    taskState = statusUpdate.state
    return taskState in TERMINAL_STATES


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
        self.all_tasks = {}
        # The root container for all tasks.
        self.rootTask = mesos_pb2.TaskInfo()

    def addTask(self, task, parent=None):
        """
        Adds the give task to the table.

        If no parent is specified, adds it as a top-level task.
        If the task is already in the table, there is no effect.
        """
        if task.task_id in self:
            return
        if not parent:
            parent = self.rootTask
        self.all_tasks[task.task_id] = TaskNode(parent, task)
        for subTask in subTaskIterator(task):
            addTask(subTask, task)

    def __getitem__(self, taskId):
        """
        Retrieves from the table the task with the given id.
        """
        return self.all_tasks[taskId].task

    def __delitem__(self, taskId):
        """
        Removes the task with the given task id from the table.
        """
        taskNode = self.all_tasks[taskId]
        removeSubTask(taskNode.parent, taskId)
        for subTask in subTaskIterator(taskNode.task):
            del self[subTask.id]
        del self.all_tasks[taskId]

    def __contains__(self, taskId):
        """
        Checks if the task with the given id is in the table.
        """
        return taskId in all_tasks

    def updateState(taskId, state):
        # TODO: fix.
        self.all_tasks[taskId].state = state

    def getState(taskId):
        # TODO: fix.
        return self.all_tasks[taskId].state

    def getParent(subTaskId):
        # TODO: fix.
        return self.all_tasks[taskId].parent

    def isRunning(taskId):
        # TODO: fix.
        return getState(taskId) == TASK_RUNNING

    def isSubTask(taskId):
        # TODO: fix.
        return taskId in self and getParent(taskId) is not rootTask


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


class SchedulerWrapper(mesos.Scheduler):
    """
    Delegates calls to the underlying scheduler.
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

    def frameworkMessage(self, driver, message):
        self.scheduler.frameworkMessage(driver, message)

    def slaveLost(self, driver, slaveId):
        self.scheduler.slaveLost(driver, slaveId)

    def executorLost(self, driver, executorId, slaveId, status):
        self.scheduler.executorLost(driver, executorId, slaveId, status)

    def error(self, driver, message):
        self.scheduler.error(driver, message)
