import mesos
import mesos_pb2

class MessageType:
    SUBTASK_UPDATE, KILL_SUBTASKS = range(2)


def getMessage(data):
    #TODO: implement
#    partially de-serialize data
#    if matches MessageTypes:
#        return deserialize_data(data)
#    else:
#        return None
    pass


def serializeSubtaskUpdate(taskUpdate):
    #TODO: implement
    pass


def serializeKillSubtasks(subtaskIds):
    #TODO: implement
    pass


def isTaskChunk(task):
    #TODO: implement
    pass


def getNextSubTask(taskChunk):
    #TODO: implement
    return subTaskIterator(taskChunk).next()


def removeSubtask(parent, subtask):
    #TODO: implement
    parent.sub_tasks.remove(subtask)


def subTaskIterator(taskChunk):
    #TODO: implement
    #for subtask in taskChunk.
    pass


def isTerminalUpdate(statusUpdate):
    #TODO: implement
    return statusUpdate in (TASK_FINISHED, TASK_FAILED, TASK_KILLED, TASK_LOST)


class TaskTable:
    """A table of all currently running/pending tasks.
    """

    class TaskNode:
        """Stores the graph node of a task within the table.
        """

        def __init__(self, parent, task, state):
            self.parent = parent
            self.task = task
            self.state = state

    def __init__(self):
        # Maps taskId to TaskNode.
        self.all_tasks = {}
        # The root container for all tasks. TODO: fix.
        self.rootTask = TaskInfo(name=None, task_id=None, slave_id=None)

    def addTask(task, parent=None):
        # TODO: fix.
        newNode = TaskNode(parent, task, TASK_STAGING)
        all_tasks[task.task_id] = newNode
        if isTaskChunk(task):
            for subTask in task.sub_tasks:
                addTask(subTask, task)

    def removeTask(taskId):
        # TODO: fix.
        taskNode = all_tasks[taskId]
        if taskNode.parent:
            removeSubTask(taskNode.parent, taskNode.task)
        if isTaskChunk(taskNode.task):
            for subTask in task.sub_tasks: # TODO: make this iterable.
                removeTask(subTask.id)
        del all_tasks[taskId]

    def hasSubTask(taskChunkId):
        # TODO: implement
        pass

    def getNextTask(taskChunkId):
        # TODO: fix.
        return getNextSubTask(all_tasks[taskChunkId].task)

    def updateState(taskId, state):
        # TODO: fix.
        self.all_tasks[taskId].state = state

    def getState(taskId):
        # TODO: fix.
        return all_tasks[taskId].state

    def getParent(subTaskId):
        # TODO: fix.
        return all_tasks[taskId].parent

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
