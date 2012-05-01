import mesos
import chunk_utils
import task_chunk_scheduler
import mesos_pb2
import pickle


# TaskStates that indicate the task is done and can be cleaned up.
TERMINAL_STATES = (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED,
            mesos_pb2.TASK_KILLED, mesos_pb2.TASK_LOST)


class TaskCollator:
    
    def __init__(self, pending_tasks):
        pass

    def getNext(self, isPrevAccepted):
        """
        Returns the next task and indicates if previous task was consumed

        """
        pass

    def chooseNextDonor(self):
        """
        Returns the next task(chunk) which will be a donor for a task

        """
        pass


class TaskConsumingOffers:
    #Map<offerId, TaskChunk> stolenTasks
    
    def __init__(self, offers, numTasks, numCurrentOffers):
        self.stolenTasks = {}
        self.offers = offers
        self.numTasks = numTasks

    def isDone(self):
        """
        Checks if all offers filled
        
        """
        pass

class TaskStealingSchedulerWrapper(task_chunk_scheduler.TaskChunkScheduler):
    """
    Delegates calls to the underlying scheduler.
    """

    def __init__(self, scheduler, driver):
        self.scheduler = scheduler
        self.driver = driver

    def registered(self, driver, frameworkId, masterInfo):
        self.scheduler.registered(self.driver, frameworkId, masterInfo)

    def reregistered(self, driver, masterInfo):
        self.scheduler.reregistered(self.driver, masterInfo)

    def disconnected(self, driver):
        self.scheduler.disconnected(self.driver)

    def resourceOffers(self, driver, offers):
        self.scheduler.resourceOffers(self.driver, offers)

    def offerRescinded(self, driver, offerId):
        self.scheduler.offerRescinded(self.driver, offerId)

    def statusUpdate(self, driver, status):
        self.scheduler.statusUpdate(self.driver, status)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.scheduler.frameworkMessage(self.driver, executorId, slaveId, message)

    def slaveLost(self, driver, slaveId):
        self.scheduler.slaveLost(self.driver, slaveId)

    def executorLost(self, driver, executorId, slaveId, status):
        self.scheduler.executorLost(self.driver, executorId, slaveId, status)

    def error(self, driver, message):
        self.scheduler.error(self.driver, message)


class TaskStealingSchedulerDriverWrapper(mesos.SchedulerDriver):
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

