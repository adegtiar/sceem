import mesos
import chunk_utils
import task_chunk_scheduler
import mesos_pb2
import pickle
import heapq


# TaskStates that indicate the task is done and can be cleaned up.
TERMINAL_STATES = (mesos_pb2.TASK_FINISHED, mesos_pb2.TASK_FAILED,
            mesos_pb2.TASK_KILLED, mesos_pb2.TASK_LOST)

class PriortyQueue(object):

  def __init__(self, initial=None, sort_key=lambda x: x, mapper=lambda x: x):
    self.sort_key = sort_key
    self.mapper = mapper
		self.mapping = {}
		self.__data = []
		
		if initial:
			for item in initial:
				self.push(item)
        
  def push(self, item):
    item_key = self.mapper(item)
    self.mapping[item_key] = item
    heapq.heappush(self.__data, (self.sort_key(item), item_key))
    
  def pop(self):
    item_key = heapq.heappop(self._data)[1]
    item_value = self.mapping[item_key]
    del self.mapping[item_key]
    return item_value

#The TaskQueue receives offers and returns tasks to give up to that offer.
class TaskQueue:
  def __init__(self, pending_tasks):
    self.queue = PriorityQueue(pending_tasks, 
                   sort_key = lambda task: -chunk_utils.numSubTasks(task),
                   mapper = lambda offer: offer.id.value)

    def fitsIn(task, offer):
      """
      Checks if task resources are less than Offer resources

      """
      taskRes = chunk_utils.getResourcesValue(task.resources)
      offerRes = chunk_utils.getResourcesValue(task.offerRes)
      for res_name, res_value in taskRes:
        if res_value > offerRes[res_name]:
          return False
      return True

    def stealHalfSubTasks(task):
      """
      Returns a list of stolenSubTasks and removes them from the task
      
      """
      subTasks = [subTask for subTask in subTaskIterator(task)]
      stolenTasks = subTasks[len(subTasks)/2:]
      for stolenTask in stolenTasks:
        chunk_utils.removeSubTask(task, stolenTask.task_id)
        
      return stolenTasks

    def stealTasks(self, offer):
      """
      Give an offer to the queue. If accepted, it will return a
      new task chunk containing the subTasks it stole. 
      If rejected, returns None

      """
      popped_tasks = []
      stolenTasksChunk = None
      
      while self.queue.hasNext():
        task = self.queue.pop()
        if (chunk_utils.numSubTasks(task) > 1 and self.fitsIn(task, offer)):
          taskCopy = mesos_pb2.TaskInfo()
          taskCopy.CopyFrom(task)
          stolenTasks = stealHalfSubTasks(taskCopy)
          stolenTasksChunk = chunk_utils.newTaskChunk(offer.slave_id,
                                                  subTasks=stolenTasks)
          stolenTasksChunk.name = "stolen chunk"
          popped_tasks.append(taskCopy)
          break
        
        popped_tasks.append(taskCopy)

        for task in popped_tasks:
          self.queue.push(task)

      return stolenTasksChunk

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



#Task stealing methods
def roundRobinTaskStealing(self, driver, offers, pending_tasks):
    pass
