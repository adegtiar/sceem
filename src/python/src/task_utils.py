import mesos
import mesos_pb2
import chunk_utils
import steal_utils
import itertools
import random
import pickle
import numpy

class Distribution:
  UNIFORM, SPLIT, NORMAL = range(3)

def generateTaskId(self):
  """
  Generates a unique task chunk id string via a counter.
  """
  return "stolen_task_chunk_id_{0}".format(COUNTER.next())


def selectTasksforOffers(offers, tasks, ratio, isTaskChunk=False):
  """
  Maps tasks to Offers and returns a dict of <offer, taskChunks>
  """
  taskQueue = steal_utils.TaskQueue(tasks)
  offerQueue = steal_utils.PriorityQueue(offers,
                          sort_key = steal_utils.getOfferSize,
                          mapper = lambda offer: offer.id.value)

  stolenTasksChunks = defaultdict(list)

  while offerQueue.hasNext():
    offer = offerQueue.pop()
    if taskChunk:
      stolenTasksChunk = taskQueue.stealTasks(offer, ratio)
    else:
      stolenTasksChunk = taskQueue.stealTasks(offer, 1)

    if stolenTasksChunk:
      stolenTasksChunk.name = "task_chunk"
      stolenTasksChunk.task_id.value = self.generateTaskId()
      stolenTasksChunks[offer.id.value].append(stolenTasksChunk)

      offerCopy = mesos_pb2.Offer()
      offerCopy.CopyFrom(offer)

      chunk_utils.decrementResources(offerCopy.resources,
                                     stolenTasksChunk.resources)

      if not chunk_utils.isOfferEmpty(offerCopy):
        offerQueue.push(offerCopy)

    return stolenTasksChunks



def getTaskList(numTasks, sizeMem, sizeCpu, taskTime,
                distribution=None, taskTime2=0):
  """
  Creates new tasks specified by config
  """
  taskTimes = getTaskTimes(numTasks, taskTime, distribution, taskTime2)
  tasks = []
  for i in xrange(numTasks):
    task = mesos.TaskInfo()
    task.task_id.value = "Task_"+str(i)

    cpu = task.resources.add()
    cpu.name = "cpus"
    cpu.type = mesos_pb2.Value.SCALAR
    cpu.scalar.value = sizeCpu

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = sizeMem

    task.args = pickle.dumps(taskTimes[i])
    tasks.append(task)

  return tasks


def getTaskTimes(numTasks, time, distribution=Distribution.UNIFORM, time2=0):
  """
  Generate TaskTimes based on given distribution
  """
  if distribution==Distribution.UNIFORM:
    taskTime = [time for i in xrange(numTasks)]

  if distribution==Distribution.SPLIT:
    taskTime = [time if (i>numTasks/2) else time2 for i in xrange(numTasks)]

  if distribution==Distribution.NORMAL:
    #TODO: clip instead of using abs
    taskTime = [abs(random.normalvariate(0,1))*time for i in xrange(numTasks)]

  return taskTime


