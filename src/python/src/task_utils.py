import mesos
import mesos_pb2
import chunk_utils
import steal_utils
import itertools
import random
import pickle
import numpy
from collections import defaultdict

class Distribution:
  UNIFORM, SPLIT, NORMAL = range(3)


COUNTER = itertools.count()
NORMAL_DIST = None
SIGMA = 1
LEFT_BOUND = SIGMA * -1.5
RANGE = 2 * -LEFT_BOUND


def getTaskChunkSize(distribution, numTasks, numSlaves):
  """
  Return Number of tasks per task Chunk
  """
  global NORMAL_DIST
  if distribution == Distribution.UNIFORM:
    return numTasks / numSlaves
  elif distribution == Distribution.NORMAL:
    if NORMAL_DIST is None:
      NORMAL_DIST = getNormList(numTasks, numSlaves)
      print "NORMAL_DIST" , NORMAL_DIST

    if len(NORMAL_DIST) == 0:
      return None

    rand = random.randint(0, len(NORMAL_DIST) - 1)
    tasks = NORMAL_DIST[rand]
    return tasks


def getNormList(numTasks, numBuckets):
  """
  Returns a list of bucket sizes distributed normally, where the indices
  of the list correspond to the bucket index. The sizes add up to numTasks.
  """
  interval = RANGE / numBuckets
  normNumbers = [random.normalvariate(0, SIGMA) for i in xrange(numTasks)]

  dictBuckets = defaultdict(int)
  for normVal in normNumbers:
    currValue = LEFT_BOUND + interval
    bucketIndex = 0
    while currValue < normVal and bucketIndex < numBuckets - 1:
      currValue += interval
      bucketIndex += 1
    dictBuckets[bucketIndex] += 1

  print "length of dictBuckets" , len(dictBuckets)
  return dictBuckets.values()


def generateTaskId():
  """
  Generates a unique task chunk id string via a counter.
  """
  return "created_task_chunk_id_{0}".format(COUNTER.next())


def selectTasksforOffers(offers, tasks, numTasks, numSlaves, distribution=Distribution.UNIFORM, isTaskChunk=False):
  """
  Maps tasks to Offers and returns a dict of <offer, taskChunks>
  """
  taskQueue = steal_utils.TaskQueue(tasks)
  offerQueue = steal_utils.PriorityQueue(offers,
                          sort_key = steal_utils.getOfferSize,
                                         mapper = lambda offer: offer.id.value)

  createdTasksChunks = defaultdict(list)

  while offerQueue.hasNext():
    offer = offerQueue.pop()
    if isTaskChunk:
      tasks_per_chunk = getTaskChunkSize(distribution, numTasks, numSlaves)
      if tasks_per_chunk is None:
        createdTasksChunk = None
      else:
        createdTasksChunk = taskQueue.stealTasks(offer, tasks_per_chunk, 0, False)
      if createdTasksChunk:
        NORMAL_DIST.remove(tasks_per_chunk)
    else:
      createdTasksChunk = taskQueue.stealTasks(offer, 1, 0, False)

    if createdTasksChunk:
      createdTasksChunk.name = "task_chunk"
      createdTasksChunk.task_id.value = generateTaskId()
      createdTasksChunks[offer.id.value].append(createdTasksChunk)

      offerCopy = mesos_pb2.Offer()
      offerCopy.CopyFrom(offer)

      chunk_utils.decrementResources(offerCopy.resources,
                                     createdTasksChunk.resources)

      if not chunk_utils.isOfferEmpty(offerCopy):
        offerQueue.push(offerCopy)

  return createdTasksChunks

def getTaskList(numTasks, sizeMem, sizeCpu, taskTime,
                distribution=Distribution.UNIFORM, taskTime2=0):
  """
  Creates new tasks specified by config
  """
  taskTimes = getTaskTimes(numTasks, taskTime, distribution, taskTime2)
  tasks = []
  for i in xrange(numTasks):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = "Task_"+str(i)
    task.name = "sub task name"

    cpu = task.resources.add()
    cpu.name = "cpus"
    cpu.type = mesos_pb2.Value.SCALAR
    cpu.scalar.value = sizeCpu

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = sizeMem

    task.data = pickle.dumps(taskTimes[i])
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
