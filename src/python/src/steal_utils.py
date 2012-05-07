import chunk_utils
import heapq
import mesos_pb2


def getOfferSize(offer):
  listResources = []
  for resource in offer.resources:
    if resource.type == mesos_pb2.Value.SCALAR:
      listResources.append((resource.name, resource.scalar.value))
  listResources = sorted(listResources)
  return tuple(res[1] for res in listResources)


class PriorityQueue(object):

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
    item_key = heapq.heappop(self.__data)[1]
    item_value = self.mapping[item_key]
    del self.mapping[item_key]
    return item_value

  def hasNext(self):
    return len(self.__data) > 0

  def __len__(self):
    return len(self.__data)


def fitsIn(task, offer):
  """
  Checks if task resources are less than Offer resources
  """
  offerCopy = mesos_pb2.Offer()
  offerCopy.CopyFrom(offer)

  chunk_utils.decrementResources(offerCopy.resources, task.resources)
  if chunk_utils.isOfferValid(offerCopy):
    return True
  return False


class TaskQueue:
  """
  The TaskQueue receives offers and returns tasks to give up to that offer.
  """

  def __init__(self, pending_tasks):
    self.queue = PriorityQueue(pending_tasks,
            sort_key = lambda task: -chunk_utils.numSubTasks(task),
            mapper = lambda offer: offer.task_id.value)

  def stealSubTasks(self, taskChunk, numToSteal, stealFromBack):
    """
    Returns a list of stolenSubTasks and removes them from the taskChunk
    """
    subTasks = [subTask for subTask in
                     chunk_utils.subTaskIterator(taskChunk)]
    if stealFromBack:
      startIndex = len(subTasks) - numToSteal
      stolenTasks = subTasks[startIndex:]
    else:
      stolenTasks = subTasks[:numToSteal]

    for stolenTask in stolenTasks:
      chunk_utils.removeSubTask(taskChunk, stolenTask.task_id)

    return stolenTasks

  def stealTasks(self, offer, numToSteal=None, minNumTasks=2, stealFromBack=True):
    """
    Give an offer to the queue. If accepted, it will return a
    new task chunk containing the subTasks it stole.
    If rejected, returns None
    """
    popped_tasks = []
    stolenTasksChunk = None

    while self.queue.hasNext():
      task = self.queue.pop()

      if (chunk_utils.numSubTasks(task) > minNumTasks and fitsIn(task, offer)):
        taskCopy = mesos_pb2.TaskInfo()
        taskCopy.CopyFrom(task)
        if numToSteal is None:  #Steal Half
          numToSteal = chunk_utils.numSubTasks(taskCopy) / 2

        stolenTasks = self.stealSubTasks(taskCopy, numToSteal, stealFromBack)
        stolenTasksChunk = chunk_utils.newTaskChunk(offer.slave_id,
                executor=taskCopy.executor, subTasks=stolenTasks)

        popped_tasks.append(taskCopy)
        break

      popped_tasks.append(task)

    for task in popped_tasks:
      self.queue.push(task)

    return stolenTasksChunk


