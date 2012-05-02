import chunk_utils
import mesos_pb2
import heapq


def getOfferSize(offer):
  listResources = []
  for resource in offer.resources:
    if resource.type == mesos_pb2.Value.SCALAR:
      listResources.append((resource.name, resource.scalar.value))
  listResources = sorted(listResources)
  return tuple(res[1] for res in listResources)
    

class PriorityQueue(object):
  
  def __init__(self, initial=None, sort_key=(lambda x: x), mapper=(lambda x: x)):
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

#The TaskQueue receives offers and returns tasks to give up to that offer.
class TaskQueue:
  def __init__(self, pending_tasks):
    self.queue = PriorityQueue(pending_tasks,
                               sort_key = lambda task: -chunk_utils.numSubTasks(task), mapper = lambda offer: offer.task_id.value)

  def fitsIn(self, task, offer):
    """
    Checks if task resources are less than Offer resources
    
    """
    offerCopy = mesos_pb2.Offer()
    offerCopy.CopyFrom(offer)
    
    chunk_utils.decrementResources(offerCopy, task)
    if chunk_utils.isOfferValid(offerCopy):
      return True
    return False
  
  def stealHalfSubTasks(self, task):
    """
    Returns a list of stolenSubTasks and removes them from the taskChunk
    
    """
    subTasks = [subTask for subTask in chunk_utils.subTaskIterator(task)]
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
        stolenTasks = self.stealHalfSubTasks(taskCopy)
        stolenTasksChunk = chunk_utils.newTaskChunk(offer.slave_id,
                                                    subTasks=stolenTasks)
        popped_tasks.append(taskCopy)
        break
        
      popped_tasks.append(task)
        
    for task in popped_tasks:
      self.queue.push(task)
      
    return stolenTasksChunk

