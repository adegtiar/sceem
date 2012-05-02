import chunk_utils
import itertools
import mesos_pb2
import steal_utils

from collections import defaultdict
from chunk_utils import SubTaskMessage, subTaskIterator
from task_chunk_scheduler import TaskChunkScheduler, TaskChunkSchedulerDriver


class TaskStealingScheduler(TaskChunkScheduler):
    """
    A scheduler wrapper that allows sub tasks of task chunks to be
    stolen by other offers.
    """
    TASK_STEALING_FIRST, RESOURCE_OFFERS_FIRST = range(2)

    def __init__(self, scheduler, offerOrder = None):
        TaskChunkScheduler.__init__(self, scheduler)
        if offerOrder is None:
            offerOrder = TaskStealingScheduler.TASK_STEALING_FIRST
        self.offerOrder = offerOrder
        self.stolenTaskIds = set()
        self.counter = itertools.count()

    def resourceOffers(self, driver, offers):
        """
        Responds to a resource offer by stealing tasks and/or forwarding
        the offer to the underlying framework.
        """
        firstOffering = TaskStealingScheduler.resourceOffersStealing
        secondOffering = TaskChunkScheduler.resourceOffers

        if self.offerOrder == TaskStealingScheduler.RESOURCE_OFFERS_FIRST:
            firstOffering, secondOffering = secondOffering, firstOffering

        firstOffering(self, driver, offers)
        # Update the offer resources by the amount the tasks consumed.
        driver.updateOffers(offers)
        secondOffering(self, driver, offers)

        driver.clearConsumedResources(offers)

    def resourceOffersStealing(self, driver, offers):
        """
        Steals currently pending tasks and launches them on the new offer.
        """
        tasksToSteal = self.selectTasksToSteal(driver, offers, driver.pendingTasks)

        for offerIdValue, taskChunks in tasksToSteal.iteritems():
            offerId = mesos_pb2.OfferID()
            offerId.value = offerIdValue

            for taskChunk in taskChunks:
                subTaskIds = [task.task_id for task in subTaskIterator(taskChunk)]
                self.stealSubTasks(driver, subTaskIds)
                print "Stolen tasks to be run in new task chunk: {0}".format(
                        taskChunk.task_id.value)
            driver.launchTasks(offerId, taskChunks)

    def selectTasksToSteal(self, driver, offers, pendingTasks):
        """
        Selects from the table of pending tasks a number of sub tasks to
        steal. Returns a mapping of offerId to the list of task (chunks) to run.
        """
        taskQueue = steal_utils.TaskQueue(pendingTasks)
        offerQueue = steal_utils.PriorityQueue(offers,
                sort_key = steal_utils.getOfferSize,
                mapper = lambda offer: offer.id.value)

        stolenTasksChunks = defaultdict(list)
        while offerQueue.hasNext():
            offer = offerQueue.pop()

            stolenTasksChunk = taskQueue.stealTasks(offer)
            if stolenTasksChunk:
                stolenTasksChunk.name = "Stolen task"
                stolenTasksChunk.task_id.value = self.generateTaskId()

                stolenTasksChunks[offer.id.value].append(stolenTasksChunk)

                offerCopy = mesos_pb2.Offer()
                offerCopy.CopyFrom(offer)

                chunk_utils.decrementResources(offerCopy.resources,
                        stolenTasksChunk.resources)
                if not chunk_utils.isOfferEmpty(offerCopy):
                    offerQueue.push(offerCopy)

        return stolenTasksChunks

    def stealSubTasks(self, driver, stolenSubTaskIds):
        """
        Informs the executors who owned the stolen tasks, and updates
        local metadata.
        """
        for parentId, subTask in driver.killSubTasks(stolenSubTaskIds):
            print "\tStealing {0} from {1}".format(subTask.task_id.value, parentId.value)
            self.stolenTaskIds.add((parentId.value, subTask.task_id.value))

    def frameworkMessage(self, driver, executor_id, slave_id, data):
        """
        Receives a framework message. Squelches stolen task updates and
        updates the table on subtask updates.
        """
        message = SubTaskMessage.fromString(data)
        if message.isValid() and message.getType() == SubTaskMessage.SUBTASK_UPDATE:
            parentTaskId, update = message.getPayload()
            if (parentTaskId.value, update.task_id.value) in self.stolenTaskIds:
                # Potentially log this and remove from stolenTasks.
                print "Squelched update from stolen task '{0}' in task chunk '{1}'".format(
                        update.task_id.value, parentTaskId.value)
            else:
                TaskChunkScheduler.frameworkMessage(self, driver, executor_id,
                        slave_id, data)
        else:
            TaskChunkScheduler.frameworkMessage(self, driver, executor_id,
                    slave_id, data)

    def statusUpdate(self, driver, update):
        """
        Updates internal state before passing on to the underlying framework.
        """
        if (update.task_id in driver.pendingTasks and
                chunk_utils.isTerminalUpdate(update)):
            del driver.pendingTasks[update.task_id]
        TaskChunkScheduler.statusUpdate(self, driver, update)

    def generateTaskId(self):
        """
        Generates a unique task chunk id string via a counter.
        """
        return "task_chunk_id_{0}".format(self.counter.next())


class TaskStealingSchedulerDriver(TaskChunkSchedulerDriver):
    """
    A scheduler driver wrapper that allows sub tasks of task chunks to be
    stolen by other offers.
    """

    def __init__(self, scheduler, framework, master, outerScheduler=None):
        if not isinstance(scheduler, TaskStealingScheduler):
            scheduler = TaskStealingScheduler(scheduler)
        # Wrap the outer scheduler with the driver overriding wrapper.
        if not outerScheduler:
            outerScheduler = chunk_utils.DriverOverridingScheduler(scheduler, self)

        TaskChunkSchedulerDriver.__init__(self, scheduler, framework, master, outerScheduler)

        self.consumedResources = defaultdict(lambda: mesos_pb2.Offer().resources)
        self.pendingTasks = chunk_utils.TaskTable()

    def launchTasks(self, offerId, tasks, filters=None):
        """
        Keeps track of the task and its launched resources before launching.
        """
        consumedResources = self.consumedResources[offerId.value]
        for task in tasks:
            # Update the resource offer with the task resources.
            chunk_utils.incrementResources(consumedResources, task.resources)
            # Add the task to the tracked tasked.
            self.pendingTasks.addTask(task)
        TaskChunkSchedulerDriver.launchTasks(self, offerId, tasks, filters)

    def updateOffers(self, offers):
        """
        Reduces the given offers by the resources consumed by the tasks
        launched for this offer.
        """
        for offer in offers:
            consumedResources = self.consumedResources[offer.id.value]
            chunk_utils.decrementResources(offer.resources, consumedResources)

    def clearConsumedResources(self, offers):
        """
        Clears the resources for offers tracked as a result of tasks launched.
        """
        for offer in offers:
            del self.consumedResources[offer.id.value]

    def killSubTasks(self, subTaskIds):
        """
        Kills the sub tasks with the given IDs, and clears them from the table.
        """
        subTasks = []
        parentIds = []
        for subTaskId in subTaskIds:
            # Backwards compatibility for passing sub tasks instead of IDs.
            if isinstance(subTaskId, mesos_pb2.TaskInfo):
                subTaskId = subTaskId.task_id
            parentIds.append(self.pendingTasks.getParent(subTaskId).task_id)
            subTasks.append(self.pendingTasks[subTaskId])
            del self.pendingTasks[subTaskId]

        TaskChunkSchedulerDriver.killSubTasks(self, subTasks)
        return zip(parentIds, subTasks)
