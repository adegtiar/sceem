import chunk_utils
import itertools
import mesos_pb2
import steal_utils

from chunk_utils import TaskChunkScheduler, TaskChunkSchedulerDriver


class TaskStealingScheduler(TaskChunkScheduler):
    """
    A scheduler wrapper that allows sub tasks of task chunks to be
    stolen by other offers.
    """
    TASK_STEALING_FIRST, RESOURCE_OFFERS_FIRST = range(2)

    def __init__(self, scheduler,
            offerOrder = TaskStealingScheduler.TASK_STEALING_FIRST):
        TaskChunkScheduler.__init__(self, scheduler)
        self.offerOrder = offerOrder
        self.stolenTaskIds = set()
        self.counter = itertools.count()

    def resourceOffers(self, driver, offers):
        """
        Responds to a resource offer by stealing tasks and/or forwarding
        the offer to the underlying framework.
        """
        firstOffering = self.resourceOffersStealing
        secondOffering = super(TaskStealingScheduler, self).resourceOffers

        if offer_order == TaskStealingScheduler.RESOURCE_OFFERS_FIRST:
            firstOffering, secondOffering = secondOffering, firstOffering

        firstOffering(driver, offers)
        # Update the offer resources by the amount the tasks consumed.
        driver.updateOffers(offers)
        secondOffering(driver, offers)

        driver.clearConsumedResources(offers)

    def resourceOffersStealing(self, driver, offers):
        """
        Steals currently pending tasks and launches them on the new offer.
        """
        tasksToSteal = self.selectTasksToSteal(driver, offers, driver.pendingTasks)

        for offer, tasks in tasksToSteal.iteriterms():
            self.stealSubtasks(tasks)
            driver.launchTasks(tasks)

    def selectTasksToSteal(self, driver, offers, pending_tasks):
        """
        Selects from the table of pending tasks a number of sub tasks to
        steal. Returns a mapping of offerId to the list of task (chunks) to run.
        """
        taskQueue = steal_utils.TaskQueue(pending_tasks)
        offerQueue = steal_utils.PriorityQueue(offers, sort_key=offerSize,
                mapper=lambda offer: offer.id.value)

        stolenTasksChunks = defaultdict(list)
        while offerQueue.hasNext():
            offer = offerQueue.pop()
            stolenTasksChunk = taskQueue.stealTasks(offer)
            if stolenTasksChunk:
                stoleTasksChunk.name = "Stolen task"
                stolenTasksChunk.task_id.value = self.generateTaskId()

                stolenTasksChunks[offer.id].append(stolenTasksChunk)

                offerCopy = mesos_pb2.Offer()
                offerCopy.CopyFrom(offer)

                chunk_utils.resourcesDecrement(offerCopy.resources,
                        stolenTaskChunk.resources)
                if not chunk_utils.isOfferEmpty(offerCopy):
                    offerQueue.push(offerCopy)

        return stolenTaskChunks

    def stealSubTasks(self, stolenSubTaskIds):
        """
        Informs the executors who owned the stolen tasks, and updates
        local metadata.
        """
        subTasks = driver.killSubTasks(subTaskIds)
        stolenTasks = ((task.executor.executor_id.value, task.task_id.value)
                for task in subTasks)
        self.stolenTasksIds.update(stolenTasks)

    def frameworkMessage(self, executor_id, slave_id, driver, data):
        """
        Receives a framework message. Squelches stolen task updates and
        updates the table on subtask updates.
        """
        message = SubTaskMessage.fromString(data)
        if message.isValid() and message.getType() == SubTaskMessage.SUBTASK_UPDATE:
            update = message.getPayload()
            if (executor_id.value, update.task_id.value) in self.stolenTaskIds:
                # Potentially log this and remove from stolenTasks.
                pass
            else:
                TaskChunkScheduler.frameworkMessage(self, driver, data)
        else:
            TaskChunkScheduler.frameworkMessage(self, driver, data)

    def statusUpdate(self, driver, update):
        """
        Updates internal state before passing on to the underlying framework.
        """
        if (update.task_id in driver.pendingTasks and
                chunk_utils.isTerminalUpdate(update)):
            del driver.pendingTasks[update.task_id]
        TaskChunkScheduler.statusUpdate(self, driver, update)

    def generateTaskId(self):
        return "task_chunk_id_{0}".format(next(self.counter))


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

    def launchTasks(self, offerId, tasks, filters):
        """
        Keeps track of the task and its launched resources before launching.
        """
        consumedResources = self.consumedResource[offerId]
        for task in tasks:
            # Update the resource offer with the task resources.
            chunk_utils.resourcesIncrement(consumedResources, task.resources)
            # Add the task to the tracked tasked.
            pendingTasks.addTask(task)
        TaskChunkSchedulerDriver.launchTasks(self, offerId, tasks, filters)

    def updateOffers(self, offers):
        """
        Reduces the given offers by the resources consumed by the tasks
        launched for this offer.
        """
        for offer in offers:
            consumedResources = self.consumedResources[offer.id]
            chunk_utils.decrementResources(offer.resources, consumedResources)

    def clearConsumedResources(self, offers):
        """
        Clears the resources for offers tracked as a result of tasks launched.
        """
        for offer in offers:
            del consumedResources[offer.id]

    def killSubTasks(subTaskIds):
        """
        Kills the sub tasks with the given IDs, and clears them from the table.
        """
        subTasks = []
        for subTaskId in subTaskIds:
            subTasks.append(self.pendingTasks[subTaskId])
            del self.pendingTasks[subTaskId]

        TaskChunkSchedulerDriver.killSubTasks(self, subTasks)
        return subTasks
