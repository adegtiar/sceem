import chunk_utils
import steal_utils

class TaskStealingScheduler(chunk_utils.TaskChunkScheduler):
    """
    A scheduler wrapper that allows sub tasks of task chunks to be
    stolen by other offers.
    """

    def __init__(self, scheduler):
        chunk_utils.SchedulerWrapper.__init__(self, scheduler)
        self.stolenTaskIds = set()

    def resourceOffers(self, driver, offers):
        if offer_order == TASK_STEALING_FIRST:
                firstOffering = resourceOffersStealing
                secondOffering = super().resourceOffers
            else:
                firstOffering = resourceOffersStealing
                secondOffering = super().resourceOffers

        firstOffering(driver, offers)
            driver.updateOffers(offers)
            secondOffering(driver, offers)
            driver.clearConsumedResources(offers.id)

    def resourceOffersStealing(self, driver, offers):
        Map<offer,tasks> tasksToSteal = selectTasksToSteal(driver, offers, driver.getPendingTasks())
        for offer, tasks in tasksToSteal.iteriterms():
            stealSubtasks(tasks)
            driver.updateTasks(offer, tasks)
            driver.launchTasks(tasks)

    def selectTasksToSteal(self, driver, offers, pending_tasks):
        return steal_utils.roundRobinStealing(driver, offers, pendingTasks)

    def stealSubtasks(self, tasks):
        stolenTasks = (task.executor.executor_id, task.task_id) for task in tasks)
        self.stolenTasks.update(stolenTasks)
        driver.killSubtasks(tasks)

    def frameworkMessage(self, executor_id, slave_id, driver, data):
        message = getMessage(data)
        if (executor_id, message[1].task_id) in stolenTasks:
            # TODO: log this and potentially remove from stolenTasks.
        elif message and message[0] == SUBTASK_UPDATE:
            super.statusUpdate(driver, message[1])
        else:
            super.frameworkMessage(driver, data)


def TaskStealingSchedulerDriver(chunk_utils.TaskChunkSchedulerDriver):

    def __init__(self, ...):
        Map<OfferID, Resources> consumedResources;
        TaskTable pendingTasks

    def updateTasks(tasks):
        #Have underlying subtasks inherit from TaskChunk
        for task in tasks:
            for subTask in subTaskIterator(task):
                subTask.executor.MergeFrom(task.executor)
                task.slave_id.value = task.slave_id.value
                if isTaskChunk(subTask):
                    updateTasks((subTask,))

    def updateResourceOffer(offerId, tasks):
        consumedResources[offerId] += sum(task.resources for task in tasks)

    # Updates the resources and calls the super method
    def launchTasks(self, offerId, tasks, filters)
        updateResourceOffer(offerId, tasks)
        for task in tasks:
            pendingTasks.addTask(task)
        super.launchTasks(offerId, tasks filters)

    def updateOffers(offers):
        for offer in offers:
            offer.resources -= consumedResources[offer.id]

    def clearConsumedResources(offers_id):
        del consumedResources[offer.id]
