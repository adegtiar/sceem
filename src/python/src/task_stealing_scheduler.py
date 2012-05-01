import chunk_utils

class TaskStealingScheduler(chunk_utils.TaskChunkScheduler):
    """
    A scheduler wrapper that allows sub tasks of task chunks to be
    stolen by other offers.
    """

    def __init__(self, scheduler):
        chunk_utils.SchedulerWrapper.__init__(self, scheduler)

    def resourceOffers(self, driver, offers):
        pass

    def resourceOffersStealing(self, driver, offers):
        pass

    def selectTasksToSteal(self, driver, offers, pending_tasks):
        pass

    def stealSubtasks(self, tasks):
        pass

    def frameworkMessage(self, executor_id, slave_id, driver, data):
        pass


def TaskStealingSchedulerDriver(chunk_utils.TaskChunkSchedulerDriver):

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
