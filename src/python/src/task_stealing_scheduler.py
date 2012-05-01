import chunk_utils

class TaskStealingScheduler(chunk_utils.TaskChunkScheduler):
    """
    A scheduler wrapper that allows sub tasks of task chunks to be
    stolen by other offers.
    """

    def __init__(self, scheduler):
        chunk_utils.SchedulerWrapper.__init__(self, scheduler)
