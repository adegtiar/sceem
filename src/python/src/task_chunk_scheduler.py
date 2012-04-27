import mesos
import chunk_utils

class TaskChunkScheduler(chunk_utils.SchedulerWrapper):
    pass

class TaskChunkSchedulerDriver(mesos.MesosSchedulerDriver):
    pass
