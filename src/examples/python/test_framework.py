#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import pdb
import sys
import time

import mesos
import mesos_pb2
import steal_utils
import chunk_utils
import task_utils
from task_chunk_scheduler import TaskChunkSchedulerDriver
from task_stealing_scheduler import TaskStealingSchedulerDriver


# The number of slaves you intend to have in the cluster.
DEFAULT_NUM_SLAVES = 1
# The size of each task, in terms of the number of seconds it takes.
DEFAULT_TASK_TIME = 0.25
# The total work per slave, in seconds.
WORK_PER_SLAVE = 6
# The distribution to use.
DEFAULT_DISTRIBUTION = task_utils.Distribution.UNIFORM
# Default memory footprint of the executor.
DEFAULT_TASK_MEM = 32
# Flag to enable Task chunking.
ENABLE_TASK_CHUNKING = True
# Scheduler driver to use.
#SCHEDULER_DRIVER = TaskStealingSchedulerDriver
SCHEDULER_DRIVER = TaskChunkSchedulerDriver
# Multiple levels of Task chunking.
ENABLE_LEVELS = False
# Number of TaskChunks created of size num_total_tasks
NUM_TASK_CHUNKS = 4


class TestScheduler(mesos.Scheduler):
  def __init__(self, executor, num_slaves, task_time, num_total_tasks):
    self.executor = executor
    self.tasksLaunched = 0
    self.tasksFinished = 0

    self.num_slaves = num_slaves
    self.task_time = task_time
    self.num_total_tasks = num_total_tasks
    self.task_cpus = None
    self.all_tasks = None
    self.task_mem = DEFAULT_TASK_MEM
    self.distribution = DEFAULT_DISTRIBUTION
    if ENABLE_LEVELS:
      self.task_ids = set("Task_" + str(i) for i in range(num_total_tasks*NUM_TASK_CHUNKS))
    else:
      self.task_ids = set("Task_" + str(i) for i in range(num_total_tasks))

  def initializeTasks(self, offers, index=0):
    for resource in offers[0].resources:
        if resource.name == "cpus":
            self.task_cpus = resource.scalar.value

    return task_utils.getTaskList(self.num_total_tasks, self.task_mem,
            self.task_cpus, self.task_time, index=index)

  def registered(self, driver, frameworkId, masterInfo):
    print "Registered with framework ID %s" % frameworkId.value

  def resourceOffers(self, driver, offers):
    print "Got %d resource offers" % len(offers)

    # Initialize the tasks.
    if self.all_tasks is None:
      if ENABLE_LEVELS:
        self.all_tasks = []
        for i in xrange(NUM_TASK_CHUNKS):
          tasks = self.initializeTasks(offers, i)
          taskChunk = chunk_utils.newTaskChunk(offers[0].slave_id,
                                               executor=self.executor,
                                               subTasks = tasks)
          self.all_tasks.append(taskChunk)
          taskChunk.task_id.value = "Initial_taskChunk_"+str(i)
          taskChunk.name = "Initial_taskChunk_"+str(i)
      else:
        self.all_tasks = self.initializeTasks(offers)

    taskChunk = chunk_utils.newTaskChunk(offers[0].slave_id,
            executor=self.executor, subTasks = self.all_tasks)

    # dict <offerId, listofTaskChunks>
    dictOffers = task_utils.selectTasksforOffers(offers, [taskChunk],
            len(self.all_tasks), self.num_slaves,
            distribution=self.distribution, isTaskChunk=ENABLE_TASK_CHUNKING)

    for offer in offers:
      subTasks = dictOffers[offer.id.value]
      print "offer_id = %s, numSubTasks" % (offer.id.value), len(subTasks)
      if subTasks:
        offerTotalTasks = 0
        for subTask in subTasks:
          offerTotalTasks += chunk_utils.numSubTasks(subTask)

        self.tasksLaunched += offerTotalTasks
        index = min(offerTotalTasks,len(self.all_tasks))
        del self.all_tasks[:index]

        print "Accepting offer on %s to start task chunk" % offer.hostname
        driver.launchTasks(offer.id, subTasks)
      else:
        print "Rejecting offer {0}".format(offer.id.value)
        driver.launchTasks(offer.id, [])

  def statusUpdate(self, driver, update):
    #print "Task %s is in state %d" % (update.task_id.value, update.state)
    if update.state == mesos_pb2.TASK_FINISHED:
      if update.task_id.value in self.task_ids:
        self.task_ids.remove(update.task_id.value)
        self.tasksFinished += 1
      else:
        #print "Task chunk finished: {0}".format(update.task_id.value)
        if ENABLE_LEVELS:
          total_tasks = self.num_total_tasks*NUM_TASK_CHUNKS
        else:
          total_tasks = self.num_total_tasks
        if self.tasksFinished == total_tasks:
          print "All tasks done, exiting"
          driver.stop()


def runSimulation(master, num_slaves, task_time):
  print "Starting simulation with task time {0}".format(task_time)
  task_utils.resetGlobals()
  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = "default"
  executor.command.value = os.path.abspath("./test-executor")

  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "Test Framework (Python)"

  tasks_per_slave = int(WORK_PER_SLAVE / task_time)
  num_total_tasks = tasks_per_slave * num_slaves
  scheduler = TestScheduler(executor, num_slaves, task_time, num_total_tasks)

  start_time = time.time()
  driver = SCHEDULER_DRIVER(
    scheduler,
    framework,
    master)
  success = driver.run() != mesos_pb2.DRIVER_STOPPED
  elapsed_time = time.time() - start_time
  print "Finished simulation with task time {0} after {1} seconds".format(
          task_time, elapsed_time)
  return success


if __name__ == "__main__":
  if len(sys.argv) not in (3, 4):
    print "Usage: %s master num_slaves, [task_sizes]" % sys.argv[0]
    sys.exit(1)

  master = sys.argv[1]
  num_slaves = int(sys.argv[2])
  if len(sys.argv) == 4:
    task_times = [float(timeString) for timeString in sys.argv[3].split(',')]
  else:
    task_times = [DEFAULT_TASK_TIME]

  for task_time in task_times:
    success = runSimulation(master, num_slaves, task_time)
    if not success:
      print "Failed. Aborting."
      sys.exit(1)

  sys.exit(0)
