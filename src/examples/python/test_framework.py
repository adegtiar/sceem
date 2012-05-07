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
import task_stealing_scheduler


# The number of slaves you intend to have in the cluster.
NUM_SLAVES = 2
# The size of each task, in terms of the number of seconds it takes.
TASK_TIME = 0.25
# The total intended length of the simulation.
SIMULATION_TIME = 32
# The distribution to use.
DISTRIBUTION = task_utils.Distribution.NORMAL

tasks_per_slave = int(SIMULATION_TIME / TASK_TIME)
num_total_tasks = tasks_per_slave * NUM_SLAVES
TASK_IDS = set("Task_" + str(i) for i in range(num_total_tasks))

TASK_MEM = 32
task_cpus = None

all_tasks = None

#all_tasks = getTaskList(num_total_tasks, 32, 2, TASK_TIME, Distribution.NORMAL)

class TestScheduler(mesos.Scheduler):
  def __init__(self, executor):
    self.executor = executor
    self.tasksLaunched = 0
    self.tasksFinished = 0
    self.subTasksToKill = []

  def registered(self, driver, frameworkId, masterInfo):
    print "Registered with framework ID %s" % frameworkId.value

  def resourceOffers(self, driver, offers):
    print "Got %d resource offers" % len(offers)
    global all_tasks

    # Initialize the tasks.
    if all_tasks is None:
        for resource in offers[0].resources:
            if resource.name == "cpus":
                task_cpus = resource.scalar.value
        all_tasks = task_utils.getTaskList(num_total_tasks, TASK_MEM, task_cpus, TASK_TIME,
                distribution=DISTRIBUTION)

    for offer in offers:
      if all_tasks:
        tasks_per_chunk = num_total_tasks / NUM_SLAVES
        tasks = all_tasks[:tasks_per_chunk]
        del all_tasks[:tasks_per_chunk]
        self.tasksLaunched += tasks_per_chunk

        taskChunk = chunk_utils.newTaskChunk(offer.slave_id,
                executor=self.executor, subTasks=tasks)
        taskChunk.task_id.value = "chunk_id_{0}".format(self.tasksLaunched)
        taskChunk.name = "taskChunk"
        self.taskChunkId = taskChunk.task_id

        print "Accepting offer on %s to start task chunk" % offer.hostname
        driver.launchTasks(offer.id, [taskChunk])
      else:
        print "Rejecting offer {0}".format(offer.id.value)
        driver.launchTasks(offer.id, [])

  def statusUpdate(self, driver, update):
    print "Task %s is in state %d" % (update.task_id.value, update.state)
    global num_total_tasks
    if update.state == mesos_pb2.TASK_FINISHED:
      if update.task_id.value in TASK_IDS:
        self.tasksFinished += 1
        if self.tasksFinished == num_total_tasks:
          print "All tasks done, exiting"
          driver.stop()
      else:
        print "Task chunk finished: {0}".format(update.task_id.value)


if __name__ == "__main__":
  if len(sys.argv) != 2:
    print "Usage: %s master" % sys.argv[0]
    sys.exit(1)

  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = "default"
  executor.command.value = os.path.abspath("./test-executor")

  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "Test Framework (Python)"


  driver = task_stealing_scheduler.TaskStealingSchedulerDriver(
    TestScheduler(executor),
    framework,
    sys.argv[1])

  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
