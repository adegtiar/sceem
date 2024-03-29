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

import chunk_utils
import mesos
import mesos_pb2
import steal_utils
import task_chunk_scheduler


TOTAL_TASKS = 256

TASK_CPUS = 2
TASK_MEM = 32

TASK_IDS = set(str(i) for i in range(TOTAL_TASKS))

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
    for offer in offers:

      tasks = []
      print "Got resource offer %s" % offer.id.value
      while self.tasksLaunched < TOTAL_TASKS:
        tid = self.tasksLaunched

        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.name = "task %d" % tid

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        if not steal_utils.fitsIn(task, offer):
            print "Offer isn't enough to run task. Ignoring."
            break

        self.tasksLaunched += 1
        print "Adding subtask %d to chunk" % tid

        tasks.append(task)
        if len(tasks) > TOTAL_TASKS / 2:
            break

      if tasks:
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
    global TOTAL_TASKS
    if update.state == mesos_pb2.TASK_FINISHED:
      if update.task_id.value in TASK_IDS:
        self.tasksFinished += 1
        if self.tasksFinished == TOTAL_TASKS:
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


  driver = task_chunk_scheduler.TaskChunkSchedulerDriver(
    TestScheduler(executor),
    framework,
    sys.argv[1])

  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
