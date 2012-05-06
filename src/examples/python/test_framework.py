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
import task_stealing_scheduler
import task_utils


TOTAL_TASKS = 256
TASK_TIME = 0.25

TASK_CPUS = 2
TASK_MEM = 32

TIME = 32

TASK_IDS = set(str(i) for i in range(TOTAL_TASKS))
Tasks = task_utils.getTaskList(TOTAL_TASKS, TASK_CPUS, TASK_MEM, TIME,distribution=task_utils.Distribution.NORMAL)


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

      if TASKS:
        tasks = TASKS[:TOTAL_TASKS/4]
        del TASKS[:TOTAL_TASKS/4]
        self.tasksLaunched += TOTAL_TASKS/4

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




import mesos
import mesos_pb2
import chunk_utils
import steal_utils
import itertools
import random
import pickle
import numpy

class Distribution:
        UNIFORM, SPLIT, NORMAL = range(3)

def generateTaskId(self):
        """
        Generates a unique task chunk id string via a counter.
        """
        return "stolen_task_chunk_id_{0}".format(COUNTER.next())


def selectTasksforOffers(offers, tasks, ratio, isTaskChunk=False):
  """
  Maps tasks to Offers and returns a dict of <offer, taskChunks>
  """
  taskQueue = steal_utils.TaskQueue(tasks)
  offerQueue = steal_utils.PriorityQueue(offers,
                          sort_key = steal_utils.getOfferSize,
                          mapper = lambda offer: offer.id.value)

  stolenTasksChunks = defaultdict(list)

  while offerQueue.hasNext():
    offer = offerQueue.pop()
    if taskChunk:
      stolenTasksChunk = taskQueue.stealTasks(offer, ratio)
    else:
      stolenTasksChunk = taskQueue.stealTasks(offer, 1)

    if stolenTasksChunk:
      stolenTasksChunk.name = "task_chunk"
      stolenTasksChunk.task_id.value = self.generateTaskId()
      stolenTasksChunks[offer.id.value].append(stolenTasksChunk)

      offerCopy = mesos_pb2.Offer()
      offerCopy.CopyFrom(offer)

      chunk_utils.decrementResources(offerCopy.resources,
                                     stolenTasksChunk.resources)

      if not chunk_utils.isOfferEmpty(offerCopy):
        offerQueue.push(offerCopy)

    return stolenTasksChunks



def getTaskList(numTasks, sizeMem, sizeCpu, taskTime,
                distribution=None, taskTime2=0):
  """
  Creates new tasks specified by config
  """
  taskTimes = getTaskTimes(numTasks, taskTime, distribution, taskTime2)
  tasks = []
  for i in xrange(numTasks):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = "Task_"+str(i)
    task.name = "sub task name"

    cpu = task.resources.add()
    cpu.name = "cpus"
    cpu.type = mesos_pb2.Value.SCALAR
    cpu.scalar.value = sizeCpu

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = sizeMem

    task.data = pickle.dumps(taskTimes[i])
    tasks.append(task)

  return tasks


def getTaskTimes(numTasks, time, distribution=Distribution.UNIFORM, time2 =0):
  """
  Generate TaskTimes based on given distribution
  """
  if distribution==Distribution.UNIFORM:
    taskTime = [time for i in xrange(numTasks)]

  if distribution==Distribution.SPLIT:
    taskTime = [time if (i>numTasks/2) else time2 for i in xrange(numTasks)]

  if distribution==Distribution.NORMAL:
    #TODO: clip instead of using abs
    taskTime = [abs(random.normalvariate(0,1))*time for i in xrange(numTasks)]

  return taskTime





TASKS = getTaskList(TOTAL_TASKS, 32, 2, TASK_TIME, Distribution.NORMAL)




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
