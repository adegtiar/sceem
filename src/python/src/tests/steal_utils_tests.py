#!/usr/bin/env python

# Add the cwd to the lookup path for modules.
import sys
sys.path.append(".")

# Replace the mesos module with the stub implementation.
import stub_mesos
sys.modules["mesos"] = stub_mesos

from chunk_utils import *
import mesos_pb2
import unittest
import steal_utils
from mock import Mock


class TestPriorityQueue(unittest.TestCase):

    def setUp(self):
        listTup = []
        for i in xrange(10):
            tup = (i, "a"+str(i))
            listTup.append(tup)
            # (1, "a1") -> mapper = a1, item_key = (1, a1)
        self.pq = steal_utils.PriorityQueue(initial=listTup,
                                           sort_key = lambda x: x[0],
                                           mapper=lambda x: x[1])

    def test_push(self):
        tup = (-1, "a"+str(-1))
        self.pq.push(tup)
        self.assertTrue(self.pq.pop() == tup)

    def test_pop(self):
        for i in xrange(10):
            self.assertTrue(self.pq.pop() == (i,"a"+str(i)))

    def test_hasNext(self):
        for i in xrange(10):
            self.assertTrue(self.pq.hasNext())
            self.pq.pop()
        self.assertFalse(self.pq.hasNext())
        


class TestTaskQueue(unittest.TestCase):
    """
    Tests for the TaskQueue.
    """

    def setUp(self):
        
        self.slave_id = Mock()
        self.slave_id.value = "slave_id"
        self.counter = 0
        self.table = self.new_table()
        self.pending_tasks = [subtask for subtask in subTaskIterator(self.table)]
        self.queue = steal_utils.TaskQueue(self.pending_tasks)
        self.offer = mesos_pb2.Offer()
            
    def new_table(self):
        tasks = []
        self.taskChunk = self.new_task_chunk(10, 10, 5)
        tasks.append(self.taskChunk)
        return newTaskChunk(self.slave_id, subTasks=tasks)
      
    def new_tasks(self, num, resSize=0, resNum=0):
        tasks = []
        for i in range(num):
            task = mesos_pb2.TaskInfo()
            task.task_id.value = "id{0}".format(i)
            if resNum > 0:
                self.add_resources(task, resSize, resNum)

            tasks.append(task)
            
        return tasks

    def new_task_chunk(self, subTasksPerChunk, resSize=0, resNum=0):
        taskChunk = newTaskChunk(self.slave_id, subTasks=self.new_tasks(subTasksPerChunk,
                                                resSize, resNum))
        taskChunk.task_id.value = "chunk_id" + str(self.counter)
        self.counter +=1
        return taskChunk

    def add_resources(self, taskChunk, sizeRes, numRes,
                      dictRes=None, operator=None):
        for i in xrange(numRes):
            resource = taskChunk.resources.add()
            resource.name = str(i)
            resource.type = mesos_pb2.Value.SCALAR
            resource.scalar.value = sizeRes
            if dictRes!=None:
                dictRes[resource.name] = operator(dictRes[resource.name], sizeRes)
                
    def test_fitsInTrue(self):
        subTasksPerChunk = 5
        task = newTaskChunk(self.slave_id, subTasks=self.new_tasks(subTasksPerChunk))
        self.add_resources(task, 10, 2)
        offer = mesos_pb2.Offer()
        self.add_resources(offer, 5, 2)
        self.assertFalse(self.queue.fitsIn(task, offer))

    def test_fitsInFalse(self):
        subTasksPerChunk = 5
        task = newTaskChunk(self.slave_id, subTasks=self.new_tasks(subTasksPerChunk))
        self.add_resources(task, 3, 2)
        offer = mesos_pb2.Offer()
        self.add_resources(offer, 5, 2)
        self.assertTrue(self.queue.fitsIn(task, offer))

    def test_fitsInMissing(self):
        subTasksPerChunk = 5
        task = newTaskChunk(self.slave_id, subTasks=self.new_tasks(subTasksPerChunk))
        self.add_resources(task, 3, 3)
        offer = mesos_pb2.Offer()
        self.add_resources(offer, 5, 2)
        self.assertFalse(self.queue.fitsIn(task, offer))

    def test_stealHalfSubTasks(self):
        subTasksPerChunk = 5
        task = newTaskChunk(self.slave_id, subTasks=self.new_tasks(subTasksPerChunk))
        subTasks = [subTask for subTask in subTaskIterator(task)]
        stolenTasksExpected = subTasks[len(subTasks)/2:]

        stolenTasks = self.queue.stealHalfSubTasks(task)
        newSubTasks = [subTask for subTask in subTaskIterator(task)]
        for task in stolenTasks:
            self.assertTrue(task in stolenTasksExpected)
            self.assertFalse(task in newSubTasks)

    def test_stealTasks(self):
        offer = mesos_pb2.Offer()
        self.add_resources(offer, 10, 5)

        taskChunk = self.queue.stealTasks(offer)
        subTasks = [subTask for subTask in subTaskIterator(taskChunk)]

        self.assertTrue(self.queue.queue.hasNext())
        remainingTaskChunk = self.queue.queue.pop()
        subTasksRemaining = [subTask for subTask in subTaskIterator(
            remainingTaskChunk)]
        
        for task in subTasks:
            self.assertFalse(task in subTasksRemaining)

    def test_stealTasksTwo(self):
        offer = mesos_pb2.Offer()
        self.add_resources(offer, 10, 5)
        newtaskChunk = self.new_task_chunk(20, 20, 5)
        addSubTask(self.table, newtaskChunk)

        self.pending_tasks = [subtask for subtask in subTaskIterator(self.table)]
        self.queue = steal_utils.TaskQueue(self.pending_tasks)

        self.assertTrue(len(self.queue.queue)== 2)
        taskChunk = self.queue.stealTasks(offer)
        
        subTasks = [subTask for subTask in subTaskIterator(taskChunk)]
        
        self.assertTrue(self.queue.queue.hasNext())
        origTaskChunk = self.queue.queue.pop()
        origsubTasks = [subTask for subTask in subTaskIterator(origTaskChunk)]
        self.assertTrue(len(origsubTasks) == 20)

        
        self.assertTrue(self.queue.queue.hasNext())
        remainingTaskChunk = self.queue.queue.pop()

        self.assertFalse(self.queue.queue.hasNext())
        subTasksRemaining = [subTask for subTask in subTaskIterator(
            remainingTaskChunk)]
        
        for task in subTasks:
            self.assertFalse(task in subTasksRemaining)

    def test_getOfferSize(self):
        offer  = mesos_pb2.Offer()
        dictOff = defaultdict(int)
        self.add_resources(offer, 6, 10, dictOff, operator.add)
        tupExpected = tuple(item[1] for item in sorted(dictOff.items()))
        self.assertTrue(steal_utils.getOfferSize(offer) == tupExpected)
        
        


if __name__ == '__main__':
    unittest.main()
