#!/usr/bin/env python

import sys

sys.path.append(".")
import stub_mesos
sys.modules["mesos"] = stub_mesos

import itertools
import mesos
import mesos_pb2
import unittest

from mock import MagicMock
from task_stealing_scheduler import *

class TestChunkScheduler(unittest.TestCase):

    def setUp(self):
        self.counter = defaultdict(itertools.count)

        self.driver = MagicMock(spec=TaskStealingSchedulerDriver)
        self.scheduler = MagicMock(spec=mesos.Scheduler)
        self.stealingScheduler = TaskStealingScheduler(self.scheduler)

    def nextId(self, offer_type):
        return "{0}_id_{1}".format(offer_type, self.counter[offer_type].next())

    def generateOffer(self):
        offer = mesos_pb2.Offer()
        offer.id.value = self.nextId("offer")
        offer.framework_id.value = self.nextId("framework")
        offer.slave_id.value = self.nextId("slave")
        offer.hostname = self.nextId("local")
        return offer

    # Test modifies a global variable, breaking other tests.
    # TODO: Re-add this when fixed.
    def resourceOffers(self):
        offers = [self.generateOffer()]

        stealing = MagicMock()

        TaskStealingScheduler.resourceOffersStealing = stealing.first
        TaskChunkScheduler.resourceOffers = stealing.second

        self.stealingScheduler.resourceOffers(self.driver, offers)

        # Stealing offer is called first.
        TaskStealingScheduler.resourceOffersStealing.assert_called_once_with(
                self.stealingScheduler, self.driver, offers)
        # Then offers are updated.
        self.driver.updateOffers.assert_called_once_with(offers)
        # Underlying resourceOffers is called second.
        TaskChunkScheduler.resourceOffers.assert_called_once_with(
                self.stealingScheduler, self.driver, offers)

    def test_resourceOffersStealing(self):
        offers = [self.generateOffer()]
        tasks = "tasks"
        self.driver.pendingTasks = MagicMock()

        scheduler = self.stealingScheduler

        scheduler.selectTasksToSteal = MagicMock()
        scheduler.selectTasksToSteal.return_value = {offers[0].id.value: tasks}
        scheduler.stealSubTasks = MagicMock()

        scheduler.resourceOffersStealing(self.driver, offers)

        scheduler.selectTasksToSteal.assert_called_once()
        scheduler.stealSubTasks.assert_called_once_with(tasks)
        self.driver.launchTasks.assert_called_once_with(offers[0].id, tasks)

    def test_selectTasksToSteal(self):
        pass


if __name__ == '__main__':
    unittest.main()
