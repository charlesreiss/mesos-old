#!/usr/bin/env python

import sys
import mesos
import mesos_pb2
import sys
import time
import os
import atexit

from subprocess import *

APACHECTL = os.getenv('APACHECTL')

def cleanup():
  try:
    # TODO(*): This will kill ALL apaches...oops.
    os.waitpid(Popen(APACHECTL + " stop", shell=True).pid, 0)
  except Exception, e:
    print e
    None

class MyExecutor(mesos.Executor):
  def __init__(self):
    self.tid = -1

  def launchTask(self, driver, task):
    self.tid = task.task_id
    driver.sendStatusUpdate(
        mesos_pb2.TaskStatus(task_id = self.tid, state=mesos_pb2.TASK_RUNNING)
        )
    Popen(APACHECTL + " start", shell=True)

  def killTask(self, driver, tid):
    if (tid.value != self.tid.value):
      print "Expecting different task id ... killing anyway!"
    cleanup()
    update = mesos_pb2.TaskStatus(task_id = tid, state=mesos_pb2.TASK_FINISHED)
    driver.sendStatusUpdate(update)

  def shutdown(driver, self):
    cleanup()

  def error(self, code, message):
    print "Error: %s" % message

if __name__ == "__main__":
  print "Starting haproxy+apache executor"
  atexit.register(cleanup)
  executor = MyExecutor()
  mesos.MesosExecutorDriver(executor).run()
