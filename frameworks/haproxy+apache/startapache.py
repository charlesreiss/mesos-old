#!/usr/bin/env python

import sys
import mesos
import mesos_pb2
import os
import pickle
import subprocess
import time

from subprocess import *

HTTPD = os.getenv('HTTPD')
HTTPD_ARGS = map(str, pickle.loads(os.getenv('HTTPD_ARGS')))

def verboseCall(args):
  print "About to call ", args
  subprocess.check_call(args)

class MyExecutor(mesos.Executor):
  def __init__(self):
    self.tid = None

  def argsForPort(self, port):
    return HTTPD_ARGS + [
      '-C', 'Define PORT ' + str(port),
      '-c', 'Listen ' + str(port)
    ]

  def launchTask(self, driver, task):
    self.tid = task.task_id
    port = int(task.data)
    driver.sendStatusUpdate(
        mesos_pb2.TaskStatus(task_id = self.tid, state=mesos_pb2.TASK_RUNNING)
        )
    verboseCall([HTTPD, '-k', 'start'] + self.argsForPort(port))

  def killTask(self, driver, tid):
    subprocess.check_call([HTTPD, '-k', 'graceful-stop'] + HTTPD_ARGS)
    update = mesos_pb2.TaskStatus(task_id = self.tid, state=mesos_pb2.TASK_FINISHED)
    driver.sendStatusUpdate(update)

  def shutdown(self, driver):
    if self.tid is not None:
      subprocess.check_call([HTTPD, '-k', 'graceful-stop'] + HTTPD_ARGS)
      update = mesos_pb2.TaskStatus(task_id = self.tid, state=mesos_pb2.TASK_FINISHED)
      driver.sendStatusUpdate(update)

  def error(self, code, message):
    print "Error: %s" % message

if __name__ == "__main__":
  print "Starting haproxy+apache executor"
  executor = MyExecutor()
  mesos.MesosExecutorDriver(executor).run()
