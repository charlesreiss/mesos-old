#!/usr/bin/env python

import mesos
import mesos_pb2
import os
import sys
import time
import httplib
import Queue
import threading

from argparse import ArgumentParser
from subprocess import *
from socket import gethostname

class ApacheWebFWScheduler(mesos.Scheduler):
  def __init__(self, min_servers, start_threshold, kill_threshold, 
               haproxy_exe, memory_required, cpu_required, monitor_server,
               server_port, apachectl_exe):
    self.min_servers = min_servers
    self.start_threshold = start_threshold
    self.kill_threshold = kill_threshold
    self.haproxy_exe = haproxy_exe
    self.apachectl_exe = apachectl_exe
    self.server_port = server_port
    self.cpu_required = cpu_required
    self.memory_required = memory_required
    self.resources = [
        mesos_pb2.Resource(
          name='cpus',
          type=mesos_pb2.Value.SCALAR,
          scalar=mesos_pb2.Value.Scalar(value=self.cpu_required)),
        mesos_pb2.Resource(
          name='mem',
          type=mesos_pb2.Value.SCALAR,
          scalar=mesos_pb2.Value.Scalar(value=self.memory_required))
      ]

    self.lock = threading.RLock()
    self.id = 0
    self.haproxy = -1
    self.reconfigs = 0
    self.servers = {}
    self.overloaded = False
    self.monitor_server = monitor_server

  def registered(self, driver, fid, masterInfo):
    print "Mesos haproxy+apache scheduler registered as framework #%s" % fid
    self.driver = driver

  def getFrameworkName(self, driver):
      return "haproxy+apache"

  def getExecutorInfo(self, driver):
    execPath = os.path.join(os.getcwd(), "startapache.sh")
    environment = mesos_pb2.Environment()
    apachectl_var = environment.variables.add()
    apachectl_var.name = 'APACHECTL'
    apachectl_var.value = self.apachectl_exe
    return mesos_pb2.ExecutorInfo(
        executor_id=mesos_pb2.ExecutorID(value='default'),
        command=mesos_pb2.CommandInfo(
          value=execPath,
          environment=environment)
        )


  def reconfigure(self):
    print "reconfiguring haproxy"
    name = "/tmp/haproxy.conf.%d" % self.reconfigs
    with open(name, 'w') as config:
      with open('haproxy.config.template', 'r') as template:
        for line in template:
          config.write(line)
      for id, host in self.servers.iteritems():
        config.write("       ")
        config.write("server %d %s:%d check\n" % (int(id), host, self.server_port))

    cmd = []
    if self.haproxy != -1:
      cmd = [self.haproxy_exe,
             "-f",
             name,
             "-sf",
             str(self.haproxy.pid)]
    else:
      cmd = [self.haproxy_exe,
             "-f",
             name]

    print "about to run ", cmd
    self.haproxy = Popen(cmd, shell = False)
    self.reconfigs += 1

  def resourceOffers(self, driver, offers):
    def getResource(offer, name):
      for resource in offer.resources:
        if resource.name == name:
          return resource.scalar.value
      return 0.0

    print "Got %d resource offers" % len(offers)
    self.lock.acquire()
    for offer in offers:
      tasks = []
      if offer.hostname in self.servers.values():
        print "Rejecting slot on host " + offer.hostname + " because we've launched a server on that machine already."
        #print "self.servers currently looks like: " + str(self.servers)
      elif not self.overloaded and len(self.servers) > 0:
        print "Rejecting slot because we've launched enough tasks."
      elif getResource(offer, 'mem') < args.memory_required:
        print ("Rejecting offer because it doesn't contain enough memory" +
          "(it has " + offer.params['mem'] + " and we need " +
          args.memory_required + ").")
      elif getResource(offer, 'cpus') < args.cpu_required:
        print "Rejecting offer because it doesn't contain enough CPUs."
      else:
        td = mesos_pb2.TaskInfo()
        td.name = 'webserver'
        td.task_id.value = str(self.id)
        td.slave_id.MergeFrom(offer.slave_id)
        td.executor.MergeFrom(self.getExecutorInfo(driver))
        cpus = td.resources.add()
        cpus.name = 'cpus'
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.cpu_required

        mem = td.resources.add()
        mem.name = 'mem'
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.memory_required

        tasks.append(td)
        self.servers[str(self.id)] = offer.hostname
        self.id += 1
        self.overloaded = False
      driver.launchTasks(offer.id, tasks)
    print "done with resourceOffer()"
    self.lock.release()

  def statusUpdate(self, driver, status):
    print "received status update from taskID " + str(status.task_id) + ", with state: " + str(status.state)
    reconfigured = False
    self.lock.acquire()
    if status.task_id.value in self.servers.keys():
      if status.state == mesos_pb2.TASK_STARTING:
        print "Task " + str(status.task_id) + " reported that it is STARTING."
        del self.servers[status.task_id.value]
        self.reconfigure()
        reconfigured = True
      elif status.state == mesos_pb2.TASK_RUNNING:
        print "Task " + str(status.task_id) + " reported that it is RUNNING, reconfiguring haproxy to include it in webfarm now."
        self.reconfigure()
        reconfigured = True
      elif status.state == mesos_pb2.TASK_FINISHED:
        del self.servers[status.task_id.value]
        print "Task " + str(status.task_id) + " reported FINISHED."
        self.reconfigure()
        reconfigured = True
      elif status.state == mesos_pb2.TASK_FAILED:
        print "Task " + str(status.task_id) + " reported that it FAILED!"
        del self.servers[status.task_id]
        self.reconfigure()
        reconfigured = True
      elif status.state == mesos_pb2.TASK_KILLED:
        print "Task " + str(status.task_id) + " reported that it was KILLED!"
        del self.servers[status.task_id.value]
        self.reconfigure()
        reconfigured = True
      elif status.state == mesos_pb2.TASK_LOST:
        print "Task " + str(status.task_id) + " reported was LOST!"
        del self.servers[status.task_id.value]
        self.reconfigure()
        reconfigured = True
      else:
        print "Task " + str(status.task_id) + " in unknown state " + str(status.state) + "!"
    self.lock.release()
    if reconfigured:
      driver.reviveOffers()
    print "done in statusupdate"

  def scaleUp(self):
    print "SCALING UP"
    self.lock.acquire()
    self.overloaded = True
    self.lock.release()

  def scaleDown(self, id):
    print "SCALING DOWN (removing server %d)" % id
    kill = False
    self.lock.acquire()
    if self.overloaded:
      self.overloaded = False
    else:
      kill = True
    self.lock.release()
    if kill:
      self.driver.killTask(id)
  
  def monitor(self):
    print "in MONITOR()"
    while True:
      time.sleep(1)
      print "done sleeping"
      try:
        conn = httplib.HTTPConnection(self.monitor_server)
        print "done creating connection"
        conn.request("GET", "/stats;csv")
        print "done with request()"
        res = conn.getresponse()
        print "testing response status"
        if (res.status != 200):
          print "response != 200"
          continue
        else:
          print "got some stats"
          data = res.read()
          lines = data.split('\n')[2:-2]

          total_queue = 0
          best_victim = None
          min_load = sys.maxint
          for line in lines:
            print 'stats line ', line
            fields = line.split(',')
            server_id = int(fields[1])
            sessions = int(fields[4]) # current number of sessions
            queue = int(fields[2]) # currently queued requests
            if sessions < min_load:
              min_load = sessions
              best_victim = server_id
            total_queue += sessions + queue
          
          queue_per_server = float(total_queue) / len(lines)
          print 'queue ', queue_per_server
          
          if queue_per_server >= self.start_threshold:
            self.scaleUp()
          elif queue_per_server <= self.kill_threshold:
            if len(lines) > self.min_servers:
              self.scaleDown(best_victim)

          conn.close()
      except Exception, e:
        print "exception in monitor(): ", e
        continue
    print "done in MONITOR()"



if __name__ == "__main__":
  parser = ArgumentParser(usage = "Usage: %prog mesos_master")
  parser.add_argument('--monitor_server', type=str)
  parser.add_argument('--master', type=str)
  parser.add_argument('--name', type=str, default='haproxy+apache')
  parser.add_argument('--start_threshold', type=float, default=25.0)
  parser.add_argument('--kill_threshold', type=float, default=5.0)
  parser.add_argument('--haproxy_exe', type=str,
                      default='/root/haproxy-1.3.20/haproxy')
  parser.add_argument('--min_servers', type=int, default=1)
  parser.add_argument('--apachectl_exe', type=str)
  parser.add_argument('--memory_required', type=float, default=1024.0)
  parser.add_argument('--cpu_required', type=float, default=1.0)
  parser.add_argument('--server_port', type=int, default=8888)

  args = parser.parse_args()

  print "sched = ApacheWebFWScheduler()"
  sched = ApacheWebFWScheduler(
      min_servers = args.min_servers,
      start_threshold = args.start_threshold,
      kill_threshold = args.kill_threshold,
      haproxy_exe = args.haproxy_exe,
      apachectl_exe = args.apachectl_exe,
      cpu_required = args.cpu_required,
      memory_required = args.memory_required,
      monitor_server = args.monitor_server,
      server_port = args.server_port)

  print "Connecting to mesos master %s" % args.master
  driver = mesos.MesosSchedulerDriver(sched, 
      mesos_pb2.FrameworkInfo(
        user='',
        name=args.name),
      args.master)


  threading.Thread(target = sched.monitor, args=[]).start()

  driver.run()

  print "Scheduler finished!"
