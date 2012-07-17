haproxy load balancer + Apache web server Mesos framework Readme
----------------------------------------------------------------

Installation instructions:
- Install Apache on all slaves. It must be in the same path.
- Install haproxy on the machine where the scheduler will run.
- If you want to use a non-standard port number, modify haproxy.config.template
  to specify it. Haproxy must be configured to serve stats from a request
  for /stats;csv

haproxy+apache is a wrapper script which runs the scheduler.

It locates its python dependencies using either:
- the PROTOBUF_EGG and MESOS_EGG environment variables (pointing to these
  egg files); or 
- the MESOS_BUILD_DIR environment variable (pointing to the MESOS_BUILD_DIR)

haproxy+apache takes many command-line arguments, see 'haproxy+apache --help'.

