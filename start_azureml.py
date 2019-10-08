#!/usr/bin/env python

import os
import sys
import json
import time
import socket
import argparse
import itertools
import threading
import subprocess

from azureml.core import Workspace, Experiment, Environment
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.compute import AmlCompute, ComputeTarget
from azureml.data.data_reference import DataReference
from azureml.core.runconfig import RunConfiguration, MpiConfiguration
from azureml.core import ScriptRunConfig
from azureml.train.estimator import Estimator
from azureml.exceptions import ComputeTargetException
from azureml.widgets import RunDetails

from nyctaxi_data import download_nyctaxi_data
from nyctaxi_data import upload_nyctaxi_data

azure_gpu_vm_names = [
  "Standard_NC6s_v2",
  "Standard_NC12s_v2",
  "Standard_NC24s_v2",
  "Standard_NC24sr_v2",
  "Standard_NC6s_v3",
  "Standard_NC12s_v3",
  "Standard_NC24s_v3",
  "Standard_NC24sr_v3"
]

azure_gpu_vm_sizes = {
  "Standard_NC6s_v2"    : 1,
  "Standard_NC12s_v2"   : 2,
  "Standard_NC24s_v2"   : 4,
  "Standard_NC24sr_v2"  : 4,
  "Standard_NC6s_v3"    : 1,
  "Standard_NC12s_v3"   : 2,
  "Standard_NC24s_v3"   : 4,
  "Standard_NC24sr_v3"  : 4
}

def spinner():
  while True:
    for cursor in '-\\|/':
      sys.stdout.write(cursor)
      sys.stdout.flush()
      time.sleep(0.1)
      sys.stdout.write('\b')
    if done:
      return

if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument("--config", default="./config.json")
  parser.add_argument("--cluster_name", default="RAPIDS")
  parser.add_argument("--experiment_name", default="RAPIDS090")
  parser.add_argument("--vm_size", default=azure_gpu_vm_names[4])
  parser.add_argument("--node_count", default=1)
  parser.add_argument("--min_nodes", default=0)
  parser.add_argument("--max_nodes", default=10)
  parser.add_argument("--admin_username", default="rapids")
  parser.add_argument("--admin_user_password", default="rapids")
  parser.add_argument("--admin_user_ssh_key", default="~/.ssh/id_rsa.pub")
  parser.add_argument("--ssh_id", default="~/.ssh/id_rsa")

  parser.add_argument("--local_notebook_port", default="8888")
  parser.add_argument("--local_dashboard_port", default="8787")

  parser.add_argument("--nyctaxi_years", default="2015")
  parser.add_argument("--nyctaxi_src_path", default=os.getcwd())
  parser.add_argument("--nyctaxi_dst_path", default="data")

  args = parser.parse_args()

  if args.vm_size not in azure_gpu_vm_names:
    print("the specified vm size must be one of ...")
    for azure_gpu_vm_size in azure_gpu_vm_names:
      print("... " + azure_gpu_vm_size)
    raise Exception("{vm_size} does not support RAPIDS".format(vm_size=args.vm_size))

  with open(args.config, 'r') as f:
    config = json.loads(f.read())

  subscription_id = config["subscription_id"]
  resource_group = config["resource_group"]
  workspace_name = config["workspace_name"]

  workspace = Workspace(workspace_name=workspace_name,
                        subscription_id=subscription_id,
                        resource_group=resource_group)

  try:
    cluster = ComputeTarget(workspace=workspace,
                            name=args.cluster_name)
    print("Found pre-existing compute target")

  except ComputeTargetException:
    print("No pre-existing compute target found ...")
    print(" ... creating a new cluster ...")
    provisioning_config = AmlCompute.provisioning_configuration(vm_size=args.vm_size,
                                                                min_nodes=args.min_nodes,
                                                                max_nodes=args.max_nodes,
                                                                idle_seconds_before_scaledown=120,
                                                                admin_username=args.admin_username,
                                                                admin_user_password=args.admin_user_password,
                                                                admin_user_ssh_key=open(os.path.expanduser(args.admin_user_ssh_key)).read().strip())
    cluster = ComputeTarget.create(workspace, args.cluster_name, provisioning_config)
    print(" ... waiting for cluster ...")
    cluster.wait_for_completion(show_output=True)

  workspace = Workspace.from_config()
  datastore = workspace.get_default_datastore()

  if ','in args.nyctaxi_years:
    years = args.nyctaxi_years.split(',')
  elif ' ' in args.nyctaxi_years:
    years = args.nyctaxi_years.split(' ')
  else:
    years = [args.nyctaxi_years]

  download_nyctaxi_data(years, args.nyctaxi_src_path)
  upload_nyctaxi_data(workspace, datastore, os.path.join(args.nyctaxi_src_path, "nyctaxi"), os.path.join(args.nyctaxi_dst_path, "nyctaxi"))

  print("Configuring MPI ...")
  n_gpus_per_node = azure_gpu_vm_sizes[args.vm_size]
  mpi_config = MpiConfiguration()
  mpi_config.process_count_per_node = n_gpus_per_node

  print("Declaring estimator ...")
  estimator = Estimator(source_directory='./rapids',
                        compute_target=cluster,
                        entry_script='init_dask.py',
                        script_params={
                          '--datastore'        : workspace.get_default_datastore(),
                          '--n_gpus_per_node'  : str(n_gpus_per_node)
                        },
                        node_count=int(args.node_count),
                        distributed_training=mpi_config,
                        use_gpu=True,
                        conda_dependencies_file='rapids-0.9.yml')

  print("Starting experiment run ...")
  experiment = Experiment(workspace, args.experiment_name).submit(estimator)
  
  print(" ... waiting for headnode ...")
  print(" ... this may take several minutes ...")
  
  done = False
  spinning_thread = threading.Thread(target=spinner)
  spinning_thread.start()
  while not "headnode" in experiment.get_metrics():
    continue
  done = True
  spinning_thread.join()

  headnode = experiment.get_metrics()["headnode"]
  print(" ... headnode ready ...")
  print(" ... headnode has ip: ", headnode)

  print("Setting up port-forwarding for jupyter notebook environment ...")
  cmd = ("ssh -vvv -o StrictHostKeyChecking=no -N" + \
         " -i {ssh_key}" + \
         " -L 0.0.0.0:{notebook_port}:{headnode}:8888" + \
         " -L 0.0.0.0:{dashboard_port}:{headnode}:8787" + \
         " {uname}@{ip} -p {port}").format(ssh_key=os.path.expanduser(args.ssh_id),
                                           notebook_port=args.local_notebook_port,
                                           dashboard_port=args.local_dashboard_port,
                                           headnode=headnode,
                                           uname=args.admin_username,
                                           ip=cluster.list_nodes()[0]['ipAddress'],
                                           port=cluster.list_nodes()[0]['port'])
  
  print(" ... executing the following command ...")
  print(" ... ", cmd)
  
  portforward_out_log_name = "portforward_out_log.txt"
  print(" ... sending verbose port-fowarding output to {} ...".format(portforward_out_log_name))
  print(" ... to access the jupyter notebook environment, point your web-browser to {}:8888".format(socket.gethostbyname(socket.gethostname())))
  portforward_out_log = open("portforward_out_log.txt", 'w')
  portforward = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  while True:
    portforward_out = portforward.stdout.readline()
    if portforward_out == '' and portforward.poll() is not None:
      portforward_out_log.close()
      break
    elif portforward_out:
      portforward_out_log.write(portforward_out)
      portforward_out_log.flush()
