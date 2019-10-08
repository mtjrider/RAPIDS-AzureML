import sys
import argparse
import subprocess
import socket

from mpi4py import MPI
from azureml.core import Run

if __name__ == '__main__':
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()

  ip = socket.gethostbyname(socket.gethostname())
  print("- my rank is ", rank)
  print("- my ip is ", ip)

  parser = argparse.ArgumentParser()
  parser.add_argument("--datastore")
  parser.add_argument("--n_gpus_per_node")
  
  args = parser.parse_args()

  if rank == 0:
    cluster = {
      "scheduler"  : ip + ":8786",
      "dashboard"  : ip + ":8787"
    }
    Run.get_context().log("headnode", ip)
    Run.get_context().log("cluster",
                          "scheduler: {scheduler}, dashbaord: {dashboard}".format(scheduler=cluster["scheduler"],
                                                                                  dashboard=cluster["dashboard"]))
    Run.get_context().log("datastore", args.datastore)
  else:
    cluster = None

  cluster = comm.bcast(cluster, root=0)
  scheduler = cluster["scheduler"]
  dashboard = cluster["dashboard"]
  print("- scheduler is ", scheduler)
  print("- dashboard is ", dashboard)

  if rank == 0:
    cmd = "dask-scheduler " + "--port " + scheduler.split(":")[1] + " --preload jupyter_preload.py " + " --dashboard-address " + dashboard
    scheduler_out_log = open("scheduler_out_log.txt", 'w')
    scheduler_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    os.environ["CUDA_VISIBLE_DEVICES"] = str(rank % int(args.n_gpus_per_node))
    cmd = "dask-cuda-worker " + scheduler + " --memory-limit 0"
    worker_out_log = open("worker_out_log.txt", 'w')
    worker_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=worker_out_log, stderr=subprocess.STDOUT)

    while True:
      scheduler_out = scheduler_proc.stdout.readline()
      if scheduler_out == '' and scheduler_proc.poll() is not None:
        scheduler_out_log.close()
        break
      elif scheduler_out:
        sys.stdout.write(scheduler_out)
        scheduler_out_log.write(scheduler_out)
        scheduler_out_log.flush()
  else:
    os.environ["CUDA_VISIBLE_DEVICES"] = str(rank % int(args.n_gpus_per_node))
    cmd = "dask-cuda-worker " + scheduler + " --memory-limit 0"
    worker_proc = subprocess.Popen(cmd.split(), universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    worker_out_log = open("worker_out_log.txt", 'w')
    while True:
      worker_out = worker_proc.stdout.readline()
      if worker_out == '' and worker_proc.poll() is not None:
        worker_out_log.close()
        break
      elif worker_out:
        sys.stdout.write(worker_out)
        worker_out_log.write(worker_out)
        worker_out_log.flush()
