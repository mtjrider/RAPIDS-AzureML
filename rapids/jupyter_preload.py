from notebook.notebookapp import NotebookApp
from azureml.core import Run
import socket

def dask_setup(scheduler):
  app = NotebookApp()
  ip = socket.gethostbyname(socket.gethostname())
  app.ip = "0.0.0.0"
  Run.get_context().log("jupyter",
                        "ip: {ip_addr}, port: {port}".format(ip_addr=ip, port=app.port))
  Run.get_context().log("jupyter-token", app.token)
  app.initialize([])
