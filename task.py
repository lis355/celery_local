from importlib import import_module

from celery_local.log import log

class Task:
    def __init__(self, schedule_task):
        method_path = schedule_task["task"]
        module_path, method_name = method_path.rsplit('.', 1)
        self.method_name = method_name

        module = import_module(module_path)
        method_task = getattr(module, method_name)
        self.method = method_task.run

        self.timedelta = schedule_task["schedule"]

        log("Got task (filtererd) {} wait time {}".format(method_path, self.timedelta))
