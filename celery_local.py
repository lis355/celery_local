from threading import Thread, Lock
import time
from importlib import import_module


class CeleryLocal:
    @staticmethod
    def log(message):
        print("[Celery-Local] {}".format(message))

    class Task:
        def __init__(self, schedule_task):
            method_path = schedule_task["task"]
            module_path, method_name = method_path.rsplit('.', 1)
            self.method_name = method_name

            module = import_module(module_path)
            method_task = getattr(module, method_name)
            self.method = method_task.run

            self.timedelta = schedule_task["schedule"]

            CeleryLocal.log("Got task (filtererd) {} wait time {}".format(method_path, self.timedelta))

    def __init__(self, celery_app, **kwargs):
        CeleryLocal.log("Starting Celery-Local")
        schedule = celery_app.conf.beat_schedule
        CeleryLocal.log("Got shedule with {} tasks".format(len(schedule)))

        tasks_filter = kwargs.get("filter", [])
        first_start = kwargs.get("first_start", True)

        tasks = []

        for task_name in schedule:
            if len(tasks_filter) == 0 or task_name in tasks_filter:
                tasks.append(self.Task(schedule[task_name]))

        mutex = Lock()

        def worker(local_task):
            if not first_start:
                CeleryLocal.log("Task {} wait first-execute, sleep for time {}".format(local_task.method_name, local_task.timedelta))
                time.sleep(local_task.timedelta.total_seconds())

            while True:
                CeleryLocal.log("Execute method for task {}".format(local_task.method_name))
                with mutex:
                    local_task.method()
                CeleryLocal.log("Success for task {}, sleep for time {}".format(local_task.method_name, local_task.timedelta))
                time.sleep(local_task.timedelta.total_seconds())

        CeleryLocal.log("Start tasks")

        for task in tasks:
            thread = Thread(target=worker, args=(task,))
            thread.daemon = True
            thread.start()
            CeleryLocal.log("Start task {}".format(task.method_name))
