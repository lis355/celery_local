from threading import Thread, Lock
import time

from celery_local.task import Task
from celery_local.log import log


def run(celery_app, **kwargs):
    log("Starting Celery-Local")
    schedule = celery_app.conf.beat_schedule
    log("Got shedule with {} tasks".format(len(schedule)))

    tasks_filter = kwargs.get("filter", [])
    first_start = kwargs.get("first_start", True)

    tasks = []

    for task_name in schedule:
        if len(tasks_filter) == 0 or task_name in tasks_filter:
            tasks.append(Task(schedule[task_name]))

    mutex = Lock()

    def worker(local_task):
        if not first_start:
            log("Task {} wait first-execute, sleep for time {}".format(local_task.method_name, local_task.timedelta))
            time.sleep(local_task.timedelta.total_seconds())

        while True:
            log("Execute method for task {}".format(local_task.method_name))
            with mutex:
                local_task.method()
            log("Success for task {}, sleep for time {}".format(local_task.method_name, local_task.timedelta))
            time.sleep(local_task.timedelta.total_seconds())

    log("Start tasks")

    for task in tasks:
        thread = Thread(target=worker, args=(task,))
        thread.daemon = True
        thread.start()
        log("Start task {}".format(task.method_name))

