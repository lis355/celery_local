# celery_local
Run celery tasks like your server same code and debug

### Using

In code, when your server is started, call ``run`` function:

```python
import celery_local

celery_local.run(app, filter=["my_task"], first_start=True)
```

``app`` - your Celery app in code (app = Celery(...))

``filter`` - array of task names, which will be executed. If empty (or None) - all tasks will be executed

``first_start`` - if True, tasks will be executed immediately
