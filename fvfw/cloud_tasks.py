# -*- coding: utf-8 -*-
# Copyright 2020 Green Valley Belgium NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# @@license_version:1.7@@

import logging
import pickle
from base64 import b64decode, b64encode
from os import environ
from typing import Callable

from flask import abort, request
from google.appengine.api.taskqueue import MAX_TASKS_PER_ADD, Queue, Task, taskqueue
from google.appengine.ext import ndb
from google.appengine.ext.deferred import deferred

from fvfw.blueprint import fvfw_blueprint
from fvfw.consts import DEBUG
from fvfw.models import NdbModel
from fvfw.utils import chunks


def create_task_url(func: Callable) -> str:
    return f'/_tasks/{func.__module__}.{func.__name__}'


class PermanentTaskFailure(Exception):
    """Indicates that a task failed, and will never succeed."""


class SingularTaskFailure(Exception):
    """Indicates that a task failed once."""


class DeferTaskEntity(NdbModel):
    data = ndb.BlobProperty(required=True)


def _get_request_header(header_name):
    try:
        return request.headers.get(header_name)
    except RuntimeError:
        logging.warning(f'Failed to get header {header_name}', exc_info=1)


def get_current_queue():
    return _get_request_header('X-Appengine-Queuename')


def get_current_task_name():
    return _get_request_header('X-Appengine-TaskName')


def get_current_version():
    if DEBUG:
        return environ.get('SERVER_PORT')
    return environ.get('GAE_VERSION')


def run(data):
    if DEBUG:
        data = b64decode(data)
    try:
        func, args, kwargs = pickle.loads(data)
    except Exception as e:
        raise PermanentTaskFailure(e)
    else:
        try:
            if DEBUG:
                prefix = f'{get_current_version()} -> '
            else:
                prefix = ''
            logging.debug('%sQueue: %s\ndeferred.run(%s.%s%s%s)',
                          prefix,
                          get_current_queue(),
                          func.__module__, func.__name__,
                          ''.join(f',\n             {repr(a)}' for a in args),
                          ''.join(f',\n             {k}={repr(v)}' for k, v in kwargs.items()))
        except:
            logging.exception('Failed to log the info of this defer (%s)', func)
        return func(*args, **kwargs)


def run_from_datastore(key):
    # string key
    entity = key.get()  # type: DeferTaskEntity
    if not entity:
        raise PermanentTaskFailure()
    try:
        run(entity.data)
        key.delete()
    except PermanentTaskFailure:
        key.delete()
        raise


@fvfw_blueprint.route('/_tasks/<path:task_name>', methods=['POST'])
@fvfw_blueprint.route('/_ah/queue/deferred', methods=['POST'])
def task_handler(tag=None):
    def run_from_request():
        """Default behavior for POST requests to deferred handler."""
        logging.debug(', '.join(f'{k}:{v}' for k, v in request.headers.items() if k.lower().startswith('x-appengine-')))

        run(request.get_data(parse_form_data=False))

    try:
        run_from_request()
    except SingularTaskFailure:
        logging.debug('Failure executing task, task retry forced')
        return abort(408)
    except PermanentTaskFailure:
        logging.exception('Permanent failure attempting to execute task')


def serialize_task(func, *args, **kwargs):
    payload = deferred.serialize(func, *args, **kwargs)
    if DEBUG:
        return b64encode(payload)
    return payload


def create_task(func: Callable, *args: object, **kwargs: object) -> taskqueue.Task:
    taskargs = {x: kwargs.pop(f"_{x}", None) for x in ('countdown', 'eta', 'name', 'target', 'retry_options')}

    taskargs['url'] = kwargs.pop('_url', deferred._DEFAULT_URL)
    if taskargs['url'] == deferred._DEFAULT_URL:
        taskargs['url'] = create_task_url(func)

    taskargs['headers'] = dict(deferred._TASKQUEUE_HEADERS)
    taskargs['headers'].update(kwargs.pop('_headers', {}))

    payload = serialize_task(func, *args, **kwargs)
    try:
        return taskqueue.Task(payload=payload, **taskargs)
    except taskqueue.TaskTooLargeError:
        key = DeferTaskEntity(data=payload).put()
        payload = serialize_task(run_from_datastore, key)
        return taskqueue.Task(payload=payload, **taskargs)


def schedule_tasks(tasks: list[Task],
                   queue_name: str = deferred._DEFAULT_QUEUE,
                   transactional: bool = False) -> list[Task]:
    queue = Queue(queue_name)
    results = []
    for chunk in chunks(tasks, MAX_TASKS_PER_ADD):
        results.extend(queue.add(chunk, transactional=transactional))
    return results


def defer(func, *args, **kwargs):
    queue_name = kwargs.pop('_queue', deferred._DEFAULT_QUEUE)
    transactional = kwargs.pop('_transactional', False)
    task = create_task(func, *args, **kwargs)
    return schedule_tasks([task], queue_name=queue_name, transactional=transactional)[0]


def try_or_defer(func, *args, **kwargs):
    try:
        kw = {k: v for k, v in kwargs.items() if not k.startswith('_')}
        func(*args, **kw)
    except Exception as e:
        if isinstance(e, PermanentTaskFailure):
            logging.info('PermanentTaskFailure', exc_info=True)
            raise e
        else:
            logging.exception(e)
        defer(func, *args, **kwargs)
        logging.exception(f'Failed to execute {func}, deferring ....')
