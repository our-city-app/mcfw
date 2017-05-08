# -*- coding: utf-8 -*-
# Copyright 2017 GIG Technology NV
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
# @@license_version:1.3@@

import httplib
import inspect
import json
import logging
import threading
import webapp2
from collections import defaultdict
from types import NoneType

from consts import DEBUG, AUTHENTICATED, NOT_AUTHENTICATED
from mcfw.exceptions import HttpException, HttpBadRequestException
from mcfw.properties import simple_types
from mcfw.rpc import run, ErrorResponse, serialize_complex_value, MissingArgumentException, parse_complex_value

DEFAULT_API_VERSION = 'v1.0'

_rest_handlers = defaultdict(dict)
_precall_hooks = []
_postcall_hooks = []


class BadRequestResponse(Exception):
    pass


class InjectedFunctions(object):
    def __init__(self):
        self._get_session_function = None
        self._api_url_template = None

    @property
    def get_current_session(self):
        return self._get_session_function

    @get_current_session.setter
    def get_current_session(self, function):
        self._get_session_function = function

    @property
    def get_api_url_template(self):
        return self._api_url_template

    @get_api_url_template.setter
    def get_api_url_template(self, function):
        self._api_url_template = function


INJECTED_FUNCTIONS = InjectedFunctions()
del InjectedFunctions


def register_precall_hook(callable_):
    _precall_hooks.append(callable_)


def register_postcall_hook(callable_):
    _postcall_hooks.append(callable_)


def rest(uri, method='get', scopes=None, version=DEFAULT_API_VERSION, uri_prefix=None, silent=False,
         silent_result=False):
    if method not in ('get', 'post', 'put', 'delete'):
        ValueError('method')
    if scopes is None:
        scopes = []
    if isinstance(scopes, str):
        scopes = [scopes]

    def wrap(f):
        if not inspect.isfunction(f):
            raise ValueError('%s is not of type function!' % f)

        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        wrapped.__name__ = f.__name__

        api_url = INJECTED_FUNCTIONS.get_api_url_template(version, uri)
        if uri_prefix:
            api_url = "/%s%s" % (uri_prefix, api_url)

        wrapped.meta = {
            'rest': True,
            'uri': api_url,
            'scopes': scopes,
            'method': method,
            "silent": silent,
            "silent_result": silent_result
        }
        if hasattr(f, 'meta'):
            wrapped.meta.update(f.meta)
        return wrapped

    return wrap


class ResponseTracker(threading.local):
    def __init__(self):
        self.current_response = None
        self.current_request = None


_current_reponse_tracker = ResponseTracker()
del ResponseTracker


class GenericRESTRequestHandler(webapp2.RequestHandler):
    @staticmethod
    def getCurrentResponse():
        return _current_reponse_tracker.current_response

    @staticmethod
    def getCurrentRequest():
        return _current_reponse_tracker.current_request

    @staticmethod
    def clearCurrent():
        _current_reponse_tracker.current_response = None

    @staticmethod
    def setCurrent(request, response):
        _current_reponse_tracker.current_request = request
        _current_reponse_tracker.current_response = response

    def ctype(self, type_, value):
        if not isinstance(type_, (list, tuple)):
            if type_ == bool:
                return bool(value) and value.lower() == 'true'
            return type_(value)
        elif isinstance(type_, list):
            return [self.ctype(type_[0], item) for item in value.split(',')]
        elif type_ == (str, unicode):
            return unicode(value)
        elif type_ == (int, long):
            return long(value)
        elif type_ == (int, long, NoneType):
            return None if value is None or value == '' else long(value)
        raise NotImplementedError()

    def get_handler(self, method, route):
        """
        Returns the handler associated with the requested URL
         Returns None if no handler was found for this url or when the method is not implemented/allowed
        Args:
            method (string)
            route (webapp2.Route)
        Returns:
            function(callable)
        """
        if route.template in _rest_handlers:
            if method in _rest_handlers[route.template]:
                return _rest_handlers[route.template][method]
            else:
                self.response.set_status(httplib.METHOD_NOT_ALLOWED, httplib.responses[httplib.METHOD_NOT_ALLOWED])
        self.response.set_status(httplib.NOT_FOUND, httplib.responses[httplib.NOT_FOUND])

    def update_kwargs(self, function, kwargs):
        for name, type_ in function.meta['kwarg_types'].iteritems():
            if name in self.request.GET:
                kwargs[name] = self.ctype(type_, self.request.GET[name])

    def get(self, *args, **kwargs):
        GenericRESTRequestHandler.setCurrent(self.request, self.response)
        f = self.get_handler('get', self.request.route)
        if not f:
            return
        self.update_kwargs(f, kwargs)
        self.write_result(self.run(f, args, kwargs))

    def post(self, *args, **kwargs):
        GenericRESTRequestHandler.setCurrent(self.request, self.response)
        f = self.get_handler('post', self.request.route)
        if not f:
            return
        self.update_kwargs(f, kwargs)
        post_data_type = f.meta['kwarg_types'].get('data')
        if post_data_type:
            is_list = type(post_data_type) is list
            if is_list:
                post_data_type = post_data_type[0]
            post_data = json.loads(self.request.body) if self.request.body else {}
            kwargs['data'] = parse_complex_value(post_data_type, post_data, is_list)
        self.write_result(self.run(f, args, kwargs))

    def put(self, *args, **kwargs):
        GenericRESTRequestHandler.setCurrent(self.request, self.response)
        f = self.get_handler('put', self.request.route)
        if not f:
            return
        self.update_kwargs(f, kwargs)
        post_data_type = f.meta['kwarg_types'].get('data')
        if post_data_type:
            is_list = type(post_data_type) is list
            if is_list:
                post_data_type = post_data_type[0]
            post_data = json.loads(self.request.body) if self.request.body else {}
            kwargs['data'] = parse_complex_value(post_data_type, post_data, is_list)
        self.write_result(self.run(f, args, kwargs))

    def delete(self, *args, **kwargs):
        GenericRESTRequestHandler.setCurrent(self.request, self.response)
        f = self.get_handler('delete', self.request.route)
        if not f:
            return
        self.update_kwargs(f, kwargs)
        self.write_result(self.run(f, args, kwargs))

    def write_result(self, result):
        self.response.headers.update({
            'Content-Type': 'application/json'
        })
        if result is not None:
            if type(result) == ErrorResponse:
                self.response.set_status(result.status_code)
                result = serialize_complex_value(result, ErrorResponse, False)
            if DEBUG:
                self.response.out.write(json.dumps(result, indent=2, sort_keys=True))
            else:
                self.response.out.write(json.dumps(result))
        else:
            self.response.set_status(httplib.NO_CONTENT)

    def run(self, f, args, kwargs):
        """
        Args:
            f (any)
            args (tuple)
            kwargs (dict)
        Returns: callable
        """
        if f.meta['authentication'] == AUTHENTICATED:
            session = INJECTED_FUNCTIONS.get_current_session()
            if not session:
                self.abort(httplib.UNAUTHORIZED)

            if f.meta['scopes']:
                scopes = set()
                simple_kwargs = {}
                for kwarg in kwargs:
                    if type(kwargs[kwarg]) in simple_types:
                        simple_kwargs[kwarg] = kwargs[kwarg]

                for scope in f.meta['scopes']:
                    scopes.add(scope.format(**simple_kwargs))

                if not any(scope in scopes for scope in session.scopes):
                    self.abort(httplib.FORBIDDEN)

        for hook in _precall_hooks:
            hook(f, *args, **kwargs)
        try:
            result = run(f, args, kwargs)
        except HttpException as http_exception:
            return ErrorResponse(http_exception)
        except MissingArgumentException as e:
            logging.debug(e)
            return ErrorResponse(HttpBadRequestException(e.message))
        except Exception as e:
            for hook in _postcall_hooks:
                hook(f, False, kwargs, e)
            raise
        for hook in _postcall_hooks:
            hook(f, True, kwargs, result)
        return result


def rest_functions(module, authentication=AUTHENTICATED):
    if authentication not in (AUTHENTICATED, NOT_AUTHENTICATED):
        raise ValueError(authentication)
    for f in (function for (name, function) in inspect.getmembers(module, lambda x: inspect.isfunction(x))):
        if hasattr(f, 'meta') and f.meta.get('rest', False):
            meta_uri = f.meta['uri']
            meta_method = f.meta['method']
            f.meta['authentication'] = authentication
            for uri in (meta_uri if isinstance(meta_uri, (list, tuple)) else (meta_uri,)):
                _rest_handlers[uri][meta_method] = f
                yield uri, GenericRESTRequestHandler
