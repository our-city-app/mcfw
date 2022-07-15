# -*- coding: utf-8 -*-
import base64
import logging
import os
import threading
import time
import types
from collections import defaultdict
from functools import wraps
from hashlib import sha256
from io import BytesIO

from google.appengine.api import memcache as mod_memcache
from google.appengine.ext import ndb

from fvfw.serialization import List, deserializer, ds_any, ds_bool, get_deserializer, get_list_serializer, \
    get_serializer, s_any, s_bool, serializer

CACHE_ATTR = u'cache_key'
CACHE_LOGGING = os.environ.get('SERVER_SOFTWARE', 'Development').startswith('Development')


class _TLocal(threading.local):

    def __init__(self):
        self.request_cache = dict()


_tlocal = _TLocal()
del _TLocal


def flush_request_cache():
    _tlocal.request_cache.clear()


def set_cache_key(wrapped, f):
    def key():
        return f.meta[CACHE_ATTR] if hasattr(f, 'meta') and CACHE_ATTR in f.meta else '%s.%s' % (
            f.__name__, f.__module__)

    if not hasattr(wrapped, 'meta'):
        wrapped.meta = {CACHE_ATTR: key()}
        return
    if CACHE_ATTR not in wrapped.meta:
        wrapped.meta[CACHE_ATTR] = key()


def ds_key(version, cache_key):
    return f'{version}-{ sha256(cache_key).hexdigest()}'


class DSCache(ndb.Model):
    creation_timestamp = ndb.IntegerProperty()
    description = ndb.StringProperty(indexed=False)
    value = ndb.BlobProperty()

    @classmethod
    def create_key(cls, hash_):
        return ndb.Key(cls, hash_)


def invalidate_cache(f, *args, **kwargs):
    f.invalidate_cache(*args, **kwargs)


cache_key_locks = defaultdict(lambda: threading.RLock())


def cached(version, lifetime=600, request=True, memcache=True, key=None, datastore=None):
    """
    Caches the result of the decorated function and returns the cached version if it exists.

    @param version: Cache version, needs to bumped everytime the semantics
    @type version: integer
    @param lifetime: Number of seconds the cached entry remains in memcache after it was created.
    @type lifetime: int
    @param request: Whether it needs to be cached in memory for the current request processing.
    @type request: bool
    @param memcache: Whether it needs to be cached in memcache.
    @type memcache: bool
    @param key: Function to create cache_key
    @param key: function
    @param datastore: Content description of cache object in datastore. Leave none to ommit the datastore cache.
    @param datastore: str
    @raise ValueError: if neither request nor memcache are True
    """

    if not request and not memcache and not datastore:
        raise ValueError("Either request or memcache or datastore needs to be True")

    if datastore and lifetime != 0:
        raise ValueError("If datastore caching is used, values other than 0 for lifetime are not permitted.")

    def wrap(f):
        base_cache_key = f.meta[CACHE_ATTR]
        f_args = f.meta["fargs"]
        f_ret = f.meta["return_type"]
        f_pure_default_args_dict = f.meta["pure_default_args_dict"]

        if isinstance(f_ret, list):
            f_ret = List(f_ret[0])
        if memcache or datastore:
            result_serializer = get_serializer(f_ret)
            result_deserializer = get_deserializer(f_ret)
        key_function = key
        if not key_function:
            def key_(kwargs):
                stream = BytesIO()
                stream.write(base_cache_key.encode('utf8'))
                kwargt = f.meta["kwarg_types"]
                for a in sorted(kwargt.keys()):
                    if a in kwargs:
                        effective_value = kwargs[a]
                    else:
                        effective_value = f_pure_default_args_dict[a]
                    if isinstance(kwargt[a], list):
                        get_list_serializer(get_serializer(kwargt[a][0]))(stream, effective_value)
                    else:
                        get_serializer(kwargt[a])(stream, effective_value)
                return stream.getvalue()

            key_function = key_

        @serializer
        def serialize_result(stream, obj):
            s_bool(stream, obj[0])
            if obj[0]:
                result_serializer(stream, obj[1])
            else:
                s_any(stream, obj[1])

        f.serializer = serialize_result

        @deserializer
        def deserialize_result(stream):
            success = ds_bool(stream)
            if success:
                result = result_deserializer(stream)
            else:
                result = ds_any(stream)
            return success, result

        f.deserializer = deserialize_result

        def cache_key(*args, **kwargs):
            kwargs_ = dict(kwargs)
            kwargs_.update({f_args[0][i]: args[i] for i in range(len(args))})
            return f'v{version}.{base64.b64encode(key_function(kwargs_))}'

        f.cache_key = cache_key

        def invalidate_cache(*args, **kwargs):
            ck = cache_key(*args, **kwargs)
            with cache_key_locks[ck]:
                if datastore:
                    @ndb.non_transactional()
                    def clear_dscache():
                        DSCache.create_key(ds_key(version, ck)).delete()

                    clear_dscache()
                if memcache:
                    attempt = 1
                    while not mod_memcache.delete(ck):  # @UndefinedVariable
                        if attempt >= 3:
                            logging.critical("MEMCACHE FAILURE !!! COULD NOT INVALIDATE CACHE !!!")
                            raise RuntimeError("Could not invalidate memcache!")
                        logging.debug("Memcache failure. Retrying to invalidate cache.")
                        time.sleep(0.25 * attempt)
                        attempt += 1

                if request and ck in _tlocal.request_cache:
                    del _tlocal.request_cache[ck]

        def update_cache(*args, **kwargs):
            # update request cache only
            if not request:
                return

            if '_data' not in kwargs:
                raise ValueError('update_cache() takes a mandatory _data argument')

            data = kwargs.pop('_data')
            ck = cache_key(*args, **kwargs)
            with cache_key_locks[ck]:
                _tlocal.request_cache[ck] = (True, data)

        f.invalidate_cache = invalidate_cache
        f.update_cache = update_cache

        @wraps(f)
        def wrapped(*args, **kwargs):
            ck = cache_key(*args, **kwargs)
            log_ck = ck if len(ck) < 100 else '%s...(length=%d)' % (ck[:100], len(ck))
            ck = cache_key(*args, **kwargs)
            with cache_key_locks[ck]:
                if request and ck in _tlocal.request_cache:
                    success, result = _tlocal.request_cache[ck]
                    if success:
                        _log('Hit(request): %s', f.__name__)
                        return result
                if memcache:
                    memcache_result = mod_memcache.get(ck)  # @UndefinedVariable
                    if memcache_result:
                        buf = BytesIO(memcache_result)
                        success, result = deserialize_result(buf)
                        if request:
                            _tlocal.request_cache[ck] = (success, result)
                        if success:
                            _log('Hit(memcache): %s', f.__name__)
                            return result
                if datastore:
                    @ndb.non_transactional()
                    def get_from_dscache():
                        dscache = DSCache.get_by_id(ds_key(version, ck))
                        if dscache:
                            buf = BytesIO(dscache.value)
                            success, result = deserialize_result(buf)
                            if request:
                                _tlocal.request_cache[ck] = (success, result)
                            if memcache:
                                mod_memcache.set(ck, dscache.value, time=lifetime)  # @UndefinedVariable
                            if success:
                                _log('Hit(ds): %s', f.__name__)
                                return True, result
                        return False, None

                    cached, result = get_from_dscache()
                    if cached:
                        return result

                cache_value = None
                try:
                    result = f(*args, **kwargs)
                    if isinstance(result, types.GeneratorType):
                        result = list(result)
                    cache_value = (True, result)
                    return result
                except Exception as e:
                    cache_value = (False, e)
                    raise
                finally:
                    if cache_value and cache_value[0]:
                        # Only store in request cache in case we're inside a transaction to avoid stale results
                        if not ndb.in_transaction():
                            if datastore or memcache:
                                buf = BytesIO()
                                serialize_result(buf, cache_value)
                                serialized_cache_value = buf.getvalue()
                            if datastore:
                                _log('Saving(ds): %s, key %s', f.__name__, log_ck)

                                @ndb.non_transactional()
                                def update_dscache():
                                    dsm = DSCache(id=ds_key(version, ck))
                                    dsm.description = datastore
                                    dsm.creation_timestamp = int(time.time())
                                    dsm.value = serialized_cache_value
                                    dsm.put()

                                update_dscache()
                            if memcache:
                                _log('Saving(memcache): %s, key %s', f.__name__, log_ck)
                                mod_memcache.set(ck, serialized_cache_value, time=lifetime)  # @UndefinedVariable
                        if request:
                            _log('Saving(request): %s, key %s', f.__name__, log_ck)
                            _tlocal.request_cache[ck] = cache_value

        return wrapped

    return wrap


def _log(msg, *args):
    if CACHE_LOGGING:
        logging.debug('[Cache] %s', msg % args)
