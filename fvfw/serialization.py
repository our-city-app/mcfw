# -*- coding: utf-8 -*-
import datetime
import json
import pickle
import time
import types
from functools import wraps
from io import BytesIO
from struct import Struct

from google.appengine.api import users
from google.appengine.ext import ndb

_serializers = {}
_ushortStruct = Struct('<H')
_intStruct = Struct('<i')
_longStruct = Struct('<q')
_longLongStruct = Struct('<q')
_doubleStruct = Struct('<d')


def register(type_, serializer, deserializer):
    _serializers[type_] = (serializer, deserializer)


def serialize(type_, obj):
    stream = BytesIO()
    _serializers[type_][0](stream, obj)
    return stream.getvalue()


def deserialize(type_, stream):
    if isinstance(stream, (str, bytes)):
        stream = BytesIO(stream)
    return _serializers[type_][1](stream)


def get_serializer(type_):
    return _serializers[type_][0]


def get_deserializer(type_):
    return _serializers[type_][1]


def serializer(f):
    @wraps(f)
    def wrapped(stream, obj, *args, **kwargs):
        if obj is None:
            stream.write('0'.encode('utf8'))
        else:
            stream.write('1'.encode('utf8'))
            f(stream, obj, *args, **kwargs)

    return wrapped


def deserializer(f):
    @wraps(f)
    def wrapped(stream, *args, **kwargs):
        if stream.read(1).decode('utf8') == '0':
            return None
        else:
            return f(stream, *args, **kwargs)

    return wrapped


@serializer
def s_str(stream, obj):
    obj_bytes = obj.encode('utf8')
    stream.write(_intStruct.pack(len(obj_bytes)))
    stream.write(obj_bytes)


@deserializer
def ds_str(stream):
    (size,) = _intStruct.unpack(stream.read(_intStruct.size))
    return stream.read(size).decode('utf8')


register(str, s_str, ds_str)


@serializer
def s_bytes(stream, obj):
    stream.write(_intStruct.pack(len(obj)))
    stream.write(obj)


@deserializer
def ds_bytes(stream):
    (size,) = _intStruct.unpack(stream.read(_intStruct.size))
    return stream.read(size)


register(bytes, s_bytes, ds_bytes)


@serializer
def s_bool(stream, obj):
    v = '1' if obj else '0'
    stream.write(v.encode('utf8'))


@deserializer
def ds_bool(stream):
    v = stream.read(1)
    return v.decode('utf8') == '1'


register(bool, s_bool, ds_bool)


@serializer
def s_ushort(stream, obj):
    stream.write(_ushortStruct.pack(obj))


@deserializer
def ds_ushort(stream):
    (value,) = _ushortStruct.unpack(stream.read(_ushortStruct.size))
    return value


@serializer
def s_long(stream, obj):
    stream.write(_longStruct.pack(obj))


@deserializer
def ds_long(stream):
    (value,) = _longStruct.unpack(stream.read(_longStruct.size))
    return value


register(int, s_long, ds_long)


@serializer
def s_long_long(stream, obj):
    stream.write(_longLongStruct.pack(obj))


@deserializer
def ds_long_long(stream):
    (value,) = _longLongStruct.unpack(stream.read(_longLongStruct.size))
    return value


register(int, s_long_long, ds_long_long)


@serializer
def s_float(stream, obj):
    stream.write(_doubleStruct.pack(obj))


@deserializer
def ds_float(stream):
    (value,) = _doubleStruct.unpack(stream.read(_doubleStruct.size))
    return value


register(float, s_float, ds_float)


@serializer
def s_dict(stream, obj):
    s_str(stream, json.dumps(obj))


@deserializer
def ds_dict(stream):
    return json.loads(ds_str(stream))


register(dict, s_dict, ds_dict)


@serializer
def s_datetime(stream, obj):
    s_long(stream, int(time.mktime(obj.timetuple())))


@deserializer
def ds_datetime(stream):
    return datetime.datetime.fromtimestamp(ds_long(stream))


register(datetime.datetime, s_datetime, ds_datetime)


@serializer
def s_key(stream, obj):
    s_str(stream, obj.urlsafe().decode('utf8'))


@deserializer
def ds_key(stream):
    return ndb.Key(urlsafe=ds_str(stream))


if 'ndb' in locals():
    register(ndb.Key, s_key, ds_key)


@serializer
def s_any(stream, obj):
    pickle.dump(obj, stream, protocol=pickle.HIGHEST_PROTOCOL)


@deserializer
def ds_any(stream):
    return pickle.load(stream)


@serializer
def s_user(stream, obj):
    s_str(stream, obj.email())


@deserializer
def ds_user(stream):
    return users.User(ds_str(stream))


def get_list_serializer(func):
    @serializer
    def s_list(stream, obj):
        if isinstance(obj, types.GeneratorType):
            obj = list(obj)
        stream.write(_intStruct.pack(len(obj)))
        for o in obj:
            func(stream, o)

    return s_list


def get_list_deserializer(func):
    @deserializer
    def ds_list(stream):
        (size,) = _intStruct.unpack(stream.read(_intStruct.size))
        return [func(stream) for _ in range(size)]

    return ds_list


class List(object):
    def __init__(self, type_):
        self.type = type_

    def __hash__(self):
        return hash('List') + hash(self.type)

    def __eq__(self, other):
        return hash(self) == hash(other)


s_str_list = get_list_serializer(s_str)
ds_str_list = get_list_deserializer(ds_str)
register(List(str), s_str_list, ds_str_list)

s_bool_list = get_list_serializer(s_bool)
ds_bool_list = get_list_deserializer(ds_bool)
register(List(bool), s_bool_list, ds_bool_list)

s_long_list = get_list_serializer(s_long)
ds_long_list = get_list_deserializer(ds_long)
register(List(int), s_long_list, ds_long_list)

s_float_list = get_list_serializer(s_float)
ds_float_list = get_list_deserializer(ds_float)
register(List(float), s_float_list, ds_float_list)


@serializer
def s_dict_list(stream, obj):
    if isinstance(obj, types.GeneratorType):
        obj = list(obj)
    s_str(stream, json.dumps(obj))


@deserializer
def ds_dict_list(stream):
    return json.loads(ds_str(stream))


register(List(dict), s_dict_list, ds_dict_list)

register(users.User, s_user, ds_user)
s_user_list = get_list_serializer(s_user)
ds_user_list = get_list_deserializer(ds_user)
register(List(users.User), s_user_list, ds_user_list)
