# -*- coding: utf-8 -*-
import inspect
import itertools
import logging
import time
import types
from functools import wraps
from typing import Any, Union

from google.appengine.ext import ndb

from fvfw.cache import set_cache_key
from fvfw.consts import MISSING
from fvfw.properties import get_members, object_factory, simple_types
from fvfw.utils import T


class MissingArgumentException(Exception):

    def __init__(self, name, func=None):
        self.name = name
        self.message = f'{name} is a required argument{(" in function %s" % func.__name__) if func else ""}!'
        super(MissingArgumentException, self).__init__(self.message)

    def __str__(self):
        return self.message


def log_access(call=True, response=True):
    def wrap(f):

        def logged(*args, **kwargs):
            if call:
                arg_str = ''
                for i, arg in enumerate(args):
                    arg_str += f'  {i}: {arg}\n'
                kwarg_str = ''
                for kw, arg in kwargs.items():
                    kwarg_str += f'  {kw}: {arg}\n'
                logging.debug(f'{f.__module__}.{f.__name__}\nargs:\n{arg_str}kwargs:\n{kwarg_str}')
            start = time.time()
            try:
                result = f(*args, **kwargs)
                if response:
                    end = time.time()
                    logging.debug(
                        f'{f.__module__}.{f.__name__} finished in {end - start} seconds returning {result}')
                return result
            except:
                if response:
                    end = time.time()
                    logging.exception(f'{f.__module__}.{f.__name__} failed in {end - start} seconds')
                raise

        set_cache_key(logged, f)
        logged.__name__ = f.__name__
        logged.__module__ = f.__module__
        if hasattr(f, 'meta'):
            logged.meta.update(f.meta)
        return logged

    return wrap


def arguments(**kwarg_types):
    """ The arguments decorator function describes & validates the parameters of the function."""
    for value in kwarg_types.values():
        _validate_type_spec(value)

    def wrap(f):
        # validate argspec
        f_args = inspect.getfullargspec(f)
        f_args = inspect.ArgSpec([a for a in f_args[0] if a not in ('self', 'cls')], f_args[1], f_args[2], f_args[3])
        f_arg_count = len(f_args[0])
        f_defaults = f_args[3]
        if not f_defaults:
            f_defaults = []
        f_arg_defaults_count = len(f_defaults)
        f_arg_no_defaults_count = f_arg_count - f_arg_defaults_count
        f_arg_defaults = {
            f_args[0][i]: f_defaults[i - f_arg_no_defaults_count] if i >= f_arg_no_defaults_count else MISSING
            for i in range(f_arg_count)}
        f_pure_default_args_dict = {f_args[0][i]: f_defaults[i - f_arg_no_defaults_count]
                                    for i in range(f_arg_no_defaults_count, f_arg_count)}
        if f_arg_count != len(kwarg_types):
            raise ValueError(
                f'{f.__name__}: function signature contains a different amount of arguments than the type annotations.'
                f'\nExpected: {list(kwarg_types.keys())}\nActual: {f_args.args}')
        unknown_args = [arg for arg in f_args[0] if arg not in kwarg_types]
        if unknown_args:
            raise ValueError(f"No type information is supplied for {', '.join(unknown_args)}!")

        @wraps(f)
        def typechecked_f(*args, **kwargs):
            arg_length = len(args)
            if arg_length > f_arg_count:
                raise ValueError(f"{f.__name__}() takes {f_arg_count} arguments ({arg_length} given)")

            for i in range(arg_length):
                kwargs[f_args[0][i]] = args[i]

            # accept MISSING as magical value or not
            accept_missing = u'accept_missing' in kwargs
            if accept_missing:
                kwargs.pop(u'accept_missing')
            # apply default value if available
            for arg in kwarg_types:
                value = kwargs.get(arg, f_arg_defaults[arg])
                if value is MISSING:
                    value = f_arg_defaults.get(arg, MISSING)
                kwargs[arg] = value
            # validate number of arguments
            if not len(kwargs) == len(kwarg_types):
                raise ValueError(f'kwarg mismatch\nExpected:{kwarg_types}\nGot:{kwargs}')
            # validate supplied arguments
            unknown_args = [arg for arg in kwargs if arg not in kwarg_types]
            if unknown_args:
                raise ValueError(f"Unknown argument(s) {', '.join(unknown_args)} supplied!")
            # validate argument values
            for arg in kwargs:
                _check_type(arg, kwarg_types[arg], kwargs[arg], accept_missing=accept_missing, func=f)
            return f(**kwargs)

        set_cache_key(typechecked_f, f)
        typechecked_f.__name__ = f.__name__
        typechecked_f.__module__ = f.__module__
        typechecked_f.meta[u"fargs"] = f_args
        typechecked_f.meta[u"kwarg_types"] = kwarg_types
        typechecked_f.meta[u"pure_default_args_dict"] = f_pure_default_args_dict
        if hasattr(f, u"meta"):
            typechecked_f.meta.update(f.meta)

        return typechecked_f

    return wrap


def returns(type_=None):
    """ The retunrs decorator function describes & validates the result of the function."""
    _validate_type_spec(type_)

    def wrap(f):
        @wraps(f)
        def typechecked_return(*args, **kwargs):
            result = f(*args, **kwargs)
            return _check_type(u'Result of function', type_, result, func=f)

        set_cache_key(typechecked_return, f)
        typechecked_return.__name__ = f.__name__
        typechecked_return.__module__ = f.__module__
        typechecked_return.meta['return_type'] = type_
        if hasattr(f, 'meta'):
            typechecked_return.meta.update(f.meta)
        return typechecked_return

    return wrap


def run(function, args, kwargs):
    kwargs['accept_missing'] = None
    result = function(*args, **kwargs)
    type_, islist = _get_return_type_details(function)
    return serialize_value(result, type_, islist, skip_missing=True)


def parse_parameters(function, parameters):
    kwarg_types = get_parameter_types(function)
    return get_parameters(parameters, kwarg_types)


def parse_complex_value(type_: T, value: Any, islist: bool) -> Union[T, list[T]]:
    if value is None or type_ is dict:
        return value
    parser = _get_complex_parser(type_)
    if islist:
        return list(map(parser, value))
    else:
        return parser(value)


def check_function_metadata(function):
    if 'kwarg_types' not in function.meta or 'return_type' not in function.meta:
        raise ValueError('Can not execute function. Too little meta information is available!')


def get_parameter_types(function):
    return function.meta['kwarg_types']


def get_parameters(parameters, kwarg_types):
    return {name: parse_parameter(name, type_, parameters[name]) if name in parameters else MISSING
            for name, type_ in kwarg_types.items()}


def get_type_details(type_, value=MISSING):
    if isinstance(type_, tuple):
        # The value can have multiple types.
        if value is not MISSING:
            # We must find the type by comparing the possible types with the real type of <value>
            value_is_list = isinstance(value, list)
            if value_is_list:
                if not value:
                    return str, True  # The type doesn't matter, the list is empty
                value = value[0]
            for t in type_:
                is_list = isinstance(t, list)
                if is_list != value_is_list:
                    continue
                if is_list:
                    type_to_check = t[0]
                else:
                    type_to_check = t
                if isinstance(value, type_to_check):
                    return type(value), is_list
                    # Weird... type not found and @arguments didn't raise... The serialization will probably fail.

    is_list = isinstance(type_, list)
    if is_list:
        type_ = type_[0]
    return type_, is_list


def serialize_complex_value(value, type_, islist, skip_missing=False):
    if type_ is dict or value is None:
        return value

    def optimal_serializer(val):
        if not isinstance(type_, object_factory) and isinstance(val, type_):
            serializer = _get_complex_serializer(val.__class__)
        else:
            serializer = _get_complex_serializer(type_)
        return serializer(val, skip_missing)

    if islist:
        return list(map(optimal_serializer, value))
    else:
        return optimal_serializer(value)


def serialize_value(value, type_, islist, skip_missing=False):
    if value is None \
            or type_ in simple_types \
            or (isinstance(type_, tuple) and all(t in simple_types for t in type_)):
        return value
    else:
        return serialize_complex_value(value, type_, islist, skip_missing)


def parse_parameter(name, type_, value):
    raw_type, is_list = get_type_details(type_, value)
    if isinstance(value, list) != is_list:
        raise ValueError(f'list expected for parameter {name} and got {value} or vice versa!')
    if is_list:
        return [_parse_value(name, raw_type, x) for x in value]
    else:
        return _parse_value(name, raw_type, value)


def _validate_type_spec(type_):
    if isinstance(type_, list) and len(type_) != 1:
        raise ValueError('Illegal type specification!')


DICT_KEY_ITERATOR_TYPE = type(iter(dict().keys()))


def _check_type(name, checktype, value, accept_missing=False, func=None):
    if value == MISSING:
        if accept_missing:
            return value
        else:
            raise MissingArgumentException(name, func)

    if value is None and (isinstance(checktype, list) or checktype not in (int, float, bool)):
        return value

    if isinstance(checktype, tuple):
        # multiple types are allowed. checking if value is one of the them.
        errors = []
        for t in checktype:
            try:
                return _check_type(name, t, value, accept_missing, func)
            except (ValueError, TypeError) as e:
                errors.append(e)
                continue
        logging.debug('\n\n'.join(map(str, errors)))
        raise ValueError(f'{name} is not of expected type {str(checktype)}! Its type is {type(value)}:\n{value}')

    if isinstance(checktype, list) and isinstance(value, list):
        checktype = checktype[0]

        for i, x in enumerate(value):
            t = checktype.get_subtype(x) if isinstance(checktype, object_factory) else checktype
            if not isinstance(x, t):
                raise ValueError(
                    f'{name}: Not all items were of expected type {str(checktype)}. Encountered an item at index {i} with type {type(x)}: {x}.')
    elif isinstance(checktype, list) and isinstance(value, (
            types.GeneratorType, ndb.Query, ndb.query.QueryIterator, itertools.chain, DICT_KEY_ITERATOR_TYPE)):
        checktype = checktype[0]

        def checkStreaming():
            for o in value:
                if not isinstance(o, checktype):
                    raise ValueError(
                        f'{name}: Not all items were of expected type {str(checktype)}. Encountered an item with type {type(o)}: {o}.')
                yield o

        return checkStreaming()
    elif checktype == type and isinstance(value, list):
        if len(value) != 1:
            raise ValueError(f'{name}: unexpected type count ({len(value)})')

        def check(t, i):
            if not isinstance(t, type):
                raise ValueError(
                    f'{name}: Not all items were of expected type {str(checktype)}. Encountered an item at index {i} with type {type(x)}: {x}.')

        if isinstance(value[0], tuple):
            for i, t in enumerate(value[0]):
                check(t, i)
        else:
            check(value[0], 0)
    else:
        if isinstance(checktype, object_factory):
            checktype = checktype.get_subtype(value)
        try:
            if not isinstance(value, checktype):
                raise ValueError(
                    f'{name} is not of expected type {str(checktype)}! Its type is {type(value)}:\n{value}')
        except TypeError as e:
            raise TypeError(f'{e}\nvalue: {value}\nchecktype: {checktype}')
    return value


_complexParserCache = {}


def _get_complex_parser(type_):
    if type_ in _complexParserCache:
        return _complexParserCache[type_]

    def parse(value):
        t = type_.get_subtype(value) if isinstance(type_, object_factory) else type_
        inst = t()

        complex_members, simple_members = get_members(t)
        for name, prop in simple_members:
            setattr(inst, name, value[name] if name in value else getattr(t, name).default)
        for name, prop in complex_members:
            setattr(inst, name, parse_complex_value(
                prop.get_subtype(inst) if (prop.subtype_attr_name and prop.subtype_mapping) else prop.type,
                value[name], prop.list) if name in value else MISSING)
        return inst

    _complexParserCache[type_] = parse
    return parse


_value_types = {int, float, bool, None}


def _parse_value(name, type_, value):
    def raize():
        raise ValueError("Incorrect type received for parameter '%s'. Expected %s and got %s (%s)."
                         % (name, type_, type(value), value))

    istuple = isinstance(type_, tuple)
    if (istuple and set(type_).issubset(_value_types)) or type_ in _value_types:
        if not isinstance(value, type_):
            raize()
        return value
    elif istuple:
        for tt in type_:
            try:
                return _parse_value(name, tt, value)
            except ValueError:
                pass
        raize()
    elif value is None:
        return None
    elif type_ == str:
        if not isinstance(value, str):
            raize()
        return value if isinstance(value, str) else str(value)
    elif type_ == str:
        if not isinstance(value, str):
            raize()
        return value
    elif not isinstance(value, dict):
        raize()
    return parse_complex_value(type_, value, False)


_complex_serializer_cache = {}


def _get_complex_serializer(type_):
    if type_ in _complex_serializer_cache:
        return _complex_serializer_cache[type_]

    def serializer(value, skip_missing):
        t = type_.get_subtype(value) if isinstance(type_, object_factory) else type_
        complex_members, simple_members = get_members(t)

        result = {name: getattr(value, name) for (name, _) in simple_members
                  if not skip_missing or getattr(value, name) is not MISSING}

        for name, prop in complex_members:
            attr = getattr(value, name)
            if not skip_missing or attr is not MISSING:
                real_type = prop.get_subtype(value) if (prop.subtype_attr_name and prop.subtype_mapping) else prop.type
                result[name] = serialize_complex_value(attr, real_type, prop.list, skip_missing)

        return result

    _complex_serializer_cache[type_] = serializer
    return serializer


def _get_return_type_details(function):
    return get_type_details(function.meta['return_type'])
