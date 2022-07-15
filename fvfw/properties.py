# -*- coding: utf-8 -*-
import pprint
import sys
from functools import reduce

from fvfw.consts import MISSING

try:
    from google.appengine.ext import ndb

    GAE = True
except ImportError:
    GAE = False  # Allow running outside app engine


def rich_str(obj):
    if GAE:
        if isinstance(obj, ndb.Model):
            try:
                d = obj.to_dict()
            except:
                d = {"__class__": obj.__class__}
            for big_data_attr in ('blob', 'picture', 'avatar'):
                if big_data_attr in d:
                    del d[big_data_attr]
            return pprint.pformat(d)
    if isinstance(obj, (dict, list)):
        return pprint.pformat(obj)
    return str(obj)


def azzert(condition, error_message=''):
    if not condition:
        raise AssertionError(reduce(lambda remainder, item: remainder + '\n  - ' + item[0] + ': ' + rich_str(item[1]),
                                    sorted(sys._getframe(1).f_locals.items()), error_message + '\n  Locals: '))


simple_types = {int, str, bool, float, None, dict}
__none__ = '__none__'


# This is basically the same as 'oneOf' in openapi
class object_factory(object):

    # subtype_enum is only used for code generation
    def __init__(self, subtype_attr_name, subtype_mapping, subtype_enum=None):
        self.subtype_attr_name = subtype_attr_name
        self.subtype_mapping = subtype_mapping
        self.subtype_enum = subtype_enum

    def get_subtype(self, instance):
        if instance is MISSING or instance is None:
            return self
        if isinstance(instance, dict):
            subtype_name = instance.get(self.subtype_attr_name)
        else:
            subtype_name = getattr(instance, self.subtype_attr_name, None)

        if subtype_name is None:
            raise ValueError("%s instance has no or empty attribute '%s'" % (type(instance), self.subtype_attr_name))

        subtype = self.subtype_mapping.get(subtype_name, None)
        if subtype is None:
            raise ValueError("'%s' not found in %s" % (subtype_name, self.subtype_mapping))

        return subtype

    @property
    def subtype_mapping_sorted(self):
        return sorted((cls for cls in self.subtype_mapping.values()), key=lambda cls: cls.__name__)

    @property
    def subtype_mapping_keys_sorted(self):
        return sorted(self.subtype_mapping.keys())


class TypedProperty(object):

    def __init__(self, type_, list_=False, doc=None, subtype_attr_name=None, subtype_mapping=None,
                 default=MISSING, required=None, deprecated=False):
        # When deprecated == True, the property is not generated in the client and is only used for backwards
        # compatibility on the server
        self.type = type_
        self.list = list_
        self.attr_name = None  # Set by _fix_up
        self.doc = doc
        azzert((subtype_attr_name and subtype_mapping) or (not subtype_mapping and not subtype_attr_name),
               "supply both subtype_attr_name and subtype_mapping")
        azzert(not (list_ and (subtype_attr_name or subtype_mapping)),
               "subtype_mapping is not supported in combination with lists")
        self.subtype_attr_name = subtype_attr_name
        self.subtype_mapping = subtype_mapping
        if default is not MISSING:
            if list_:
                azzert(hasattr(default, '__iter__') and len(default) == 0,
                       "Only empty list allowed as default value for list properties")
            if default is None:
                azzert(self.type not in ('bool', 'int', 'float'),
                       "None not allowed for %s properties" % self.type)

        self.default = default
        if default is None:
            if required is True:
                raise Exception('default cannot be None when required is True')
            else:
                # When setting 'default' to None, set 'required' to False automatically
                required = False
        self.required = True if required is None else required
        self.deprecated = deprecated

    def __doc__(self):
        return self.doc

    def __get__(self, instance, owner):
        if not instance:
            return self
        else:
            if self.list and not hasattr(instance, self.attr_name):
                setattr(instance, self.attr_name, [])
            return getattr(instance, self.attr_name, self.default)

    def __set__(self, instance, value):
        if value is not MISSING:
            if self.list:
                if not isinstance(value, list):
                    raise ValueError(
                        f'Expected [{self.type}] for \'{self.attr_name}\' and got {type(value)} - {value}!')
                for i, x in enumerate(value):
                    if self._is_invalid_type(x, instance):
                        raise ValueError(
                            f'Not all items are from expected type {str(self.type)}! Encountered item at index {i} with type {type(x)}.')
            else:
                if self._is_invalid_type(value, instance):
                    raise ValueError(f'Expected {self.type} for \'{self.attr_name}\' and got {type(value)} - {value}!')
        # If you get "TypeError: attribute name must be string, not None" on the next line,
        # then _fix_up has not been called on this property. This is usually due to the class this property is
        # registered in does not have the 'TO' class as parent.
        setattr(instance, self.attr_name, value)

    def _fix_up(self, code_name):
        """Internal helper called to tell the property its name.

        This is called by _fix_up_properties() which is called by TOMetaClass when finishing the construction of
         a TO subclass.
        The name passed in is the name of the class attribute to which the Property is assigned (a.k.a. the code name).
        Note that this means that each TypedProperty instance must be assigned to (at most) one class attribute.
        E.g. to declare three strings, you must call unicode_property() three times, you cannot write
        >>> foo = bar = baz = UnicodeProperty()
        """
        if self.attr_name is None:
            self.attr_name = '_' + code_name

    def _is_invalid_type(self, value, instance):
        type_ = self.type.get_subtype(value) if isinstance(self.type, object_factory) else self.type
        if value and not isinstance(value, type_):
            return True

        if self.subtype_attr_name and self.subtype_mapping:
            subtype = self.get_subtype(instance)

            if value and not isinstance(value, subtype):
                raise ValueError(f'Expected {subtype} and got {type(value)} - {value}!')
        if isinstance(self, BoolProperty) and not isinstance(value, bool):
            raise ValueError(f'Expected boolean for {self.attr_name} and got {type(value)} - {value}!')

        return False

    def get_subtype(self, instance):
        subtype_name = getattr(instance, self.subtype_attr_name, None)
        if subtype_name is None:
            raise ValueError(f'{type(instance)} instance has no or empty attribute \'{self.subtype_attr_name}\'')

        subtype = self.subtype_mapping.get(subtype_name, None)
        if subtype is None:
            raise ValueError(f'\'{subtype_name}\' not found in {self.subtype_mapping}')

        return subtype


class UnicodeProperty(TypedProperty):

    def __init__(self, doc=None, empty_string_is_null=False, default=MISSING, required=False,
                 deprecated=False):
        TypedProperty.__init__(self, str, False, doc, default=default,
                               required=required, deprecated=deprecated)
        self._empty_string_is_null = empty_string_is_null

    def __get__(self, instance, owner):
        value = TypedProperty.__get__(self, instance, owner)
        return None if self._empty_string_is_null and isinstance(value, str) and value == '' else value

    def __set__(self, instance, value):
        if self.required and value is None:
            raise TypeError(f'Property {self.attr_name} is required and cannot be None')
        super(UnicodeProperty, self).__set__(instance, value)


class UnicodeListProperty(TypedProperty):
    def __init__(self, doc=None, deprecated=False):
        super(UnicodeListProperty, self).__init__(str, True, doc, default=[], required=True,
                                                  deprecated=deprecated)


class BoolProperty(TypedProperty):
    def __init__(self, doc=None, default=MISSING, deprecated=False):
        # type: (str, bool, bool) -> None
        super(BoolProperty, self).__init__(bool, False, doc, default=default, required=True,
                                           deprecated=deprecated)


class LongProperty(TypedProperty):
    def __init__(self, doc=None, default=MISSING, required=False, deprecated=False):
        super(LongProperty, self).__init__(int, False, doc, default=default, required=required,
                                           deprecated=deprecated)


class LongListProperty(TypedProperty):
    def __init__(self, doc=None, deprecated=False):
        super(LongListProperty, self).__init__(int, True, doc, default=[], required=True,
                                               deprecated=deprecated)


class FloatProperty(TypedProperty):
    def __init__(self, doc=None, default=MISSING, required=False, deprecated=False):
        super(FloatProperty, self).__init__((float, int), False, doc, default=default, required=required,
                                            deprecated=deprecated)


class FloatListProperty(TypedProperty):
    def __init__(self, doc=None, deprecated=False):
        super(FloatListProperty, self).__init__((float, int), True, doc, default=[], required=True,
                                                deprecated=deprecated)


class DictProperty(TypedProperty):
    def __init__(self, value_type, required=False, doc=None, deprecated=False):
        if type(value_type) not in simple_property_types:
            raise ValueError(f'DictProperty does not support {value_type} as value type.')
        super(DictProperty, self).__init__(dict, doc, default={} if required else None, required=required,
                                           deprecated=deprecated)
        self.value_type = value_type


simple_property_types = {UnicodeProperty, UnicodeListProperty, FloatProperty, FloatListProperty, BoolProperty,
                         LongProperty, LongListProperty, DictProperty}

_members_cache = {}


def get_members(type_):
    if type_ in simple_types:
        return [], []
    if type_ in _members_cache:
        return _members_cache[type_]
    else:
        members = type_._get_properties()
        simple_members = [(name, prop) for name, prop in members if type(prop) in simple_property_types]
        complex_members = [x for x in members if x not in simple_members]
        _members_cache[type_] = complex_members, simple_members
    return complex_members, simple_members
