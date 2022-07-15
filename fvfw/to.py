# coding=utf-8
from typing import Any, Type, TypeVar

from fvfw.properties import TypedProperty
from fvfw.rpc import parse_complex_value, serialize_complex_value

TO_TYPE = TypeVar('TO_TYPE', bound='TO')


class TOMetaClass(type):
    """Metaclass for TO.
    This exists to fix up the property -- they need to know their name.
    This is accomplished by calling the class's _fix_properties() method."""

    def __init__(cls, name, bases, classdict):
        super(TOMetaClass, cls).__init__(name, bases, classdict)
        cls._fix_up_properties()

    def __repr__(self):
        props = ('%s=%r' % (prop.attr_name, prop) for _, prop in self._get_properties())
        return '%s<%s>' % (self.__name__, ', '.join(props))


class TO(object, metaclass=TOMetaClass):
    _properties: dict[str, TypedProperty] = None

    def __str__(self):
        # Useful when debugging. Can be evaluated to get an object with the same properties back.
        return '%s(%s)' % (self.__class__.__name__, ', '.join('%s=%r' % (k, getattr(self, k))
                                                              for k in self.to_dict()))

    __repr__ = __str__

    def __init__(self, **kwargs):
        if 'type' in kwargs and isinstance(kwargs['type'], str):
            # Fix for creating objects with subtype_mapping via constructor
            setattr(self, 'type', kwargs['type'])
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__class__.__name__)

    @classmethod
    def _fix_up_properties(cls):
        """Fix up the properties by calling their _fix_up() method.

        Note: This is called by MetaModel, but may also be called manually
        after dynamically updating a TO class.
        """
        cls._properties = {}  # Map of {name: TypedProperty}
        if cls.__module__ == __name__:  # Skip the classes in *this* file.
            return
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, TypedProperty):
                attr._fix_up(name)
                cls._properties[name] = attr

    @classmethod
    def _get_properties(cls):
        return sorted(iter(cls._properties.items()), key=lambda p: p[0])

    def to_dict(self, include: list[str] = None, exclude: list[str] = None) -> dict[str, Any]:
        result = serialize_complex_value(self, type(self), False, skip_missing=True)
        if include:
            if not isinstance(include, list):
                include = {include}
            return {key: result[key] for key in include if key in result}
        if exclude:
            if not isinstance(exclude, list):
                exclude = {exclude}
            blacklisted_keys = set(result.keys()) - set(exclude)
            return {key: result[key] for key in blacklisted_keys if key in result}
        return result

    @classmethod
    def from_dict(cls: Type[TO_TYPE], data: dict) -> TO_TYPE:
        return parse_complex_value(cls, data, False)

    @classmethod
    def from_list(cls: Type[TO_TYPE], data: list[dict]) -> list[TO_TYPE]:
        return parse_complex_value(cls, data, True)

    @classmethod
    def from_model(cls, m):
        return cls.from_dict(m.to_dict())
