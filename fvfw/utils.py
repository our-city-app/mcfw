# -*- coding: utf-8 -*-¬
from inspect import ismethod
from typing import TypeVar

T = TypeVar('T')


class Enum(object):

    @classmethod
    def all(cls):
        return [getattr(cls, a) for a in dir(cls) if not a.startswith('_') and not ismethod(getattr(cls, a))]

    @classmethod
    def items(cls):
        return [(a, getattr(cls, a)) for a in dir(cls) if not a.startswith('_') and not ismethod(getattr(cls, a))]


def chunks(iterable, amount):
    """Yield successive amount-sized chunks from iterable."""
    for i in range(0, len(iterable), amount):
        yield iterable[i:i + amount]
