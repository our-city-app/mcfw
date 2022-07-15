# -*- coding: utf-8 -*-
from os import environ


class MissingClass(object):

    def __reduce__(self):
        # See pickle documentation:
        #
        # If a string is returned, the string should be interpreted as the name of a global variable.
        # It should be the objects local name relative to its module; the pickle module searches the module
        # namespace to determine the objects module. This behaviour is typically useful for singletons.
        return 'MISSING'

    def default(self, value, value_if_missing):
        return value_if_missing if value is self else value


MISSING = MissingClass()
del MissingClass

SERVER_SOFTWARE = environ.get('SERVER_SOFTWARE', 'Development')
DEBUG = SERVER_SOFTWARE.startswith('Development')
