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

from datetime import date, datetime

from google.appengine.api import users
from google.appengine.ext.ndb.model import Expando, GeoPt, JsonProperty, Key, Model


class NdbModel(Model):
    NAMESPACE = None

    def __init__(self, *args, **kwargs):
        if not kwargs.get('key'):
            kwargs['namespace'] = self.NAMESPACE
        super(NdbModel, self).__init__(*args, **kwargs)

    def _convert_properties(self, prop):
        from fvfw.to import TO
        if isinstance(prop, list):
            for index, p in enumerate(prop):
                prop[index] = self._convert_properties(p)
        elif isinstance(prop, bytes):
            return prop.decode('utf-8')
        elif isinstance(prop, users.User):
            return prop.email()
        elif isinstance(prop, datetime):
            return prop.isoformat() + u'Z'
        elif isinstance(prop, date):
            return prop.isoformat()
        elif isinstance(prop, GeoPt):
            return {'lat': prop.lat, 'lon': prop.lon}
        elif isinstance(prop, Key):
            return prop.id()
        elif isinstance(prop, dict):
            for p in prop:
                prop[p] = self._convert_properties(prop[p])
        elif isinstance(prop, Expando):
            return self._convert_properties(prop.to_dict())
        elif isinstance(prop, TO):
            return prop.to_dict()
        return prop

    def to_dict(self, extra_properties=None, include=None, exclude=None):
        """
            Converts the model to a JSON serializable dictionary

        Args:
            extra_properties (list[str]): Extra properties to add that are present as a @property of the model
            include(set): Optional set of property names to include, default all.
            exclude(set): Optional set of property names to skip, default none.
        Returns:
            dict
        """
        exclude = exclude or []
        if not extra_properties:
            extra_properties = []
        if 'id' not in exclude:
            extra_properties.append('id')
        if not include:
            # Remove orphans (values present in Cloud Datastore but not represented in the Model subclass)
            defined_properties = set(self.__class__._properties.keys())
            include = include or {k for k in self._properties.keys() if k in defined_properties}
        result = super(NdbModel, self).to_dict(include=include, exclude=exclude)
        for p in extra_properties:
            if hasattr(self, p):
                result[p] = getattr(self, p)
        return self._convert_properties(result)

    @classmethod
    def query(cls, *args, **kwargs):
        kwargs['namespace'] = kwargs.get('namespace', cls.NAMESPACE)
        return super(NdbModel, cls).query(*args, **kwargs)

    @classmethod
    def get_by_id(cls, id, parent=None, **ctx_options):
        return super(NdbModel, cls).get_by_id(id, parent=parent, namespace=cls.NAMESPACE, **ctx_options)

    @classmethod
    def get_or_insert(cls, *args, **kwds):
        return super(NdbModel, cls).get_or_insert(*args, namespace=cls.NAMESPACE, **kwds)


# JSON property that can be parsed to an object
class TOProperty(JsonProperty):
    def __init__(self, cls, name=None, compressed=False, json_type=None, **kwds):
        super(TOProperty, self).__init__(name, compressed=compressed, json_type=json_type, **kwds)
        self.cls = cls

    def _from_base_type(self, value):
        from fvfw.rpc import parse_complex_value
        return parse_complex_value(self.cls, value, False) if value else None

    def _to_base_type(self, value):
        from fvfw.rpc import serialize_complex_value
        return serialize_complex_value(value, self.cls, False, skip_missing=True) if value else None
