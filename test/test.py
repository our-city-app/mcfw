# -*- coding: utf-8 -*-
# Copyright 2018 Mobicage NV
# NOTICE: THIS FILE HAS BEEN MODIFIED BY MOBICAGE NV IN ACCORDANCE WITH THE APACHE LICENSE VERSION 2.0
# Copyright 2018 GIG Technology NV
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
# @@license_version:1.5@@

import unittest

from fvfw.properties import UnicodeProperty, LongProperty, TypedProperty
from fvfw.to import TO


class Test(unittest.TestCase):

    def test_typed_property(self):
        class MyLittleTO(TO):
            name = UnicodeProperty()
            age = LongProperty()

            def __str__(self):
                return f'{self.name} is {self.age} years old'

            def __eq__(self, other):
                return self.name == other.name and self.age == other.age

        class MyPetTO(TO):
            person = TypedProperty(MyLittleTO)
            crew = TypedProperty(MyLittleTO, True)

        felix = MyPetTO()
        felix.person = MyLittleTO(name='nice', age=69)
        felix.crew = [MyLittleTO(name='bart'), MyLittleTO(name='donatello')]

        ceaser = MyPetTO(
            person=MyLittleTO(
                name='ceaser',
                age=35,
            )
        )
        ceaser.crew = [MyLittleTO()]
        ceaser.crew[0].name = 'donatello'
        ceaser.crew[0].age = 34

        for person in ceaser.crew:
            print(person)

        serialized = ceaser.to_dict()
        print(serialized)
        ceaser2 = MyPetTO.from_dict(serialized)
        self.assertEqual(ceaser.person.name, ceaser2.person.name)
        self.assertEqual(ceaser.person.age, ceaser2.person.age)
        self.assertEqual(ceaser.crew[0], ceaser2.crew[0])


if __name__ == '__main__':
    unittest.main()
