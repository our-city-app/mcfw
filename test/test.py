import unittest
import sys
sys.path.append('..')

from mcfw.properties import unicode_property, long_property, typed_property
from mcfw.rpc import serialize_complex_value, parse_complex_value

class Test(unittest.TestCase):

    def test_mix_type_property(self):

        class MyLittleTO(object):
            name = unicode_property('name')
            age = long_property('age')

            def __str__(self):
                return u"%s is %s years old" % (self.name, self.age)

            def __eq__(self, other):
                return self.name == other.name and self.age == other.age

        class MyPetTO(object):
            person = typed_property('person', (unicode, MyLittleTO))
            crew = typed_property('crew', (unicode, MyLittleTO), True)

        felix = MyPetTO()
        felix.person = u"nice"
        felix.crew = [u"bart", u"donatello"]

        ceaser = MyPetTO()
        ceaser.person = MyLittleTO()
        ceaser.person.name = u"ceaser"
        ceaser.person.age = 35
        ceaser.crew = [u"bart", MyLittleTO()]
        ceaser.crew[1].name = u"donatello"
        ceaser.crew[1].age = 34

        for person in ceaser.crew:
            print person

        serialized = serialize_complex_value(ceaser, MyPetTO, False)
        print serialized
        ceaser2 = parse_complex_value(MyPetTO, serialized, False)
        self.assertEquals(ceaser.person.name, ceaser2.person.name)
        self.assertEquals(ceaser.person.age, ceaser2.person.age)
        self.assertEquals(ceaser.crew[0], ceaser2.crew[0])
        self.assertEquals(ceaser.crew[1], ceaser2.crew[1])


if __name__ == '__main__':
    unittest.main()
