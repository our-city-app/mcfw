[![Build Status](https://travis-ci.org/rogerthat-platform/mcfw.svg?branch=master)](https://travis-ci.org/rogerthat-platform/mcfw)

Here is an example of how the @rest function can be used:

```python
# -*- coding: utf-8 -*-
from mcfw.exceptions import HttpInternalServerErrorException
from mcfw.restapi import rest
from mcfw.rpc import arguments, returns
from mcfw.properties import unicode_property


class ExampleObject(object):
    name = unicode_property('name')

    def __init__(self, name):
        self.name = name

@rest('/hello/<name:[^/]+>', 'get', [], 'v1.0')
@returns(ExampleObject)
@arguments(name=unicode)
def hello_world(name):
    try:
        sentence = 'hello {}'.format(name)
        return ExampleObject(sentence)
    except Exception as e:
        raise HttpInternalServerErrorException(e.message)
```

- Raise a subclass of a `HttpException` to return a specific status code. In this example HttpInternalServerErrorException is raised, so a status code of `500` will be return to the client should an error occur.
- Use @rest(url, method, scopes, api_version). The url can be prefixed, by setting the function INJECTED_FUNCTIONS.get_api_url_template.
An example of this function is:
```python
def get_api_url_template(version, path):
    return '/api/{}/{}'.format(version, path)

```

The full URL for the example hello_world method would look like this: `/api/v1.0/hello/world`
- Use `@returns(return_type)` to specify the return type. The returned object will then be serialized to json and sent to the client.
- Use `@arguments` to specify the type of the arguments. post data (only for POST/PUT requests) will be provided in a 'data' argument to your function. Ensure the type of this data is also provided in `@arguments`. GET parameters (e.g. `?param1=example&param2=test`) will be supplied to the decorated function as arguments, as well as the parameters present in the URL template(`name`, in the example)
- Use `<variable_name:variable_regex>` when getting/updating/deleting 1 type of object in the URL. This variable will be supplied to the decorated function. If the type doesn't match the type specified in `@arguments` a http status code 400 (Bad request) will be returned. Using only `.*` (e.g) `/hello/.*` will **not** work. It is also recommended to use `[^/]+` instead of `.*` in the parameters as this won't conflict with any possible subroutes.
- @rest, @returns and @arguments should be placed in this exact order. Any other order will **not** work
- It's not necessary to add the routes added via @rest to app.yaml, these will match the `/api/.*` route which is present in the framework.
