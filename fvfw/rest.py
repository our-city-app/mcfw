from typing import Callable, get_type_hints, get_origin, get_args

from flask import request, Response, jsonify

from fvfw.rpc import _check_type, parse_complex_value, serialize_complex_value


def rest():
    """Decorator that changes the 'data' argument of rest functions to be the type specified in the type annotation.
    Converts the return type of these rest functions to a Response in case a TO was returned."""

    def decorator(func: Callable):
        hints = get_type_hints(func)
        _return_type = hints.get('return')
        if not _return_type:
            raise Exception(f'You must specify the return type when using the @rest decorator: {func.__name__}')
        return_type_is_list = get_origin(_return_type) is list
        return_type = get_args(_return_type)[0] if return_type_is_list else _return_type
        _request_type = hints.get('data')
        request_type_is_list = get_origin(_request_type) is list
        request_type = get_args(_request_type)[0] if request_type_is_list else _request_type

        def wrapper(**kwargs):
            for arg_name, hint in hints.items():
                if arg_name == 'return':
                    continue
                if arg_name == 'data' and request_type_is_list:
                    hint = [hint]
                _check_type(arg_name, hint, kwargs[arg_name], accept_missing=False, func=func)
            if request_type:
                data = parse_complex_value(request_type, request.get_json(), request_type_is_list)
                kwargs['data'] = data
            result = func(**kwargs)
            rtype = [return_type] if return_type_is_list else return_type
            _check_type('Result of function', rtype, result, func=func)
            if isinstance(result, Response):
                return result
            if return_type is None:
                return Response(status=204)
            serialized = serialize_complex_value(result, return_type, return_type_is_list, skip_missing=True)
            return jsonify(serialized)

        wrapper.__name__ = func.__name__
        return wrapper

    return decorator
