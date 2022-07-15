# -*- coding: utf-8 -*-
import http.client
from http import HTTPStatus

import requests

from fvfw.properties import LongProperty, TypedProperty, UnicodeProperty
from fvfw.to import TO


class HttpException(Exception):
    http_code = 0

    def __init__(self, error=None, data=None, **kwargs):
        self.data = data or {}
        if not error and self.http_code in http.client.responses:
            error = http.client.responses[self.http_code]
        self.error = error
        super(HttpException, self).__init__(self, error, **kwargs)

    @classmethod
    def from_http_result(cls, http_result):
        # type: (requests.Response) -> HttpException
        err_cls = MAPPING.get(http_result.status_code)
        error = None
        data = None
        try:
            content = http_result.json()
            if 'error' in content:
                error = content['error']
            if 'data' in content:
                data = content['data']
        except:
            pass
        if not err_cls:
            err = HttpException(error, data)
            err.http_code = http_result.status_code
        else:
            err = err_cls(error, data)
        return err


class HttpBadRequestException(HttpException):
    http_code = HTTPStatus.BAD_REQUEST.value

    def __init__(self, *args, **kwargs):
        super(HttpBadRequestException, self).__init__(*args, **kwargs)


class HttpUnauthorizedException(HttpException):
    http_code = HTTPStatus.UNAUTHORIZED.value

    def __init__(self, *args, **kwargs):
        super(HttpUnauthorizedException, self).__init__(*args, **kwargs)


class HttpForbiddenException(HttpException):
    http_code = HTTPStatus.FORBIDDEN.value

    def __init__(self, *args, **kwargs):
        super(HttpForbiddenException, self).__init__(*args, **kwargs)


class HttpNotFoundException(HttpException):
    http_code = HTTPStatus.NOT_FOUND.value

    def __init__(self, *args, **kwargs):
        super(HttpNotFoundException, self).__init__(*args, **kwargs)


class HttpConflictException(HttpException):
    http_code = HTTPStatus.CONFLICT.value

    def __init__(self, *args, **kwargs):
        super(HttpConflictException, self).__init__(*args, **kwargs)


class HttpUnprocessableEntityException(HttpException):
    http_code = HTTPStatus.UNPROCESSABLE_ENTITY.value

    def __init__(self, *args, **kwargs):
        super(HttpUnprocessableEntityException, self).__init__(*args, **kwargs)


class HttpTooManyRequestsException(HttpException):
    http_code = HTTPStatus.TOO_MANY_REQUESTS.value

    def __init__(self, *args, **kwargs):
        super(HttpTooManyRequestsException, self).__init__(*args, **kwargs)


class HttpInternalServerErrorException(HttpException):
    http_code = HTTPStatus.INTERNAL_SERVER_ERROR.value

    def __init__(self, *args, **kwargs):
        super(HttpInternalServerErrorException, self).__init__(*args, **kwargs)


class HttpBadGatewayException(HttpException):
    http_code = HTTPStatus.BAD_GATEWAY.value

    def __init__(self, *args, **kwargs):
        super(HttpBadGatewayException, self).__init__(*args, **kwargs)


MAPPING = {
    HttpBadRequestException.http_code: HttpBadRequestException,
    HttpUnauthorizedException.http_code: HttpUnauthorizedException,
    HttpForbiddenException.http_code: HttpForbiddenException,
    HttpNotFoundException.http_code: HttpNotFoundException,
    HttpConflictException.http_code: HttpConflictException,
    HttpUnprocessableEntityException.http_code: HttpUnprocessableEntityException,
    HttpTooManyRequestsException.http_code: HttpTooManyRequestsException,
    HttpInternalServerErrorException.http_code: HttpInternalServerErrorException,
    HttpBadGatewayException.http_code: HttpBadGatewayException,
}


class ErrorResponse(TO):
    status_code = LongProperty()
    error = UnicodeProperty()
    data = TypedProperty(dict)

    def __init__(self, rest_exception):
        """
        Args:
            rest_exception (fvfw.exceptions.HttpException):
        """
        super(ErrorResponse, self).__init__(
            status_code=rest_exception.http_code,
            error=f'{rest_exception.error}',
            data=rest_exception.data,
        )
