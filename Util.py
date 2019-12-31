import timeit
import logging
from flask import  make_response, json as flask_json

log = logging.getLogger()
log.setLevel(logging.INFO)

class Timer:
    # TODO - support passing log-level as a keyword parameter
    def __init__(self, message=None, extra=None):
        self.message = message
        self.extra = extra

    def __enter__(self):
        self.start = timeit.default_timer()
        return self

    def __exit__(self, *args):
        self.end = timeit.default_timer()
        self.interval = self.end - self.start
        if self.message:
            extra = {'duration': self.interval * 1000}
            extra.update(self.extra)
            log.debug(self.message, extra=extra)

class Response():
    def jsonResponse(obj, status=200, headers=None):
        responseHeaders = headers or {}
        responseHeaders.update({
            "Content-type": "application/json"
        })
        data = flask_json.dumps(obj)

        rsp = make_response(data, status)
        for x in responseHeaders:
            rsp.headers[x] = responseHeaders[x]

        log.debug("Response: %s" % repr(rsp))
        return rsp

class Validate():
    def validateRequestData(data, required_fields=None):
        error_fields = []
        if required_fields:
            if type(required_fields) is list:
                for x in required_fields:
                    if x not in data or data[x] is None:
                        error_fields.append(x)
        return (len(error_fields) == 0, error_fields)
