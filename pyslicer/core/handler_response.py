#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyslicer.core.helper_handler_exceptions import slicer_exceptions


class SDHandlerResponse(object):
    def __init__(self, result, status_code, headers):
        self.status_code = status_code
        self.headers = headers
        self.result = result

    def _raise_error(self):
        """Find API error."""
        code_error = int(self.result['errors'][0]['code'])
        exception = slicer_exceptions[code_error]
        raise exception(**self.result['errors'][0])

    def request_successful(self):
        """Returns true if request was successful
        """
        if 'errors' in self.result:
            self._raise_error()
        return True
