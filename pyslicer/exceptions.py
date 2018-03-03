#!/usr/bin/env python
# -*- coding: utf-8 -*-


# General SlicingDice Exception

class SlicingDiceException(Exception):
    def __init__(self, *args, **kwargs):
        self.code = kwargs.pop('code', None)
        self.message = kwargs.pop('message', None)
        self.more_info = kwargs.pop('more-info', None)
        super(SlicingDiceException, self).__init__(self, *args, **kwargs)

    def __str__(self):
        return "SlicingDiceException(code={}, message={}, more_info={})".format(
            self.code, self.message, self.more_info)


class InternalException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InternalException, self).__init__(self, *args, **kwargs)


# Specific SlicingDice Exceptions

class SlicingDiceHTTPError(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(SlicingDiceHTTPError, self).__init__(self, *args, **kwargs)


class DemoUnavailableException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(DemoUnavailableException, self).__init__(self, *args, **kwargs)


class RequestRateLimitException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(RequestRateLimitException, self).__init__(self, *args, **kwargs)


class RequestBodySizeExceededException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(RequestBodySizeExceededException, self).__init__(self, *args,
                                                               **kwargs)


class IndexEntitiesLimitException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(IndexEntitiesLimitException, self).__init__(self, *args, **kwargs)


class IndexColumnsLimitException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(IndexColumnsLimitException, self).__init__(self, *args, **kwargs)


# Validation exceptions


class MaxLimitException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(MaxLimitException, self).__init__(self, *args, **kwargs)


class InvalidQueryException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidQueryException, self).__init__(self, *args, **kwargs)


class InvalidColumnTypeException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidColumnTypeException, self).__init__(self, *args, **kwargs)


class InvalidSlicingDiceKeysException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidSlicingDiceKeysException, self).__init__(self, *args,
                                                              **kwargs)


class InvalidQueryTypeException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidQueryTypeException, self).__init__(self, *args,
                                                        **kwargs)


class WrongTypeException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(WrongTypeException, self).__init__(self, *args,
                                                 **kwargs)


class InvalidInsertException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidInsertException, self).__init__(self, *args,
                                                     **kwargs)


class InvalidColumnException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidColumnException, self).__init__(self, *args,
                                                     **kwargs)


class InvalidColumnNameException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidColumnNameException, self).__init__(self, *args,
                                                         **kwargs)


class InvalidColumnDescriptionException(SlicingDiceException):
    def __init__(self, *args, **kwargs):
        super(InvalidColumnDescriptionException, self).__init__(self, *args,
                                                                **kwargs)
