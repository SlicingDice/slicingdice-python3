#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .. import exceptions
from collections import defaultdict


__mapped_errors = {
    2: exceptions.DemoUnavailableException,
    1502: exceptions.RequestRateLimitException,
    1507: exceptions.RequestBodySizeExceededException,
    2012: exceptions.IndexEntitiesLimitException,
    2013: exceptions.IndexColumnsLimitException,
}

slicer_exceptions = defaultdict(
    lambda: exceptions.SlicingDiceException,
    __mapped_errors
)
