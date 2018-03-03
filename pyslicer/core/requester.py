#!/usr/bin/env python
# -*- coding: utf-8 -*-
import aiohttp

from .. import exceptions


class Requester(object):
    def __init__(self, use_ssl, timeout):
        self.use_ssl = use_ssl
        self.session = aiohttp.ClientSession(read_timeout=timeout)

    async def post(self, url, data, headers):
        """Executes a post request result object"""
        try:
            async with self.session.post(url, data=data, headers=headers,
                                         verify_ssl=self.use_ssl) as resp:
                return resp.status, await resp.text()
        except aiohttp.ClientConnectorError as e:
            raise exceptions.SlicingDiceHTTPError(e)
        except aiohttp.ServerTimeoutError as e:
            raise exceptions.SlicingDiceHTTPError(e)

    async def put(self, url, data, headers):
        """Returns a put request result object"""
        try:
            async with self.session.put(url, data=data, headers=headers,
                                        verify_ssl=self.use_ssl) as resp:
                return resp.status, await resp.text()
        except aiohttp.ClientConnectorError as e:
            raise exceptions.SlicingDiceHTTPError(e)
        except aiohttp.ServerTimeoutError as e:
            raise exceptions.SlicingDiceHTTPError(e)

    async def get(self, url, headers):
        """Returns a get request result object"""
        try:
            async with self.session.get(url, headers=headers,
                                        verify_ssl=self.use_ssl) as resp:
                return resp.status, await resp.text()
        except aiohttp.ClientConnectorError as e:
            raise exceptions.SlicingDiceHTTPError(e)
        except aiohttp.ServerTimeoutError as e:
            raise exceptions.SlicingDiceHTTPError(e)

    async def delete(self, url, headers):
        """Returns a delete request result object"""
        try:
            async with self.session.delete(url, headers=headers,
                                           verify_ssl=self.use_ssl) as resp:
                return resp.status, await resp.text()
        except aiohttp.ClientConnectorError as e:
            raise exceptions.SlicingDiceHTTPError(e)
        except aiohttp.ServerTimeoutError as e:
            raise exceptions.SlicingDiceHTTPError(e)
