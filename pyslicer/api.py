#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from . import exceptions
from .core.requester import Requester


class SlicingDiceAPI(object):
    """A python interface to make requests in Slicing Dice API"""

    BASE_URL = os.environ.get(
        'SD_API_ADDRESS',
        'https://api.slicingdice.com/v1')

    def __init__(
            self, master_key=None, write_key=None, read_key=None,
            custom_key=None, use_ssl=True, timeout=60):
        """Instantiate a new SlicerDicer object.

        Keyword arguments:
        key(string or SlicerKey) -- Key(s) to access API
        use_ssl(bool) -- Define if the request uses verification SSL for
            HTTPS requests. Defaults False.(Optional)
        timeout(int) -- Define timeout to request,
            defaults 60 secs(Optional).
        """
        self.keys = self._organize_keys(
            master_key, custom_key, read_key, write_key)
        self._api_key = self._get_key()[0]
        self._requester = Requester(use_ssl, timeout)

    @staticmethod
    def _organize_keys(master_key, custom_key, read_key, write_key):
        return {
            "master_key": master_key,
            "custom_key": custom_key,
            "read_key": read_key,
            "write_key": write_key
        }

    def _get_key(self):
        if self.keys["master_key"] is not None:
            return [self.keys["master_key"], 2]
        elif self.keys["custom_key"] is not None:
            return [self.keys["custom_key"], 2]
        elif self.keys["write_key"] is not None:
            return [self.keys["write_key"], 1]
        elif self.keys["read_key"] is not None:
            return [self.keys["read_key"], 0]
        raise exceptions.InvalidSlicingDiceKeysException("You need put a key.")

    def _check_key(self, key_level):
        """Select automatically a key to make the request in Slicing Dice

        Keyword arguments:
        key_level(int) -- Define the key level needed
        """
        current_key_level = self._get_key()
        if current_key_level[1] == 2:
            return current_key_level[0]
        if current_key_level[1] != key_level:
            raise exceptions.InvalidSlicingDiceKeysException(
                "This key is not allowed to perform this operation.")
        return current_key_level[0]

    async def _make_request(self, url, req_type, key_level, json_data=None,
                            string_data=None, content_type='application/json'):
        """Returns a object request result

        Keyword arguments:
        url(string) -- the url to make a request
        req_type(string) -- the request type (POST, PUT, DELETE or GET)
        key_level(int) -- Define the key level needed
        json_data(json) -- The json to use on request (default None)
        content_type(string) -- The content_type to use in the request (default
         'application/json')
        """
        self._check_key(key_level)
        headers = {'Content-Type': content_type,
                   'Authorization': self._api_key}

        data = json_data
        if string_data is not None and json_data is None:
            data = string_data
        req = None

        if req_type == "post":
            req = self._requester.post(
                url,
                data=data,
                headers=headers)
        elif req_type == "get":
            req = self._requester.get(
                url,
                headers=headers)

        elif req_type == "delete":
            req = self._requester.delete(
                url,
                headers=headers)

        elif req_type == "put":
            req = self._requester.put(
                url,
                data=data,
                headers=headers)

        status, result = await req
        return result
