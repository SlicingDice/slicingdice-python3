#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2016 The Simbiose Ventures Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A library that provides a Python client to Slicing Dice API"""
import ujson

from . import exceptions
from .api import SlicingDiceAPI
from .url_resources import URLResources
from .utils import validators


class SlicingDice(SlicingDiceAPI):
    """A python interface to Slicing Dice API

    Example usage:

        To create an object of the SlicingDice:

            from pyslicer.api import SlicingDice
            sd = SlicingDice('my-token')

        To create a column:

                column_json = {
                    'name': 'Pyslicer String Column',
                    'description': 'Pyslicer example description',
                    'type': 'string',
                    'cardinality': 'low'}
                print sd.create_column(column_json)

        To make a query:

                query_json = {
                    'type': 'count',
                    'select': [
                        {
                            "pyslicer-string-column":
                                {
                                    "equal": "test_value_1"
                                }
                        },
                        "or",
                        {
                            "pyslicer-string-column":
                                {
                                    "equal": "test_value_2"
                                }
                        },
                    ]
                }
                print sd.query(query_json)

        To insert data:

                inserting_json = {
                    'foo@bar.com': {
                        'pyslicer-string-column': 'test_value_1',
                        'pyslicer-integer-column': 42,
                    },
                    'baz@bar.com': {
                        'pyslicer-string-column': 'test_value_2',
                        'pyslicer-integer-column': 42,
                    },
                }
                print sd.insert(inserting_json)
    """

    def __init__(
            self, write_key=None, read_key=None, master_key=None,
            custom_key=None, use_ssl=True, timeout=60):
        """Instantiate a new SlicingDice object.

        Keyword arguments:
        key(string or SlicerKey obj) -- Key to access API
        use_ssl(bool) -- Define if the request uses verification SSL for
            HTTPS requests. Defaults False.(Optional)
        timeout(int) -- Define timeout to request,
            defaults 60 secs(default 30).
        """
        super(SlicingDice, self).__init__(
            master_key, write_key, read_key, custom_key, use_ssl, timeout)

    async def _count_query_wrapper(self, url, query):
        """Validate count query and make request.

        Keyword arguments:
        url(string) -- Url to make request
        query(dict) -- A count query
        """
        sd_count_query = validators.QueryCountValidator(query)
        if sd_count_query.validator():
            return await self._make_request(
                url=url,
                json_data=ujson.dumps(query),
                req_type="post",
                key_level=0)

    async def _data_extraction_wrapper(self, url, query):
        """Validate data extraction query and make request.

        Keyword arguments:
        url(string) -- Url to make request
        query(dict) -- A data extraction query
        """
        sd_extraction_result = validators.QueryDataExtractionValidator(query)
        if sd_extraction_result.validator():
            return await self._make_request(
                url=url,
                json_data=ujson.dumps(query),
                req_type="post",
                key_level=0)

    async def _saved_query_wrapper(self, url, query, update=False):
        """Validate saved query and make request.

        Keyword arguments:
        url(string) -- Url to make request
        query(dict) -- A saved query
        update(bool) -- Indicates with operation is update a
            saved query or not.(default false)
        """
        req_type = "post"
        if update:
            req_type = "put"
        return await self._make_request(
            url=url,
            json_data=ujson.dumps(query),
            req_type=req_type,
            key_level=2)

    async def get_database(self):
        """Get a database associated with this client (related to keys passed
         on construction)"""
        url = SlicingDice.BASE_URL + URLResources.DATABASE
        return await self._make_request(
            url=url,
            req_type="get",
            key_level=2
        )

    async def create_column(self, data):
        """Create column in Slicing Dice

        Keyword arguments:
        data -- A dictionary or list on the Slicing Dice column
            format.
        """
        sd_data = validators.ColumnValidator(data)
        if sd_data.validator():
            url = SlicingDice.BASE_URL + URLResources.COLUMN
            return await self._make_request(
                url=url,
                req_type="post",
                json_data=ujson.dumps(data),
                key_level=1)

    async def get_columns(self):
        """Get a list of columns"""
        url = SlicingDice.BASE_URL + URLResources.COLUMN
        return await self._make_request(
            url=url,
            req_type="get",
            key_level=2)

    async def insert(self, data):
        """Insert data into Slicing Dice API

        Keyword arguments:
        data -- A dictionary in the Slicing Dice data format
            format.
        """
        sd_data = validators.InsertValidator(data)
        if sd_data.validator():
            url = SlicingDice.BASE_URL + URLResources.INSERT
            return await self._make_request(
                url=url,
                json_data=ujson.dumps(data),
                req_type="post",
                key_level=1)

    async def count_entity(self, query):
        """Make a count entity query

        Keyword arguments:
        query -- A dictionary in the Slicing Dice query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_COUNT_ENTITY
        return await self._count_query_wrapper(url, query)

    async def count_entity_total(self, dimensions=None):
        """Make a count entity total query

        Keyword arguments:
        dimensions -- A dictionary containing the dimensions in which
                  the total query will be performed
        """
        query = {}
        if dimensions is not None:
            query['dimensions'] = dimensions
        url = SlicingDice.BASE_URL + URLResources.QUERY_COUNT_ENTITY_TOTAL
        return await self._make_request(
            url=url,
            req_type="post",
            json_data=ujson.dumps(query),
            key_level=0)

    async def count_event(self, query):
        """Make a count event query

        Keyword arguments:
        data -- A dictionary query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_COUNT_EVENT
        return await self._count_query_wrapper(url, query)

    async def aggregation(self, query):
        """Make a aggregation query

        Keyword arguments:
        query -- An aggregation query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_AGGREGATION
        if "query" not in query:
            raise exceptions.InvalidQueryException(
                "The aggregation query must have up the key 'query'.")
        columns = query["query"]
        if len(columns) > 5:
            raise exceptions.MaxLimitException(
                "The aggregation query must have up to 5 columns per request.")
        return await self._make_request(
            url=url,
            json_data=ujson.dumps(query),
            req_type="post",
            key_level=0)

    async def top_values(self, query):
        """Make a top values query

        Keyword arguments:
        query -- A dictionary query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_TOP_VALUES
        sd_query_top_values = validators.QueryValidator(query)
        if sd_query_top_values.validator():
            return await self._make_request(
                url=url,
                json_data=ujson.dumps(query),
                req_type="post",
                key_level=0)

    async def exists_entity(self, ids, dimension=None):
        """Make a exists entity query

        Keyword arguments:
        ids -- A list with entities to check if exists
        dimension -- In which dimension entities check be checked
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_EXISTS_ENTITY
        if len(ids) > 100:
            raise exceptions.MaxLimitException(
                "The query exists entity must have up to 100 ids.")
        query = {
            'ids': ids
        }
        if dimension:
            query['dimension'] = dimension
        return await self._make_request(
            url=url,
            json_data=ujson.dumps(query),
            req_type="post",
            key_level=0)

    async def get_saved_query(self, query_name):
        """Get a saved query

        Keyword arguments:
        query_name(string) -- The name of the saved query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_SAVED + query_name
        return await self._make_request(
            url=url,
            req_type="get",
            key_level=0)

    async def get_saved_queries(self):
        """Get all saved queries

        Keyword arguments:
        query_name(string) -- The name of the saved query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_SAVED
        return await self._make_request(
            url=url,
            req_type="get",
            key_level=2)

    async def delete_saved_query(self, query_name):
        """Delete a saved query

        Keyword arguments:
        query_name(string) -- The name of the saved query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_SAVED + query_name
        return await self._make_request(
            url=url,
            req_type="delete",
            key_level=2
        )

    async def create_saved_query(self, query):
        """Get a list of queries saved

        Keyword arguments:
        query -- A dictionary query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_SAVED
        return await self._saved_query_wrapper(url, query)

    async def update_saved_query(self, name, query):
        """Get a list of queries saved

        Keyword arguments:
        name -- The name of the saved query to update
        query -- A dictionary query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_SAVED + name
        return await self._saved_query_wrapper(url, query, True)

    async def result(self, query):
        """Get a data extraction result

        Keyword arguments:
        query -- A dictionary query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_DATA_EXTRACTION_RESULT
        return await self._data_extraction_wrapper(url, query)

    async def score(self, query):
        """Get a data extraction score

        Keyword arguments:
        query -- A dictionary query
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_DATA_EXTRACTION_SCORE
        return await self._data_extraction_wrapper(url, query)

    async def sql(self, query):
        """ Make a sql query to SlicingDice

        :param query: the query written in SQL format
        :return: The response from the SlicingDice
        """
        url = SlicingDice.BASE_URL + URLResources.QUERY_SQL
        return await self._make_request(
            url=url,
            string_data=query,
            req_type="post",
            key_level=0,
            content_type='application/sql')
