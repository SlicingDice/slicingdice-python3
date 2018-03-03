#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import six

from .. import exceptions
from pyslicer.utils.data_utils import is_str_empty

MAX_QUERY_SIZE = 10

MAX_INSERTION_BATCH_SIZE = 1000


class SDBaseValidator(object):
    """Base column, query and insertion validator."""
    __metaclass__ = abc.ABCMeta

    def check_dictionary_value(self, dictionary_value):
        if isinstance(dictionary_value, dict):
            self.check_dictionary(dictionary_value)
        elif isinstance(dictionary_value, list):
            self.check_list(dictionary_value)
        else:
            if dictionary_value is None or dictionary_value == "":
                raise exceptions.InvalidQueryException(
                    "This query has invalid keys or values.")

    def check_dictionary(self, dictionary):
        if not dictionary:
            raise exceptions.InvalidQueryException(
                "This query has invalid keys or values.")

        for key in dictionary:
            if isinstance(key, dict):
                dictionary_value = key.get('query')
            else:
                dictionary_value = dictionary[key]
            self.check_dictionary_value(dictionary_value)

    def check_list(self, dictionary_list):
        if not dictionary_list:
            raise exceptions.InvalidQueryException(
                "This query has invalid keys or values.")

        for dictionary_value in dictionary_list:
            self.check_dictionary_value(dictionary_value)

    def __init__(self, dictionary):
        if not dictionary:
            raise exceptions.InvalidQueryException(
                "This query has invalid keys or values.")

        self.check_dictionary(dictionary)

        self.data = dictionary

    @abc.abstractmethod
    def validator(self):
        pass


class SavedQueryValidator(SDBaseValidator):
    def __init__(self, dictionary_query):
        """
        Parameters:
            dictionary_query(dict) -- A dict query
        """
        super(SavedQueryValidator, self).__init__(dictionary_query)
        self._list_query_types = [
            "count/entity", "count/event", "count/entity/total",
            "aggregation", "top_values"]

    def _has_valid_type(self):
        """Check if query type is valid
        Returns:
            true if have query type valid
        """
        query_type = self.data['type']
        if query_type not in self._list_query_types:
            raise exceptions.InvalidQueryTypeException(
                "This dictionary don't have query type valid.")
        return True

    def validator(self):
        """
        Returns:
            true if saved query is valid
        """
        if self._has_valid_type():
            return True
        return False


class QueryCountValidator(SDBaseValidator):
    def __init__(self, queries):
        """
        Parameters:
            queries(dict) -- A dict query
        """
        super(QueryCountValidator, self).__init__(queries)

    def validator(self):
        """
        Returns:
            true if count query is valid
        """
        query_size = len(self.data)

        # bypass-cache property should not be considered as query
        if "bypass-cache" in self.data:
            query_size -= 1

        if query_size > MAX_QUERY_SIZE:
            raise exceptions.MaxLimitException(
                "The query count entity has a limit of 10 queries by request.")
        return True


class QueryValidator(SDBaseValidator):
    def __init__(self, queries):
        """
        Parameters:
            queries(dict) -- A dict query
        """
        super(QueryValidator, self).__init__(queries)

    def exceeds_queries_limit(self):
        """Check if query exceeds the limit of 5 queries per request

        Returns:
            true if exceeds the limit
            false otherwise
        """
        if len(self.data) > 5:
            return True
        return False

    def exceeds_columns_limit(self):
        """Check if query exceeds the limit of 5 columns per request

        Returns:
            false if don't exceeds the limit
        """
        for key, value in six.iteritems(self.data):
            if len(value) > 6:
                raise exceptions.MaxLimitException(
                    "The query '{0}' exceeds the limit of columns "
                    "per query in request".format(key))
            if 'contains' not in value and 'equal' not in value:
                raise exceptions.InvalidQueryException(
                    "Each query only can have a 'contain' or a 'equal'.")

        return False

    def exceeds_values_contains_limit(self):
        """Check if query exceeds the limit of 5 contains values per query

        Returns:
            false if don't exceeds the limit
        """
        for key, value in six.iteritems(self.data):
            if "contains" in value and len(value['contains']) > 5:
                raise exceptions.MaxLimitException(
                    "The query '{0}' exceeds the limit of contains "
                    "per query in request".format(key))
        return False

    def validator(self):
        """Returns true if query is ok"""
        if not (self.exceeds_queries_limit() and
                self.exceeds_columns_limit() and
                self.exceeds_values_contains_limit()):
            return True
        return False


class QueryDataExtractionValidator(SDBaseValidator):
    def __init__(self, queries):
        """
        Parameters:
            queries(dict) -- A dict query
        """
        super(QueryDataExtractionValidator, self).__init__(queries)

    def _valid_keys(self):
        """Validate a data extraction query

        Returns:
            true if query is valid
        """
        if "columns" in self.data:
            value = self.data["columns"]
            if not isinstance(value, list) and value != "all":
                raise exceptions.InvalidQueryException(
                    "The key 'columns' in query has a invalid value.")
            else:
                if len(value) > 10:
                    raise exceptions.InvalidQueryException(
                        "The key 'columns' in data extraction result must "
                        "have up to 10 columns.")
        if "limit" in self.data:
            limit = self.data['limit']
            if not isinstance(limit, int):
                raise exceptions.InvalidQueryException(
                    "The key 'limit' in query has a invalid value.")
        return True

    def validator(self):
        if self._valid_keys():
            return True
        return False


class InsertValidator(SDBaseValidator):
    def __init__(self, dictionary_to_insert):
        """
        Parameters:
            dictionary_to_insert(dict) -- A dict query
        """
        super(InsertValidator, self).__init__(dictionary_to_insert)

    def _has_empty_column(self):
        """Check empty columns in dictionary
        Returns:
            false if dictionary don't have empty columns
        """
        for value in self.data.values():
            # Value is a dictionary when it is an entity being inserted:
            # "my-entity": {"year": 2016}
            # It can also be a parameter, such as "auto-create":
            # "auto-create": ["dimension", "column"]
            if not isinstance(
                    value, (dict, list)) or value is None or len(
                        str(value)) == 0:
                raise exceptions.WrongTypeException(
                    "The value for an id should be a dictionary")
        return False

    def check_insertion_size(self):
        insertion_batch_size = len(self.data)

        # auto-create property should not be considered as insertion of data
        if "auto-create" in self.data:
            insertion_batch_size -= 1

        if insertion_batch_size > MAX_INSERTION_BATCH_SIZE:
            raise exceptions.InvalidInsertException(
                "Your insertion command shouldn't have more than 1000 values.")

        return True

    def validator(self):
        """
        Returns:
            true if query is valid
        """
        if not self._has_empty_column() and self.check_insertion_size():
            return True


class ColumnValidator(SDBaseValidator):
    def __init__(self, data_column):
        """
        Parameters:
            data_column -- A dict or list of columns
        """
        if not isinstance(data_column, list):
            data_column = [data_column]
        for dictionary_column in data_column:
            super(ColumnValidator, self).__init__(dictionary_column)
            self._valid_type_columns = [
                "unique-id", "boolean", "string", "integer", "decimal",
                "enumerated", "date", "integer-time-series",
                "decimal-time-series", "string-time-series", "datetime"
            ]

    def _validate_name(self):
        """Validate column name"""
        if 'name' not in self.data:
            raise exceptions.InvalidColumnException(
                "The column should have a name.")
        else:
            name = self.data['name']
            if is_str_empty(name):
                raise exceptions.InvalidColumnNameException(
                    "The column's name can't be empty/None.")
            elif len(name) > 80:
                raise exceptions.InvalidColumnNameException(
                    "The column's name have a very big name.(Max: 80 chars)")

    def _validate_description(self):
        """Validate column description"""
        description = self.data['description']
        if is_str_empty(description):
            raise exceptions.InvalidColumnDescriptionException(
                "The column's description can't be empty/None.")
        elif len(description) > 300:
            raise exceptions.InvalidColumnDescriptionException(
                "The column's description have a very big name.(Max: 300chars)")

    def _validate_column_type(self):
        """Validate column type"""
        if 'type' not in self.data:
            raise exceptions.InvalidColumnException(
                "The column should have a type.")
        type_column = self.data['type']
        if type_column not in self._valid_type_columns:
            raise exceptions.InvalidColumnTypeException(
                "This column have a invalid type.")

    def _validate_column_decimal_type(self):
        """Validate column decimal type"""
        decimal_types = ["decimal", "decimal-time-series"]
        if self.data['type'] not in decimal_types:
            raise exceptions.InvalidColumnException(
                "This column is only accepted on type 'decimal' or"
                "'decimal-time-series'")

    def _check_str_type_integrity(self):
        """Check if column string type has everything you need"""
        if 'cardinality' not in self.data:
            raise exceptions.InvalidColumnException(
                "The column with type string should have "
                "'cardinality' key.")
        cardinality_types = ["high", "low"]
        if self.data['cardinality'] not in cardinality_types:
            raise exceptions.InvalidColumnException(
                "The column 'cardinality' has invalid value.")

    def _validate_enumerate_type(self):
        if 'range' not in self.data:
            raise exceptions.InvalidColumnException(
                "The 'enumerate' type needs of the 'range' parameter.")

    def validator(self):
        """
        Returns:
            true if new column is valid
        """
        self._validate_name()
        self._validate_column_type()
        if self.data['type'] == "string":
            self._check_str_type_integrity()
        if self.data['type'] == "enumerated":
            self._validate_enumerate_type()
        if 'description' in self.data:
            self._validate_description()
        if 'decimal-place' in self.data:
            self._validate_column_decimal_type()
        return True
