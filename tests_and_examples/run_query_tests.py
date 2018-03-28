"""Tests SlicingDice endpoints.

This script tests SlicingDice by running tests suites, each composed by:
    - Creating columns
    - Insertion of data
    - Querying
    - Comparing results

All tests are stored in JSON files at ./examples named as the query being
tested:
    - count_entity.json
    - count_event.json

In order to execute the tests, simply replace API_KEY by the demo API key and
run the script with:
    $ python run_tests.py
"""

import json
import os
import sys
import time

import asyncio

import ujson

from pyslicer import SlicingDice
from pyslicer.exceptions import SlicingDiceException

# Suppress HTTPS warnings
# import requests
# from requests.packages.urllib3.exceptions import InsecureRequestWarning
#
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class SlicingDiceTester(object):
    per_test_insertion = False

    """Test orchestration class."""
    def __init__(self, api_key, verbose=False):
        # The Slicing Dice API client
        self.client = SlicingDice(master_key=api_key)

        # Translation table for columns with timestamp
        self.column_translation = {}

        # Sleep time in seconds
        self.sleep_time = int(os.environ.get("CLIENT_SLEEP_TIME", 10))
        # Directory containing examples to test
        self.path = 'examples/'
        # Examples file format
        self.extension = '.json'

        self.num_successes = 0
        self.num_fails = 0
        self.failed_tests = []

        self.verbose = verbose

    async def run_tests(self, query_type):
        """Run all tests for a given query type.

        Parameters:
        query_type -- String containing the name of the query that will be
            tested. This name must match the JSON file name as well.
        """
        test_data = self.load_test_data(query_type)
        num_tests = len(test_data)

        self.per_test_insertion = "insert" in test_data[0]
        if not self.per_test_insertion:
            insertion_data = self.load_test_data(query_type, suffix="_insert")
            for insertion in insertion_data:
                await self.client.insert(insertion)
            time.sleep(self.sleep_time)

        for i, test in enumerate(test_data):
            self._empty_column_translation()

            print('({}/{}) Executing test "{}"'.format(i + 1, num_tests,
                                                       test['name']))

            if 'description' in test:
                print('  Description: {}'.format(test['description']))

            print('  Query type: {}'.format(query_type))

            try:
                if self.per_test_insertion:
                    auto_create = test['insert'].get('auto-create', [])
                    if auto_create:
                        self.get_columns_from_insertion_data(test)
                    else:
                        await self.create_columns(test)
                    await self.insert_data(test)

                result = await self.execute_query(query_type, test)
                result = ujson.loads(result)
            except SlicingDiceException as e:
                result = {'result': {'error': str(e)}}

            await self.compare_result(query_type, test, result)

    def _empty_column_translation(self):
        """Erase column translation table so tests don't interfere each
        other."""
        self.column_translation = {}

    def load_test_data(self, query_type, suffix=''):
        """Load all test data from JSON file for a given query type.

        Parameters:
        query_type -- String containing the name of the query that will be
            tested. This name must match the JSON file name as well.

        Return:
        Test data as a dictionary.
        """
        filename = self.path + query_type + suffix + self.extension
        return json.load(open(filename))

    async def create_columns(self, test):
        """Create columns for a given test.

        Parameters:
        test -- Dictionary containing test name, columns metadata, data to be
            inserted, query, and expected results.
        """
        is_singular = len(test['columns']) == 1
        column_or_columns = 'column' if is_singular else 'columns'
        print('  Creating {} {}'.format(len(test['columns']),
                                        column_or_columns))

        for column in test['columns']:
            self._append_timestamp_to_column_name(column)
            await self.client.create_column(column)

            if self.verbose:
                print('    - {}'.format(column['api-name']))

    def _append_timestamp_to_column_name(self, column):
        """Append integer timestamp to column name.

        This technique allows the same test suite to be executed over and over
        again, since each execution will use different column names.

        Parameters:
        column -- Dictionary containing column data, such as "name" and
            "api-name".
        """
        old_name = '{}'.format(column['api-name'])

        timestamp = self._get_timestamp()
        column['name'] += timestamp
        column['api-name'] += timestamp
        new_name = '{}'.format(column['api-name'])

        self.column_translation[old_name] = new_name

    @staticmethod
    def _get_timestamp():
        """Get integer timestamp in string format.

        Return:
        String with integer timestamp.
        """
        # Appending integer timestamp including second decimals
        return str(int(time.time() * 10))

    def get_columns_from_insertion_data(self, test):
        """Get all column names from inserted data and translate them.

        Parameters:
        test -- Dictionary containing test name, columns metadata, data to be
            inserted, query, and expected results.
        """
        print('  Auto-creating columns')
        for entity, data in test['insert'].items():
            if entity != 'auto-create':
                for column in data.keys():
                    if column not in self.column_translation:
                        self._append_timestamp_to_column_name(
                            {"api-name": column, "name": column})

    async def insert_data(self, test):
        """Insert data to SlicingDice.

        Parameters:
        test -- Dictionary containing test name, columns metadata, data to be
            inserted, query, and expected results.
        """
        is_singular = len(test['insert']) == 1
        entity_or_entities = 'entity' if is_singular else 'entities'
        print('  Inserting {} {}'.format(len(test['insert']),
                                         entity_or_entities))

        insertion_data = self._translate_column_names(test['insert'])

        if self.verbose:
            print('    - {}'.format(insertion_data))

        await self.client.insert(insertion_data)

        # Wait a few seconds so the data can be inserted by SlicingDice
        time.sleep(self.sleep_time)

    async def execute_query(self, query_type, test):
        """Execute query at SlicingDice.

        Parameters:
        query_type -- String containing the name of the query that will be
            tested. This name must match the JSON file name as well.
        test -- Dictionary containing test name, columns metadata, data to be
            inserted, query, and expected results.
        """
        if self.per_test_insertion:
            query_data = self._translate_column_names(test['query'])
        else:
            query_data = test['query']
        print('  Querying')

        if self.verbose:
            print('    - {}'.format(query_data))

        result = None
        if query_type == 'count_entity':
            result = await self.client.count_entity(
                query_data)
        elif query_type == 'count_event':
            result = await self.client.count_event(
                query_data)
        elif query_type == 'top_values':
            result = await self.client.top_values(
                query_data)
        elif query_type == 'aggregation':
            result = await self.client.aggregation(
                query_data)
        elif query_type == 'score':
            result = await self.client.score(
                query_data)
        elif query_type == 'result':
            result = await self.client.result(
                query_data)
        elif query_type == 'sql':
            result = await self.client.sql(query_data)

        return result

    def _translate_column_names(self, json_data):
        """Translate column name to match column name with timestamp.

        Parameters:
        json_data -- JSON data to have the column name translated.

        Return:
        JSON data with new column name.
        """
        data_string = json.dumps(json_data)

        for old_name, new_name in self.column_translation.items():
            data_string = data_string.replace(old_name, new_name)

        return json.loads(data_string)

    async def compare_result(self, query_type, test, result):
        """Compare query expected and received results, exiting if they differ.

        Parameters:
        query_type -- String containing the name of the query that will be
            tested. This name must match the JSON file name as well.
        test -- Dictionary containing test name, columns metadata, data to be
            inserted, query, and expected results.
        result -- Dictionary containing received result after querying
            SlicingDice.
        """
        if self.per_test_insertion:
            expected = self._translate_column_names(test['expected'])
        else:
            expected = test['expected']

        for key, value in expected.items():
            if value == 'ignore':
                continue

            if not isinstance(result, dict) or key not in result:
                self.num_fails += 1
                self.failed_tests.append(test['name'])

                print('  Expected: "{}": {}'.format(key, value))
                print('  Result:   "{}": {}'.format(key, result))
                print('  Status: Failed')
                return

            if not self.compare_values(value, result[key]):
                time.sleep(self.sleep_time * 3)
                query_ = test['query']
                if isinstance(query_, dict):
                    query_.update({"bypass-cache": True})
                try:
                    result2 = await self.execute_query(query_type, test)
                    result2 = ujson.loads(result2)
                    if self.compare_values(value, result2[key]):
                        print("  Passed at second try")
                        continue
                except SlicingDiceException as e:
                    print(str(e))

                self.num_fails += 1
                self.failed_tests.append(test['name'])

                print('  Expected: "{}": {}'.format(key, value))
                print('  Result:   "{}": {}'.format(key, result[key]))
                print('  Status: Failed')
                return

        self.num_successes += 1

        print('  Status: Passed')

    @staticmethod
    def compare_values(expected, result):
        if isinstance(expected, dict):
            if not isinstance(result, dict):
                return False

            if len(expected) != len(result):
                return False

            for key, value_expected in expected.items():
                value_got = result[key]
                if not SlicingDiceTester.compare_values(value_expected,
                                                        value_got):
                    return False

            return True
        if isinstance(expected, list):
            if not isinstance(result, list):
                return False

            if len(expected) != len(result):
                return False

            for i, value in enumerate(expected):
                equal = False
                for j, got in enumerate(result):
                    if SlicingDiceTester.compare_values(value, got):
                        equal = True
                        break

                if not equal:
                    return False

            return True

        return expected == result


async def main():
    # SlicingDice queries to be tested. Must match the JSON file name.
    query_types = [
        'count_entity',
        'count_event',
        'top_values',
        'aggregation',
        'score',
        'result',
        # 'sql'
    ]

    # Testing class with demo API key or one of your API key
    # by enviroment variable
    # http://panel.slicingdice.com/docs/#api-details-api-connection-api-keys-demo-key
    api_key = os.environ.get(
        "SD_API_KEY", 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
                      'eyJfX3NhbHQiOiJkZW1vNzA3OG0iLCJwZXJtaXNzaW9uX2xldmVsIjozLCJwcm9qZWN0X2lkIjoyNzA3OCwiY2xpZW50X2lkIjoxMH0.'
                      'BR7Nm_AMWb0laZ9sgZWt5tDiqvoYk7LPMSQD4URT8Lg')

    # MODE_TEST give us if you want to use endpoint Test or Prod
    sd_tester = SlicingDiceTester(
        api_key=api_key,
        verbose=False)

    try:
        for query_type in query_types:
            await sd_tester.run_tests(query_type)
    except KeyboardInterrupt:
        pass

    print('Results:')
    print('Successes:', sd_tester.num_successes)
    print('Fails:', sd_tester.num_fails)

    for failed_test in sd_tester.failed_tests:
        print('    - {}'.format(failed_test))

    print

    if sd_tester.num_fails > 0:
        is_singular = sd_tester.num_fails == 1
        test_or_tests = 'test has' if is_singular else 'tests have'
        print('FAIL: {} {} failed'.format(sd_tester.num_fails, test_or_tests))
        sys.exit(1)
    else:
        print('SUCCESS: All tests passed')


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
