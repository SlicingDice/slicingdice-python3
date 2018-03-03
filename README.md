# SlicingDice Official Python Client (v2.0.1)
### Build Status: [![CircleCI](https://circleci.com/gh/SlicingDice/slicingdice-python.svg?style=svg)](https://circleci.com/gh/SlicingDice/slicingdice-python)

Official Python client for [SlicingDice](http://www.slicingdice.com/), Data Warehouse and Analytics Database as a Service.  

[SlicingDice](http://www.slicingdice.com/) is a serverless, API-based, easy-to-use and really cost-effective alternative to Amazon Redshift and Google BigQuery.

## Documentation

If you are new to SlicingDice, check our [quickstart guide](https://docs.slicingdice.com/docs/quickstart-guide) and learn to use it in 15 minutes.

Please refer to the [SlicingDice official documentation](https://docs.slicingdice.com/) for more information on [how to create a database](https://docs.slicingdice.com/docs/how-to-create-a-database), [how to insert data](https://docs.slicingdice.com/docs/how-to-insert-data), [how to make queries](https://docs.slicingdice.com/docs/how-to-make-queries), [how to create columns](https://docs.slicingdice.com/docs/how-to-create-columns), [SlicingDice restrictions](https://docs.slicingdice.com/docs/current-restrictions) and [API details](https://docs.slicingdice.com/docs/api-details).

## Tests and Examples

Whether you want to test the client installation or simply check more examples on how the client works, take a look at the [tests and examples directory](tests_and_examples/).

## Installing

In order to install the Python client, you only need to use [`pip`](https://packaging.python.org/installing/).

```bash
pip install pyslicer --extra-index-url=https://packagecloud.io/slicingdice/clients/pypi/simple
```

## Usage

The following code snippet is an example of how to add and query data
using the SlicingDice python client. We entry data informing
`user1@slicingdice.com` has age 22 and then query the database for
the number of users with age between 20 and 40 years old.
If this is the first register ever entered into the system,
 the answer should be 1.

```python
from pyslicer import SlicingDice
import asyncio

# Configure the client
client = SlicingDice(master_key='API_KEY')
loop = asyncio.get_event_loop()

# Inserting data
insert_data = {
    "user1@slicingdice.com": {
        "age": 22
    },
    "auto-create": ["dimension", "column"]
}
loop.run_until_complete(client.insert(insert_data))

# Querying data
query_data = {
    "query-name": "users-between-20-and-40",
    "query": [
        {
            "age": {
                "range": [
                    20,
                    40
                ]
            }
        }
    ]
}
print(loop.run_until_complete(client.count_entity(query_data)))
```

## Reference

`SlicingDice` encapsulates logic for sending requests to the API. Its methods are thin layers around the [API endpoints](https://docs.slicingdice.com/docs/api-details), so their parameters and return values are JSON-like `dict` objects with the same syntax as the [API endpoints](https://docs.slicingdice.com/docs/api-details)

### Attributes

* `keys (str)` - [API key](https://docs.slicingdice.com/docs/api-keys) to authenticate requests with the SlicingDice API.

### Constructor

`__init__(self, write_key=None, read_key=None, master_key=None, custom_key=None, use_ssl=True, timeout=60)`
* `write_key (str)` - [API key](https://docs.slicingdice.com/docs/api-keys) to authenticate requests with the SlicingDice API Write Key.
* `read_key (str)` - [API key](https://docs.slicingdice.com/docs/api-keys) to authenticate requests with the SlicingDice API Read Key.
* `master_key (str)` - [API key](https://docs.slicingdice.com/docs/api-keys) to authenticate requests with the SlicingDice API Master Key.
* `custom_key (str)` - [API key](https://docs.slicingdice.com/docs/api-keys) to authenticate requests with the SlicingDice API Custom Key.
* `use_ssl (bool)` - Define if the requests verify SSL for HTTPS requests.
* `timeout (int)` - Amount of time, in seconds, to wait for results for each request.

### `get_database()`
Get information about current database(related to api keys informed on construction). This method corresponds to a [`GET` request at `/database`](https://docs.slicingdice.com/docs/how-to-list-edit-or-delete-databases).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio
client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()
print(loop.run_until_complete(client.get_database()))
```

#### Output example

```json
{
    "name": "Database 1",
    "description": "My first database",
    "dimensions": [
    	"default",
        "users"
    ],
    "updated-at": "2017-05-19T14:27:47.417415",
    "created-at": "2017-05-12T02:23:34.231418"
}
```

### `get_columns()`
Get all created columns, both active and inactive ones. This method corresponds to a [GET request at /column](https://docs.slicingdice.com/docs/how-to-list-edit-or-delete-columns).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()

print(loop.run_until_complete(client.get_columns()))
```

#### Output example

```json
{
    "active": [
        {
          "name": "Model",
          "api-name": "car-model",
          "description": "Car models from dealerships",
          "type": "string",
          "category": "general",
          "cardinality": "high",
          "storage": "latest-value"
        }
    ],
    "inactive": [
        {
          "name": "Year",
          "api-name": "car-year",
          "description": "Year of manufacture",
          "type": "integer",
          "category": "general",
          "storage": "latest-value"
        }
    ]
}
```

### `create_column(json_data)`
Create a new column. This method corresponds to a [POST request at /column](https://docs.slicingdice.com/docs/how-to-create-columns#section-creating-columns-using-column-endpoint).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()

column = {
    "name": "Year",
    "api-name": "year",
    "type": "integer",
    "description": "Year of manufacturing",
    "storage": "latest-value"
}
print(loop.run_until_complete(client.create_column(column)))
```

#### Output example

```json
{
    "status": "success",
    "api-name": "year"
}
```

### `insert(json_data)`
Insert data to existing entities or create new entities, if necessary. This method corresponds to a [POST request at /insert](https://docs.slicingdice.com/docs/how-to-insert-data).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_WRITE_API_KEY')
loop = asyncio.get_event_loop()

insert_data = {
    "user1@slicingdice.com": {
        "car-model": "Ford Ka",
        "year": 2016
    },
    "user2@slicingdice.com": {
        "car-model": "Honda Fit",
        "year": 2016
    },
    "user3@slicingdice.com": {
        "car-model": "Toyota Corolla",
        "year": 2010,
        "test-drives": [
            {
                "value": "NY",
                "date": "2016-08-17T13:23:47+00:00"
            }, {
                "value": "NY",
                "date": "2016-08-17T13:23:47+00:00"
            }, {
                "value": "CA",
                "date": "2016-04-05T10:20:30Z"
            }
        ]
    },
    "user4@slicingdice.com": {
        "car-model": "Ford Ka",
        "year": 2005,
        "test-drives": {
            "value": "NY",
            "date": "2016-08-17T13:23:47+00:00"
        }
    },
    "auto-create": ["dimension", "column"]
}
print(loop.run_until_complete(client.insert(insert_data)))
```

#### Output example

```json
{
    "status": "success",
    "inserted-entities": 4,
    "inserted-columns": 12,
    "took": 0.023
}
```

### `exists_entity(ids, dimension=None)`
Verify which entities exist in a dimension (uses `default` dimension if not provided) given a list of entity IDs. This method corresponds to a [POST request at /query/exists/entity](https://docs.slicingdice.com/docs/exists).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()

ids = [
    "user1@slicingdice.com",
    "user2@slicingdice.com",
    "user3@slicingdice.com"
]
print(loop.run_until_complete(client.exists_entity(ids)))
```

#### Output example

```json
{
    "status": "success",
    "exists": [
        "user1@slicingdice.com",
        "user2@slicingdice.com"
    ],
    "not-exists": [
        "user3@slicingdice.com"
    ],
    "took": 0.103
}
```

### `count_entity_total()`
Count the number of inserted entities in the whole database. This method corresponds to a [POST request at /query/count/entity/total](https://docs.slicingdice.com/docs/total).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio
client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()

print(loop.run_until_complete(client.count_entity_total()))
```

#### Output example

```json
{
    "status": "success",
    "result": {
        "total": 42
    },
    "took": 0.103
}
```

### `count_entity_total(dimensions)`
Count the total number of inserted entities in the given dimensions. This method corresponds to a [POST request at /query/count/entity/total](https://docs.slicingdice.com/docs/total#section-counting-specific-tables).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()

dimensions = ['default']

print(loop.run_until_complete(client.count_entity_total(dimensions)))
```

#### Output example

```json
{
    "status": "success",
    "result": {
        "total": 42
    },
    "took": 0.103
}
```

### `count_entity(json_data)`
Count the number of entities matching the given query. This method corresponds to a [POST request at /query/count/entity](https://docs.slicingdice.com/docs/count-entities).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()

query = [
    {
        "query-name": "corolla-or-fit",
        "query": [
            {
                "car-model": {
                    "equals": "toyota corolla"
                }
            },
            "or",
            {
                "car-model": {
                    "equals": "honda fit"
                }
            }
        ],
        "bypass-cache": False
    },
    {
        "query-name": "ford-ka",
        "query": [
            {
                "car-model": {
                    "equals": "ford ka"
                }
            }
        ],
        "bypass-cache": False
    }
]
print(loop.run_until_complete(client.count_entity(query)))
```

#### Output example

```json
{
    "status": "success",
    "result": {
        "corolla-or-fit": 2,
        "ford-ka": 2
    },
    "took": 0.049
}
```

### `count_event(json_data)`
Count the number of occurrences for time-series events matching the given query. This method corresponds to a [POST request at /query/count/event](https://docs.slicingdice.com/docs/count-events).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()

query = [
    {
        "query-name": "test-drives-in-ny",
        "query": [
            {
                "test-drives": {
                    "equals": "NY",
                    "between": [
                        "2016-08-16T00:00:00Z",
                        "2016-08-18T00:00:00Z"
                    ]
                }
            }
        ],
        "bypass-cache": True
    },
    {
        "query-name": "test-drives-in-ca",
        "query": [
            {
                "test-drives": {
                    "equals": "CA",
                    "between": [
                        "2016-04-04T00:00:00Z",
                        "2016-04-06T00:00:00Z"
                    ]
                }
            }
        ],
        "bypass-cache": True
    }
]
print(loop.run_until_complete(client.count_event(query)))
```

#### Output example

```json
{
    "status": "success",
    "result": {
        "test-drives-in-ny": 3,
        "test-drives-in-ca": 1
    },
    "took": 0.046
}
```

### `top_values(json_data)`
Return the top values for entities matching the given query. This method corresponds to a [POST request at /query/top_values](https://docs.slicingdice.com/docs/top-values).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()
query = {
    "car-year": {
        "year": 2
    },
    "car models": {
        "car-model": 3
    }
}
print(loop.run_until_complete(client.top_values(query)))
```

#### Output example

```json
{
    "result": {
        "car models": {
            "car-model": [
                {
                    "quantity": 2,
                    "value": "ford ka"
                },
                {
                    "quantity": 1,
                    "value": "honda fit"
                },
                {
                    "quantity": 1,
                    "value": "toyota corolla"
                }
            ]
        },
        "car-year": {
            "year": [
                {
                    "quantity": 2,
                    "value": "2016"
                },
                {
                    "quantity": 1,
                    "value": "2010"
                }
            ]
        }
    },
    "took": 0.034,
    "status": "success"
}
```

### `aggregation(json_data)`
Return the aggregation of all columns in the given query. This method corresponds to a [POST request at /query/aggregation](https://docs.slicingdice.com/docs/aggregations).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()
query = {
    "query": [
        {
            "car-model": 2,
            "equals": [
                "honda fit",
                "toyota corolla"
            ]
        }
    ]
}
print(loop.run_until_complete(client.aggregation(query)))
```

#### Output example

```json
{
    "result": {
        "year": [
            {
                "quantity": 2,
                "value": "2016",
                "car-model": [
                    {
                        "quantity": 1,
                        "value": "honda fit"
                    }
                ]
            },
            {
                "quantity": 1,
                "value": "2005"
            }
        ]
    },
    "took":0.079,
    "status":"success"
}
```

### `get_saved_queries()`
Get all saved queries. This method corresponds to a [GET request at /query/saved](https://docs.slicingdice.com/docs/saved-queries).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()
print(loop.run_until_complete(client.get_saved_queries()))
```

#### Output example

```json
{
    "status": "success",
    "saved-queries": [
        {
            "name": "users-in-ny-or-from-ca",
            "type": "count/entity",
            "query": [
                {
                    "state": {
                        "equals": "NY"
                    }
                },
                "or",
                {
                    "state-origin": {
                        "equals": "CA"
                    }
                }
            ],
            "cache-period": 100
        }, {
            "name": "users-from-ca",
            "type": "count/entity",
            "query": [
                {
                    "state": {
                        "equals": "NY"
                    }
                }
            ],
            "cache-period": 60
        }
    ],
    "took": 0.103
}
```

### `create_saved_query(json_data)`
Create a saved query at SlicingDice. This method corresponds to a [POST request at /query/saved](https://docs.slicingdice.com/docs/saved-queries).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()
query = {
    "name": "my-saved-query",
    "type": "count/entity",
    "query": [
        {
            "car-model": {
                "equals": "honda fit"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        }
    ],
    "cache-period": 100
}
print(loop.run_until_complete(client.create_saved_query(query)))
```

#### Output example

```json
{
    "status": "success",
    "name": "my-saved-query",
    "type": "count/entity",
    "query": [
        {
            "car-model": {
                "equals": "honda fit"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        }
    ],
    "cache-period": 100,
    "took": 0.103
}
```

### `update_saved_query(query_name, json_data)`
Update an existing saved query at SlicingDice. This method corresponds to a [PUT request at /query/saved/QUERY_NAME](https://docs.slicingdice.com/docs/saved-queries).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()
new_query = {
    "type": "count/entity",
    "query": [
        {
            "car-model": {
                "equals": "honda fit"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        }
    ],
    "cache-period": 100
}
print(loop.run_until_complete(client.update_saved_query('my-saved-query', new_query)))
```

#### Output example

```json
{
    "status": "success",
    "name": "my-saved-query",
    "type": "count/entity",
    "query": [
        {
            "car-model": {
                "equals": "honda fit"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        }
    ],
    "cache-period": 100,
    "took": 0.103
}
```

### `get_saved_query(query_name)`
Executed a saved query at SlicingDice. This method corresponds to a [GET request at /query/saved/QUERY_NAME](https://docs.slicingdice.com/docs/saved-queries).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()
print(loop.run_until_complete(client.get_saved_query('my-saved-query')))
```

#### Output example

```json
{
    "status": "success",
    "type": "count/entity",
    "query": [
        {
            "car-model": {
                "equals": "honda fit"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        }
    ],
    "result": {
        "my-saved-query": 2
    },
    "took": 0.043
}
```

### `delete_saved_query(query_name)`
Delete a saved query at SlicingDice. This method corresponds to a [DELETE request at /query/saved/QUERY_NAME](https://docs.slicingdice.com/docs/saved-queries).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_API_KEY')
loop = asyncio.get_event_loop()
print(loop.run_until_complete(client.delete_saved_query('my-saved-query')))
```

#### Output example

```json
{
    "status": "success",
    "deleted-query": "my-saved-query",
    "type": "count/entity",
    "query": [
        {
            "car-model": {
                "equals": "honda fit"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        }
    ],
    "took": 0.043
}
```

### `result(json_data)`
Retrieve inserted values for entities matching the given query. This method corresponds to a [POST request at /data_extraction/result](https://docs.slicingdice.com/docs/result-extraction).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()
query = {
    "query": [
        {
            "car-model": {
                "equals": "ford ka"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "honda fit"
            }
        }
    ],
    "columns": ["car-model", "year"],
    "limit": 2
}
print(loop.run_until_complete(client.result(query)))
```

#### Output example

```json
{
    "status": "success",
    "data": {
        "customer5@mycustomer.com": {
            "year": "2005",
            "car-model": "ford ka"
        },
        "user1@slicingdice.com": {
            "year":"2016",
            "car-model": "ford ka"
        }
    },
    "page": 1,
    "took": 0.053
}
```

### `score(json_data)`
Retrieve inserted values as well as their relevance for entities matching the given query. This method corresponds to a [POST request at /data_extraction/score](https://docs.slicingdice.com/docs/score-extraction).

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()
query = {
    "query": [
        {
            "car-model": {
                "equals": "toyota corolla"
            }
        },
        "or",
        {
            "car-model": {
                "equals": "honda fit"
            }
        }
    ],
    "columns": ["car-model", "year"],
    "limit": 2
}
print(loop.run_until_complete(client.score(query)))
```

#### Output example

```json
{
    "status": "success",
    "data": {
        "user3@slicingdice.com": {
            "score": 1,
            "year": "2010",
            "car-model": "toyota corolla"
        },
        "user2@slicingdice.com": {
            "score": 1,
            "year": "2016",
            "car-model": "honda fit"
        }
    },
    "page": 1,
    "next-page": null,
    "took": 0.063
}
```

### `sql(query)`
Retrieve inserted values using a SQL syntax. This method corresponds to a POST request at /query/sql.

#### Request example

```python
from pyslicer import SlicingDice
import asyncio

client = SlicingDice('MASTER_OR_READ_API_KEY')
loop = asyncio.get_event_loop()
query = "SELECT COUNT(*) FROM default WHERE age BETWEEN 0 AND 49"
print(loop.run_until_complete(client.sql(query)))
```

#### Output example

```json
{
   "took":0.063,
   "result":[
       {"COUNT": 3}
   ],
   "count":1,
   "status":"success"
}
```

## License

[MIT](https://opensource.org/licenses/MIT)
# slicingdice-python3
Official Python 3 client for SlicingDice, Data Warehouse and Analytics Database as a Service.
