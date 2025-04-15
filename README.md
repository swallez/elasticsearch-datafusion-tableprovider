# elasticsearch-datafusion-tableprovider

A [DataFusion table provider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
that loads Elasticsearch indexes as DataFusion tables, and transforms DataFusion queries to
[ES|QL](https://www.elastic.co/guide/en/elasticsearch/reference/master/esql.html) queries.

**Note**: this is an experiment, mapping from Elasticsearch types to DataFusion is incomplete, and
there is some plan optimizations to be done to push more processing down to Elasticsearch.

Use it as follows:
```
CREATE EXTERNAL TABLE some_index STORED AS ELASTICSEARCH LOCATION <url> OPTIONS(<options>)
```

Where `<url>` can be either a http URL, or an `env:<ES_NAME>` URL that will cause the table
configuration to be read from environment variables.

When using a http URL, the following options can be provided to configure authentication:
* `api_key`: use API key authentication,
* `token`: use bearer token authentication,
* `user` & `password`: basic authentication.

When using an env url, the configuration is read from environment variables. Assuming `env:PRODUCTS` url:
* `ELASTICSEARCH_PRODUCTS_URL`: Elasticsearch server URL,
* `ELASTICSEARCH_PRODUCTS_API_KEY`: API key authentication (other schemes aren't implemented yet for env URLs)


License: Apache-2
