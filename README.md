## Trino Plugin for kdb+
 
A [Trino](https://trino.io) plugin for [kdb+](https://code.kx.com/q/learn/) - currently in beta state. 
Feedback (bugs, feature requests, questions) welcome!

Sample catalog definition:

```
connector.name=kdb
kdb.host=localhost
kdb.port=8000
```

### Features

The plugin currently supports:
- All the KDB primitive types and nested arrays
- Table and pass through queries
  - Inside dynamic queries upper case letters must be escaped as \\\<letter> since Trino converts all "table names" to lower case
- Basic schema introspection
- Limited filter and limit pass through
  - Experimental support for `like '<pattern>'` push down (see `push.down.like` property). 
    Support is limited: cases with special escape characters, multiple like expressions on the same
    column for example will not be pushed down.
- Aggregation push down for count, sum, max, min, stddev, variance, count_if, bool_and, bool_or
- Supports tables in nested namespaces
- *(Alpha)* Insertion support for plain in-memory tables

#### Fine Print

- KDB columns of unknown type `()` will be mapped to VARCHAR and converted with `Object.toString` (except for char arrays, which convert with `String.valueOf`)

### Configuration Options

Settings that can be used in catalog file:

| Config | Description                                                                                                                                                  |
| ----- |--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kdb.host` | Hostname of KDB server                                                                                                                                       | 
| `kdb.port` | Port of KDB server                                                                                                                                           | 
| `kdb.user` | _(Optional)_ User for authenticating with KDB server                                                                                                         | 
| `kdb.password` | _(Optional)_ Password for authenticating with KDB server                                                                                                     | 
| `page.size` | _(Optional)_ Size of pages (in number of rows) retrieved from KDB (default: 50,000)                                                                          |
| `use.stats` | _(Optional)_ Support stats for KDB either pre-generated or calculated on the fly (see `dynamic.stats`) (default: true)                                       |
| `dynamic.stats` | _(Optional)_ Support on the fly stats generation. Note this can have a detrimental effect on query planning speed for large tables (default: false) |                   
| `kdb.metadata.refresh.interval.seconds` | _(Optional)_ Refresh interval, in seconds, for KDB metadata (default: 3600 = 1 hour)                                                                         |
| `push.down.aggregation` | _(Optional)_ Enable aggregation push down (default: true)                                                                                                    |
| `virtual.tables` | _(Optional)_ Treat all tables as virtual - not supporting features such as direct `select [x]` queries (default: false)                                      |
| `insert.function` | _(Optional)_ Insert function to use to insert data into KDB tables (default: insert)                                                                         |
| `push.down.like` | _(Optional, experimental)_ Push down like filters (default: false)                                                                                           | 

#### Session Property overrides

| Property | Default                                                       |
| -------- |---------------------------------------------------------------|
| `push_down_aggregation` | Session override for catalog property `push.down.aggregation` |
| `use_stats` | Session override for catalog property `use.stats`           |
| `dynamic_stats` | Session override for catalog property `dynamic.stats` |
| `page_size` | Session override for catalog property `page.size`           |
| `virtual_tables` | Session override for catalog property `virtual.tables`      |
| `insert_function` | Session override for catalog property `insert.function`                            |
| `push_down_like` | Session override for catalog property `push.down.like`                             |

### Pre-Generated Stats

Stats generation is still quite raw and can take extremely long for partitioned tables. As an alternatives stats can 
be precomputed and stored in two tables:

```q
.trino.stats:([table: `symbol$()] rowcount: `long$())

.trino.colstats:([table: `symbol$(); column: `symbol$()] 
  distinct_count: `long$(); 
  null_fraction: `double$();
  size: `long$()`; 
  min_value: `double$();
  max_value: `double$())
```

### Building

This library depends on javakdb, which can be built from [GitHub](https://github.com/KxSystems/javakdb). To build:

```mvn package```

The resulting shaded jar then needs to be dropped into `${TRINO_HOME}/plugins/kdb/`.

The unit tests currently require a local instance of KDB running at port 8000.