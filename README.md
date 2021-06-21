## Trino Plugin for kdb+
 
A simple [Trino](https://trino.io) plugin for [kdb+](https://code.kx.com/q/learn/) - currently in very alpha state. 

A sample catalog definition should look like:

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
- Aggregation push down for count, sum, max, min, stddev, variance, count_if, bool_and, bool_or

#### Fine Print

- KDB columns of unknown type `()` will be mapped to VARCHAR and converted with `Object#toString`.

### Configuration Options

Settings that can be used in catalog file:

| Config | Description |
| ----- | ----------- |
| `kdb.host` | Hostname of KDB server | 
| `kdb.port` | Port of KDB server | 
| `kdb.user` | _(Optional)_ User for authenticating with KDB server | 
| `kdb.password` | _(Optional)_ Password for authenticating with KDB server | 
| `page.size` | _(Optional)_ Size of pages (in number of rows) retrieved from KDB (default: 50,000) |
| `use.stats` | _(Optional)_ Calculate stats for KDB tables, on the fly and cached in memory (default: true) |
| `kdb.metadata.refresh.interval.seconds` | _(Optional)_ Refresh interval, in seconds, for KDB metadata (default: 3600 = 1 hour) |
| `push.down.aggregation` | _(Optional)_ Enable aggregation push down (default: true) |

#### Session Property overrides

| Property | Default |
| -------- | ------- |
| `push_down_aggregation` | Catalog property `push.down.aggregation` |
| `use_stats` | Catalog property `use.stats` |

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