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
- All the KDB primitive types
- Table and pass through queries
- Basic schema introspection
- Limited filter and limit pass through

### Configuration Options

Settings that can be used in catalog file:

| Config | Description |
| ----- | ----------- |
| kdb.host | Hostname of KDB server | 
| kdb.port | Port of KDB server | 
| kdb.user | _(Optional)_ User for authenticating with KDB server | 
| kdb.password | _(Optional)_ Password for authenticating with KDB server | 
| page.size | _(Optional)_ Size of pages (in number of rows) retrieved from KDB (default: 50,000)

### Building

This library depends on javakdb, which can be built from [GitHub](https://github.com/KxSystems/javakdb). To build:

```mvn package```

The resulting shaded jar then needs to be dropped into `${TRINO_HOME}/plugins/kdb/`.

The unit tests currently require a local instance of KDB running at port 8000.