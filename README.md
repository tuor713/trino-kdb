## Trino Plugin for kdb+
 
A simple [Trino](https://trino.io) plugin for [kdb+](https://code.kx.com/q/learn/) - currently in very alpha state. 

A sample catalog definition should look like:

```
connector.name=kdb
kdb.host=localhost
kdb.port=8000
kdb.user=<user>
kdb.password=<password>
```

### Features

The plugin currently supports:
- All the KDB primitive types
- Table and passthrough queries
- Basic schema introspection
- Limited filter pass through

### Building

This library depends on javakdb, which can be built from [GitHub](https://github.com/KxSystems/javakdb).

The unit tests currently require a local instance of KDB running at port 8000 with schema:

```
atable:([] name:`Dent`Beeblebrox`Prefect; iq:98 42 126)

btable:([] 
  booleans:001b; 
  guids: 3?0Ng; 
  bytes: `byte$1 2 3; 
  shorts: `short$1 2 3; 
  ints: `int$1 2 3; 
  longs: `long$1 2 3; 
  reals: `real$1 2 3; 
  floats: `float$1 2 3; 
  chars:"abc"; 
  strings:("hello"; "world"; "trino"); 
  symbols:`a`b`c; 
  timestamps: `timestamp$1 2 3; 
  months: `month$1 2 3; 
  dates: `date$1 2 3; 
  datetimes: `datetime$1 2 3; 
  timespans: `timespan$1 2 3; 
  minutes: `minute$1 2 3; 
  seconds: `second$1 2 3; 
  times: `time$1 2 3 )

ctable:([] const:1000000#1; linear:til 1000000)
```
