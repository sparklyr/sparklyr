# Sparklyr 0.5.0 (UNRELEASED)

- Added support to `spark_connect` remotely using `master = "sparklyr://address:port"`
  for `sparklyr` backends launched in service mode and with the config
  `sparklyr.service.remote` set to `TRUE`
  
- Added `sparklyr.service.remote` config to allow remote connections into the
  sparklyr gateway, disabled by default.

- Added support to parameterize network timeouts using the
  `sparklyr.backend.timeout`, `sparklyr.gateway.start.timeout`,
  `sparklyr.gateway.local.timeout` and `sparklyr.gateway.remote.timeout`
  config settings.

- Added support in `spark_connect` with `mode="gateway"` to access a remote
  `sparklyr` backend.

- Added terminate parameter to `spark_disconnect` and `spark_disconnect_all`
  to terminate `sparklyr` service mode.

- Improved logging while establishing connections to `sparklyr`.

- Added `sparklyr.gateway.port`, `sparklyr.gateway.address` and `sparklyr.service`
  as config settings.

- Added eclipse project to ease development of the scala codebase within 
  `sparklyr`.

- Added `filter` parameter to `spark_log` to fitler with ease entries by a character
  string.

- Added support for `spark_connect(service = TRUE)` to run `sparklyr` backend
  as a long running service that supports reconnection, multiple clients
  and remote access.

- Increased network timeout for sparklyr.backend.timeout.

- Moved `spark.jars.default` setting from options to spark config.

- `sparklyr` now properly respects the Hive metastore directory with the
  `sdf_save_table()` and `sdf_load_table()` APIs for Spark < 2.0.0.

- Added `sdf_quantile()` as a means of computing (approximate) quantiles
  for a column of a Spark DataFrame.

- Added support for `n_distinct(...)`, based on call to Hive function
  `count(DISTINCT ...)`. (#220)

# Sparklyr 0.4.0

- First release to CRAN.
