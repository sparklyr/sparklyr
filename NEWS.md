# Sparklyr 0.5.0 (UNRELEASED)

- Added support to parameterize network timeouts using the
  sparklyr.monitor.timeout and sparklyr.backend.timeout config settings.

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
