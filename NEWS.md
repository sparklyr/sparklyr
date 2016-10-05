# Sparklyr 0.5.0 (UNRELEASED)

- Added support to configure spark environment variables using `spark.env.*` config.

- Added support for the `Tokenizer` and `RegexTokenizer` feature transformers.
  These are exported as the `ft_tokenizer()` and `ft_regex_tokenizer()` functions.

- Resolved an issue where attempting to call `copy_to()` with an R `data.frame`
  containing many columns could fail with a Java StackOverflow. (#244)

- Resolved an issue where attempting to call `collect()` on a Spark DataFrame
  containing many columns could produce the wrong result. (#242)

- Added support to parameterize network timeouts using the
  `sparklyr.backend.timeout`, `sparklyr.gateway.start.timeout` and
  `sparklyr.gateway.connect.timeout` config settings.

- Improved logging while establishing connections to `sparklyr`.

- Added `sparklyr.gateway.port` and `sparklyr.gateway.address` as config settings.

- Added eclipse project to ease development of the scala codebase within 
  `sparklyr`.

- Added `filter` parameter to `spark_log` to fitler with ease entries by a character
  string.

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
