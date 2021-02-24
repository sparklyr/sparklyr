# Sparklyr 1.5.2.9001

### Data

- Made `sparklyr` compatible with both dbplyr edition 1 and edition 2 APIs

- Revised `sparklyr`'s integration with `dbplyr` API so that `dplyr::select()`,
  `dplyr::mutate()`, and `dplyr::summarize()` verbs on Spark dataframes
  involving `where()` predicates can be correctly translated to Spark SQL
  (e.g., one can have `sdf %>% select(where(is.numeric))` and
  `sdf %>% summarize(across(starts_with("Petal"), mean))`, etc)

- Implemented `dplyr::if_all()` and `dplyr::if_any()` support for Spark
  dataframes

- Added support for `partition_by` option in `stream_write_*` methods

- Fixed a bug with URI handling affecting all `spark_read_*` methods

- Avoided repeated creations of SimpleDataFormat objects and setTimeZone calls
  while collecting Data columns from a Spark dataframe

- Schema specification for struct columns in `spark_read_*()` methods are now
  supported (e.g.,
  `spark_read_json(sc, path, columns = list(s = list(a = "integer, b = "double")))`
  says expect a struct column named `s` with each element containing a field
  named `a` and a field named `b`)

- `sdf_quantile()` and `ft_quantile_discretizer()` now support approximation of
  weighted quantiles using a modified version of the Greenwald-Khanna algorithm
  that takes relative weight of each data point into consideration.

### Serialization

- `spark_write_rds()` was implemented to support exporting all partitions of a
  Spark dataframe in parallel into RDS (version 2) files. Such RDS files will be
  written to the default file system of the Spark instance (i.e., local file if
  the Spark instance is running locally, or a distributed file system such as
  HDFS if the Spark instance is deployed over a cluster). The resulting RDS
  files, once downloaded onto the local file system, should be deserialized into
  R dataframes using `collect_from_rds()` (which calls `readRDS()` internally
  and also performs some important post-processing steps to support timestamp
  columns, date columns, and struct columns properly in R).

- `copy_to()` can now import list columns of temporal values within a R
  dataframe as arrays of Spark SQL date/timestamp types when working with Spark
  3.0 or above

- Fixed a bug with `copy_to()`'s handling of NA values in list columns of a R
  dataframe

- Spark map type will be collected as list instead of environment in R in order
  to support empty string as key

### Spark ML

- Implemented R interface for Power Iteration Clustering

- The `handle_invalid` option is added to `ft_vector_indexer()` (supported by
  Spark 2.3 or above)

### Misc

- Fixed a bug with `~` within some path components not being normalized in
  `sparklyr::livy_install()`

# Sparklyr 1.5.2

### Connections

- Fixed `op_vars()` specification in `dplyr::distinct()` verb for Spark
  dataframes

- `spark_disconnect()` now closes the Spark monitoring connection correctly

### Data

- Implement support for stratified sampling in `ft_dplyr_transformer()`

- Added support for `na.rm` in dplyr `rowSums()` function for Spark dataframes

# Sparklyr 1.5.1

### Connections

- A bug in how multiple `--conf` values were handled in some scenarios within
  the spark-submit shell args which was introduced in sparklyr 1.4 has been
  fixed now.

- A bug with `livy.jars` configuration was fixed (#2843)

### Data

- `tbl()` methods were revised to be compatible with `dbplyr` 2.0 when handling
  inputs of the form `"<schema name>.<table name>"`

# Sparklyr 1.5.0

### Connections

- `spark_web()` has been revised to work correctly in environments such as
  RStudio Server or RStudio Cloud where the Spark web UI URLs such as
  "http://localhost:4040/jobs/" needs to be translated with
  `rstudioapi::translateLocalUrl()` to be accessible.

- The problem with bundle file name collisions when `session_id` is not provided
  has been fixed in `spark_apply_bundle()`.

- Support for `sparklyr.livy.sources` is removed completely as it is no longer
  needed as a workaround when Spark version is specified.

### Data

- `stream_lag()` is implemented to provide the equivalent functionality of
  `dplyr::lag()` for streaming Spark dataframes while also supporting additional
  filtering of "outdated" records based on timestamp threshold.

- A specialized version of `dplyr::distinct()` is implemented for Spark
  dataframes that supports `.keep_all = TRUE` and correctly satisfies the "rows
  are a subset of the input but appear in the same order" requirement stated in
  the `dplyr` documentation.

- The default value for the `repartition` parameter of `sdf_seq()` has been
  corrected.

- Some implementation detail was revised to make `sparklyr` 1.5 fully compatible
  with `dbplyr` 2.0.

- `sdf_expand_grid()` was implemented to support roughly the equivalent of
  `expand.grid()` for Spark dataframes while also offering additional Spark-
  specific options such as broadcast hash joins, repartitioning, and caching of
  the resulting Spark dataframe in memory.

- `sdf_quantile()` now supports calculation for multiple columns.

- Both `lead()` and `lag()` methods for dplyr interface of `sparklyr` are fixed
  to correctly accept the `order_by` parameter.

- The `cumprod()` window aggregation function for dplyr was reimplemented to
  correctly handle null values in Spark dataframes.

- Support for `missing` parameter is implemented for the `ifelse()`/`if_else()`
  function for dplyr.

- A `weighted.mean()` summarizer was implemented for dplyr interface of
  `sparklyr`.

- A workaround was created to ensure `NA_real_` is handled correctly within the
  contexts of `dplyr::mutate()` and `dplyr::transmute()` methods (e.g.,
  `sdf %>% dplyr::mutate(z = NA_real_)` should result in a column named "z" with
  double-precision SQL type)

- Support for R-like subsetting operator (`[`) was implemented for selecting a
  subset of columns from a Spark dataframe.

- The `rowSums()` function was implemented for dplyr interface of `sparklyr`.

- The `sdf_partition_sizes()` function was created to enable efficient query of
  partition sizes within a Spark dataframe.

- Stratified sampling for Spark dataframes has been implemented and can be
  expressed using dplyr grammar as
  `<spark dataframe> %>% dplyr::group_by(<columns>) %>% dplyr::sample_n(...)` or
  `<spark dataframe> %>% dplyr::group_by(<columns>) %>% dplyr::sample_frac(...)`
  where `<columns>` is a list of grouping column(s) defining the strata
  (i.e., the sampling specified by `dplyr::sample_n()` or `dplyr::sample_frac()`
  will be applied to each group defined by `dplyr::group_by(<columns>)`)

- The implementations of `dplyr::sample_n()` and `dplyr::sample_frac()` have
  been revised to first perform aggregations on individual partitions before
  merging aggregated results from all partitions, which is more efficient than
  `mapPartitions()` followed by `reduce()`.

- `sdf_unnest_longer()` and `sdf_unnest_wider()` were implemented and offer
  the equivalents of `tidyr::unnest_longer()` and `tidyr::unnest_wider()` for
  for Spark dataframes.

### Serialization

- `copy_to()` now serializes R dataframes into RDS format instead of CSV format
  if `arrow` is unavailable. RDS serialization is approximately 48% faster than
  CSV and allows multiple correctness issues related to CSV serialization to be
  fixed easily in `sparklyr`.

- `copy_to()` and `collect()` now correctly preserve `NA_real_` (`NA_real_` from
  a R dataframe, once translated as `null` in a Spark dataframe, used to be
  incorrectly collected as `NaN` in previous versions of `sparklyr`).

- `copy_to()` can now distinguish `"NA"` from `NA` as expected.

- `copy_to()` now supports importing binary columns from R dataframes to Spark.

- Reduced serialization overhead in Spark-based `foreach` parallel backend
  created with `registerDoSpark()`.

# Sparklyr 1.4.0

### Connections

- RAPIDS GPU acceleration plugin can now be enabled with
  `spark_connect(..., package = "rapids")` and configured with `spark_config`
  options prefixed with "spark.rapids."

- Enabled support for http{,s} proxy plus additional CURL options for Livy
  connections

- In sparklyr error message, suggest `options(sparklyr.log.console = TRUE)` as a
  trouble-shooting step whenever the "sparklyr gateway not responding" error
  occurs

- Addressed an inter-op issue with Livy + Spark 2.4
  (https://github.com/sparklyr/sparklyr/issues/2641)

- Added configurable retries for Gateway ports query
  (https://github.com/sparklyr/sparklyr/pull/2654)

- App name setting now takes effect as expected in YARN cluster mode
  (https://github.com/sparklyr/sparklyr/pull/2675)

### Data

- Support for newly introduced higher-order functions in Spark 3.0 (e.g.,
  `array_sort`, `map_filter`, `map_zip_with`, and many others)

- Implemented parallelizable weighted sampling methods for sampling from a Spark
  data frames with and without replacement using exponential variates

- Replaced `dplyr::sample_*` implementations based on `TABLESAMPLE` with
  alternative implementation that can return exactly the number of rows or
  fraction specified and also properly support sampling with-replacement,
  without-replacement, and repeatable sampling use cases

- All higher-order functions and sampling methods are made directly accessible
  through `dplyr` verbs

- Made `grepl` part of the `dplyr` interface for Spark data frames

- Tidyr verbs such as `pivot_wider`, `pivot_longer`, `nest`, `unnest`,
  `separate`, `unite`, and `fill` now have specialized implementations in
  `sparklyr` for working with Spark data frames

- Made `dplyr::inner_join`, `dplyr::left_join`, `dplyr::right_join`, and
  `dplyr::full_join` replace `'.'` with `'_'` in `suffix` parameter when working
  with Spark data frames (https://github.com/sparklyr/sparklyr/issues/2648)

### Distributed R

- Fixed an issue with global variables in `registerDoSpark`
  (https://github.com/sparklyr/sparklyr/pull/2608)

- Revised `spark_read_compat_param` to avoid collision on names assigned to
  different Spark data frames

### Misc

- Fixed a rendering issue with HTML reference pages

- Made test reporting in Github CI workflows more informative
  (https://github.com/sparklyr/sparklyr/pull/2672)

### Spark ML

- `ft_robust_scaler` was created as the R interface for the `RobustScaler`
  functionality in Spark 3 or above

# Sparklyr 1.3.1

### Distributed R

- Fixed a bug in ordering of parameters for a lamba expression when the lambda
  expression passed to a `hof_*` method is specified with a R formula and the
  lambda takes 2 parameters

# Sparklyr 1.3.0

### Spark ML

- `ml_evaluate()` methods are implemented for ML clustering and classification models

### Distributed R

- Created helper methods to integrate Spark SQL higher-order functions with
  `dplyr::mutate`

- Implemented option to pass partition index as a named parameter to `spark_apply()`
  transform function

- Enabled transform function of `spark_apply()` to return nested lists

- Added option to return R objects instead of Spark data frame rows from transform
  function of `spark_apply`

- `sdf_collect()` now supports fetching Spark data frame row-by-row rather than
  column-by-column, and fetching rows using iterator instead of collecting all
  rows into memory

- Support for `partition` when using barrier execution in `spark_apply` (#2454)

### Connections

- Sparklyr can now connect with Spark 2.4 built with Scala 2.12 using
  `spark_connect(..., scala_version = "2.12")`

- Hive integration can now be disabled by configuration in `spark_connect()` (#2465)

- A JVM object reference counting bug affecting secondary Spark connections was fixed
  (#2515)

- Revised JObj envs initialization for Databricks connections (#2533)

### Serialization

- Timezones, if present in data, are correctly represented now in Arrow serialization

- Embedded nul bytes are removed from strings when reading strings from Spark to R
  (#2250)

- Support to collect objectts of type `SeqWrapper` (#2441)

### Data

- Created helper methods to integrate Spark SQL higher-order functions with
  `dplyr::mutate`

- New `spark_read()` method to allow user-defined R functions to be run
  on Spark workers to import data into a Spark data frame

- `spark_write()` method is implemented allow user-defined functions to be run on
  Spark workers to export data from a Spark data frame

- Avro functionalities such as `spark_read_avro()`, `spark_write_avro()`,
  `sdf_from_avro()`, and `sdf_to_avro()` are implemented and can be optionally
  enabled with `spark_connect(..., package = "avro")`

### Extensions

- Fixed a bug where Spark package repositories specification was not honored by
  `spark_dependency()`. The `repositories` parameter of `spark_dependency()` now
  works as expected.

### Misc

- Fixed warnings for deprecated functions (#2431)

- More test coverage for Databricks Connect and Databricks Notebook modes

- Embedded R sources are now included as resources rather than as a Scala string
  literal in `sparklyr-*.jar` files, so that they can be updated without
  re-compilation of Scala source files

- A mechanism is created to verify embedded sources in `sparklyr-*.jar` files
  are in-sync with current R source files and this verification is now part of
  the Github CI workflow for `sparklyr`

# Sparklyr 1.2.0

### Distributed R

- Add support for using Spark as a foreach parallel backend

- Fixed a bug with how `columns` parameter was interpreted in `spark_apply`

### Data

- Allow `sdf_query_plan` to also get analyzed plan

- Add support for serialization of R date values into corresponding Hive date
  values

- Fixed the issue of date or timestamp values representing the UNIX epoch
  (1970-01-01) being deserialized incorrectly into NAs

- Better support for querying and deserializing Spark SQL struct columns when
  working with Spark 2.4 or above

- Add support in `copy_to()` for columns with nested lists (#2247).

- Significantly improve `collect()` performance for columns with nested
  lists (#2252).

### Connection

- Add support for Databricks Connect

- Add support for `copy_to` in Databricks connection

- Ensure spark apply bundle files created by multiple Spark sessions don't
  overwrite each other

- Fixed an interop issue with spark-submit when running with Spark 3 preview

- Fixed an interop issue with Sparklyr gateway connection when running with
  Spark 3 preview

- Fixed a race condition of JVM object with refcount 1 being removed from JVM
  object tracker before pending method invocation(s) on them could be initiated
  (NOTE: previously this would only happen when the R process was running under
   high memory pressure)

- Allow a chain of JVM method invocations to be batched into 1 `invoke` call

- Removal of unneeded objects from JVM object tracker no longer blocks
  subsequent JVM method invocations

- Add support for JDK11 for Spark 3 preview.

### Misc

- Support for installing Spark 3.0 Preview 2.

- Emit more informative error message if network interface required for
  `spark_connect` is not up

- Fixed a bug preventing more than 10 rows of a Spark table to be printed from R

- Fixed a spelling error in `print` method for `ml_model_naive_bayes` objects

- Made `sdf_drop_duplicates` an exported function (previously it was not
  exported by mistake)

- Fixed a bug in `summary()` of `ml_linear_regression`

# Sparklyr 1.1.0

### Distributed R

- Add support for barrier execution mode with `barrier = TRUE` in `spark_apply()` (@samuelmacedo83, #2216).

### Streaming

- Add support for `stream_read_delta()` and `stream_write_delta()`.

- Fixed typo in `stream_read_socket()`.

### Data

- Allow using Scala types in schema specifications. For example, `StringType` in the
  `columns` parameter for `spark_read_csv()` (@jozefhajnala, #2226)

- Add support for `DBI 1.1` to implement missing `dbQuoteLiteral` signature (#2227).

### Livy

- Add support for Livy 0.6.0.

- Deprecate uploading sources to Livy, a jar is now always used and the `version`
  parameter in `spark_connect()` is always required.

- Add config `sparklyr.livy.branch` to specify the branch used for the sparklyr JAR.

- Add config `sparklyr.livy.jar` to configure path or URL to sparklyr JAR.

# Data

- Add support for `partition_by` when using `spark_write_delta()` (#2228).

# Sparklyr 1.0.5

### Serialization

- R environments are now sent to Scala Maps rather than `java.util.Map[Object, Object]` (#1058).

### Data

- Allow `sdf_sql()` to accept glue strings (@yutannihilation, #2171).

- Support to read and write from Delta Lake using `spark_read_delta()`
  and `spark_write_delta()` (#2148).

### Connections

- `spark_connect()` supports new `packages` parameter to easily
  enable `kafka` and `delta` (#2148).

- `spark_disconnect()` returns invisibly (#2028).

### Configuration

- Support to specify config file location using the
  `SPARKLYR_CONFIG_FILE` environment variable (@AgrawalAmey, #2153).

### Compilation

- Support for Scala 12 (@lu-wang-dl, #2154).

### YARN

- Fix `curl_fetch_memory` error when using YARN Cluster mode (#2157).

# Sparklyr 1.0.4

### Arrow

- Support for Apache Arrow 0.15 (@nealrichardson, #2132).

# Sparklyr 1.0.3

### Kuberenetes

- Support for port forwarding in Windows using RStudio terminal.

### dplyr

- Fix support for `compute()` in Spark 1.6 (#2099)

### Data

- The `spark_read_()` functions now support multiple parameters (@jozefhajnala, #2118).

### Connections

- Fix for Qubole connections for single user and multiple sessions (@vipul1409, #2128).

# Sparklyr 1.0.2

### Connections

- Support for Qubole connections using `mode = "quobole"` (@vipul1409, #2039).

### Extensions

- When `invoke()` fails due to mismatched parameters, warning with info is logged.

### RStudio

- Spark UI path can now be accessed even when the R session and Spark are bussy.

### Distributed

- Configuration setting `sparklyr.apply.serializer` can be used to select serializer version in `spark_apply()`.

- Fix for `spark_apply_log()` and use `RClosure` as logging component.

### ML

- `ml_corr()` retrieve a `tibble` for better formatting.

### Misc

- Support for Spark 2.3.3 and 2.4.3.

### Data

- The `infer_schema` parameter now defaults to `is.null(column)`.

- The `spark_read_()` functions support loading data with named `path`
  but no explicit `name`.

# Sparklyr 1.0.1

### ML

- `ml_lda()`: Allow passing of optional arguments via `...` to regex tokenizer, stop words remover, and count vectorizer components in the formula API.

- Implemented `ml_evaluate()` for logistic regression, linear regression, and GLM models.

- Implemented `print()` method for `ml_summary` objects.

- Deprecated `compute_cost()` for KMeans in Spark 2.4 (#1772).

- Added missing internal constructor for clustering evaluator (#1936).

- `sdf_partition()` has been renamed to `sdf_random_split()`.

- Added `ft_one_hot_encoder_estimator()` (#1337).

### Misc

- Added `sdf_crosstab()` to create contingency tables.

- Fix `tibble::as.tibble()` deprecation warning.

### Connections

- Reduced default memory for local connections when Java x64 is not installed (#1931).

### Batches

- Add support in `spark-submit` with R file to pass additional arguments to R file (#1942).

### Distributed R

- Fix support for multiple library paths when using `spark.r.libpaths` (@mattpollock, #1956).

### Extensions

- Support for creating an Spark extension package using `spark_extension()`.

- Add support for repositories in `spark_dependency()`.

### DataFrames

- Fix `sdf_bind_cols()` when using `dbplyr` 1.4.0.

### Kubernetes

- Fix regression in `spark_config_kubernetes()` configuration helper.

# Sparklyr 1.0.0

### Arrow

- Support for Apache Arrow using the `arrow` package.

### ML

- The `dataset` parameter for estimator feature transformers has been deprecated (#1891).

- `ml_multilayer_perceptron_classifier()` gains probabilistic classifier parameters (#1798).

- Removed support for all undocumented/deprecated parameters. These are mostly dot case parameters from pre-0.7.

- Remove support for deprecated `function(pipeline_stage, data)` signature in `sdf_predict/transform/fit` functions.

- Soft deprecate `sdf_predict/transform/fit` functions. Users are advised to use `ml_predict/transform/fit` functions instead.

- Utilize the ellipsis package to provide warnings when unsupported arguments are specified in ML functions.

### Livy

- Support for sparklyr extensions when using Livy.

- Significant performance improvements by using `version` in
  `spark_connect()` which enables using the sparklyr JAR rather than
  sources.

- Improved memory use in Livy by using string builders and avoid print
  backs.

### Data

- Fix for `DBI::sqlInterpolate()` and related methods to properly
  quote parameterized queries.

- `copy_to()` names tables `sparklyr_tmp_` instead of `sparklyr_` for
  consistency with other temp tables and to avoid rendering them under
  the connections pane.

- `copy_to()` and `collect()` are not re-exported since they are commonly
  used even when using `DBI` or outside data analysis use cases.

- Support for reading `path` as the second parameter in `spark_read_*()`
  when no name is specified (e.g. `spark_read_csv(sc, "data.csv")`).

- Support for batches in `sdf_collect()` and `dplyr::collect()` to retrieve
  data incrementally using a callback function provided through a
  `callback` parameter. Useful when retrieving larger datasets.

- Support for batches in `sdf_copy_to()` and `dplyr::copy_to()` by passing
  a list of callbacks that retrieve data frames. Useful when uploading
  larger datasets.

- `spark_read_source()` now has a `path` parameter for specifying file path.

- Support for `whole` parameter for `spark_read_text()` to read an
  entire text file without splitting contents by line.

### Broom

- Implemented `tidy()`, `augment()`, and `glance()` for `ml_lda()`and `ml_als()` models (@samuelmacedo83)

### Connections

- Local connection defaults now to 2GB.

- Support to install and connect based on major Spark versions, for
  instance: `spark_connect(master = "local", version = "2.4")`.

- Support for installing and connecting to Spark 2.4.

### Serialization

- Faster retrieval of string arrays.

### YARN

- New YARN action under RStudio connection pane extension to launch YARN
  UI. Configurable through the `sparklyr.web.yarn` configuration setting.

- Support for property expansion in `yarn-site.xml` (@lgongmsft, #1876).

## Distributed R

- The `memory` parameter in `spark_apply()` now defaults to `FALSE` when
  the `name` parameter is not specified.

## Other

- Removed dreprecated `sdf_mutate()`.

- Remove exported `ensure_` functions which were deprecated.

- Fixed missing Hive tables not rendering under some Spark
  distributions (#1823).

- Remove dependency on broom.

- Fixed re-entrancy job progress issues when running RStudio 1.2.

- Tables with periods supported by setting
  `sparklyr.dplyr.period.splits` to `FALSE`.

 - `sdf_len()`, `sdf_along()` and `sdf_seq()` default to 32 bit integers
   but allow support for 64 bits through `bits` parameter.

 - Support for detecting Spark version using `spark-submit`.

# Sparklyr 0.9.4

- Improved multiple streaming documentation examples (#1801, #1805, #1806).

- Fix issue while printing Spark data frames under `tibble` 2.0.0 (#1829).

- Support for `stream_write_console()` to write to console log.

- Support for `stream_read_scoket()` to read socket streams.

- Fix to `spark_read_kafka()` to remove unused `path`.

# Sparklyr 0.9.3

- Fix to make `spark_config_kubernetes()` work with variable `jar`
  parameters.

- Support to install and use Spark 2.4.0.

- Improvements and fixes to `spark_config_kubernetes()`
  parameters.

- Support for `sparklyr.connect.ondisconnect` config setting to
  allow cleanup of resources when using kubernetes.

- `spark_apply()` and `spark_apply_bundle()` properly dereference
  symlinks when creating package bundle (@awblocker, #1785)

- Fix `tableName` warning triggered while connecting.

- Deprecate `sdf_mutate()` (#1754).

- Fix requirement to specify `SPARK_HOME_VERSION` when `version`
  parameter is set in `spark_connect()`.

- Cloudera autodetect Spark version improvements.

- Fixed default for `session` in `reactiveSpark()`.

- Removed `stream_read_jdbc()` and `stream_write_jdbc()` since they are
  not yet implemented in Spark.

- Support for collecting NA values from logical columns (#1729).

- Proactevely clean JVM objects when R object is deallocated.

# Sparklyr 0.9.2

- Support for Spark 2.3.2.

- Fix installation error with older versions of `rstudioapi` (#1716).

- Fix missing callstack and error case while logging in
  `spark_apply()`.

- Proactevely clean JVM objects when R object is deallocated.

### Broom

- Implemented `tidy()`, `augment()`, and `glance()` for `ml_linear_svc()`and `ml_pca()` models (@samuelmacedo83)

# Sparklyr 0.9.2

- Support for Spark 2.3.2.

- Fix installation error with older versions of `rstudioapi` (#1716).

- Fix missing callstack and error case while logging in
  `spark_apply()`.

- Fix regression in `sdf_collect()` failing to collect tables.

- Fix new connection RStudio selectors colors when running
  under OS X Mojave.

- Support for launching Livy logs from connection pane.

# Sparklyr 0.9.2

- Removed `overwrite` parameter in `spark_read_table()` (#1698).

- Fix regression preventing using R 3.2 (#1695).

- Additional jar search paths under Spark 2.3.1 (#1694)

# Sparklyr 0.9.1

- Terminate streams when Shiny app terminates.

- Fix `dplyr::collect()` with Spark streams and improve printing.

- Fix regression in `sparklyr.sanitize.column.names.verbose` setting
  which would cause verbose column renames.

- Fix to `stream_write_kafka()` and `stream_write_jdbc()`.

# Sparklyr 0.9.0

### Streaming

- Support for `stream_read_*()` and `stream_write_*()` to read from and
  to Spark structured streams.

- Support for `dplyr`, `sdf_sql()`, `spark_apply()` and scoring pipeline
  in Spark streams.

- Support for `reactiveSpark()` to create a `shiny` reactive over a Spark
  stream.

- Support for convenience functions `stream_*()` to stop, change triggers,
  print, generate test streams, etc.

### Monitoring

- Support for interrupting long running operations and recover gracefully
  using the same connection.

- Support cancelling Spark jobs by interrupting R session.

- Support for monitoring job progress within RStudio, required RStudio 1.2.

- Progress reports can be turned off by setting `sparklyr.progress` to `FALSE`
  in `spark_config()`.

### Kubernetes

- Added config `sparklyr.gateway.routing` to avoid routing to ports since
  Kubernetes clusters have unique spark masters.

- Change backend ports to be choosen deterministically by searching for
  free ports starting on `sparklyr.gateway.port` which default to `8880`. This
  allows users to enable port forwarding with `kubectl port-forward`.

- Added support to set config `sparklyr.events.aftersubmit` to a function
  that is called after `spark-submit` which can be used to automatically
  configure port forwarding.

## Batches

- Added support for `spark_submit()` to assist submitting non-interactive
  Spark jobs.

### Spark ML

- **(Breaking change)** The formula API for ML classification algorithms no longer indexes numeric labels, to avoid the confusion of `0` being mapped to `"1"` and vice versa. This means that if the largest numeric label is `N`, Spark will fit a `N+1`-class classification model, regardless of how many distinct labels there are in the provided training set (#1591).
- Fix retrieval of coefficients in `ml_logistic_regression()` (@shabbybanks, #1596).
- **(Breaking change)** For model objects, `lazy val` and `def` attributes have been converted to closures, so they are not evaluated at object instantiation (#1453).
- Input and output column names are no longer required to construct pipeline objects to be consistent with Spark (#1513).
- Vector attributes of pipeline stages are now printed correctly (#1618).
- Deprecate various aliases favoring method names in Spark.
  - `ml_binary_classification_eval()`
  - `ml_classification_eval()`
  - `ml_multilayer_perceptron()`
  - `ml_survival_regression()`
  - `ml_als_factorization()`
- Deprecate incompatible signatures for `sdf_transform()` and `ml_transform()` families of methods; the former should take a `tbl_spark` as the first argument while the latter should take a model object as the first argument.
- Input and output column names are no longer required to construct pipeline objects to be consistent with Spark (#1513).

### Data

- Implemented support for `DBI::db_explain()` (#1623).

- Fixed for `timestamp` fields when using `copy_to()` (#1312, @yutannihilation).

- Added support to read and write ORC files using `spark_read_orc()` and
  `spark_write_orc()` (#1548).

### Livy

- Fixed `must share the same src` error for `sdf_broadcast()` and other
  functions when using Livy connections.

- Added support for logging `sparklyr` server events and logging sparklyr
  invokes as comments in the Livy UI.

- Added support to open the Livy UI from the connections viewer while
  using RStudio.

- Improve performance in Livy for long execution queries, fixed
  `livy.session.command.timeout` and support for
  `livy.session.command.interval` to control max polling while waiting
  for command response (#1538).

- Fixed Livy version with MapR distributions.

- Removed `install` column from `livy_available_versions()`.

### Distributed R

- Added `name` parameter to `spark_apply()` to optionally name resulting
  table.

- Fix to `spark_apply()` to retain column types when NAs are present (#1665).

- `spark_apply()` now supports `rlang` anonymous functions. For example,
  `sdf_len(sc, 3) %>% spark_apply(~.x+1)`.

- Breaking Change: `spark_apply()` no longer defaults to the input
  column names when the `columns` parameter is nos specified.

- Support for reading column names from the R data frame
  returned by `spark_apply()`.

- Fix to support retrieving empty data frames in grouped
  `spark_apply()` operations (#1505).

- Added support for `sparklyr.apply.packages` to configure default
  behavior for `spark_apply()` parameters (#1530).

- Added support for `spark.r.libpaths` to configure package library in
  `spark_apply()` (#1530).

### Connections

- Default to Spark 2.3.1 for installation and local connections (#1680).

- `ml_load()` no longer keeps extraneous table views which was cluttering up the RStudio Connections pane (@randomgambit, #1549).

- Avoid preparing windows environment in non-local connections.

### Extensions

- The `ensure_*` family of functions is deprecated in favor of [forge](https://github.com/rstudio/forge) which doesn't use NSE and provides more informative errors messages for debugging (#1514).

- Support for `sparklyr.invoke.trace` and `sparklyr.invoke.trace.callstack` configuration
  options to trace all `invoke()` calls.

- Support to invoke methods with `char` types using single character strings (@lawremi, #1395).

### Serialization

- Fixed collection of `Date` types to support correct local JVM timezone to UTC ().

### Documentation

- Many new examples for `ft_binarizer()`, `ft_bucketizer()`, `ft_min_max_scaler`, `ft_max_abs_scaler()`, `ft_standard_scaler()`, `ml_kmeans()`, `ml_pca()`, `ml_bisecting_kmeans()`, `ml_gaussian_mixture()`, `ml_naive_bayes()`, `ml_decision_tree()`, `ml_random_forest()`, `ml_multilayer_perceptron_classifier()`, `ml_linear_regression()`, `ml_logistic_regression()`, `ml_gradient_boosted_trees()`, `ml_generalized_linear_regression()`, `ml_cross_validator()`, `ml_evaluator()`, `ml_clustering_evaluator()`, `ml_corr()`, `ml_chisquare_test()` and `sdf_pivot()` (@samuelmacedo83).

### Broom

- Implemented `tidy()`, `augment()`, and `glance()` for `ml_aft_survival_regression()`, `ml_isotonic_regression()`, `ml_naive_bayes()`, `ml_logistic_regression()`, `ml_decision_tree()`, `ml_random_forest()`, `ml_gradient_boosted_trees()`, `ml_bisecting_kmeans()`, `ml_kmeans()`and `ml_gaussian_mixture()` models (@samuelmacedo83)

### Configuration

- Deprecated configuration option `sparklyr.dplyr.compute.nocache`.

- Added `spark_config_settings()` to list all `sparklyr` configuration settings and
  describe them, cleaned all settings and grouped by area while maintaining support
  for previous settings.

- Static SQL configuration properties are now respected for Spark 2.3, and `spark.sql.catalogImplementation` defaults to `hive` to maintain Hive support (#1496, #415).

- `spark_config()` values can now also be specified as `options()`.

- Support for functions as values in entries to `spark_config()` to enable advanced
  configuration workflows.

# Sparklyr 0.8.4

- Added support for `spark_session_config()` to modify spark session settings.

- Added support for `sdf_debug_string()` to print execution plan for a Spark DataFrame.

- Fixed DESCRIPTION file to include test packages as requested by CRAN.

- Support for `sparklyr.spark-submit` as `config` entry to allow customizing the `spark-submit`
  command.

- Changed `spark_connect()` to give precedence to the `version` parameter over `SPARK_HOME_VERSION` and
  other automatic version detection mechanisms, improved automatic version detection in Spark 2.X.

- Fixed `sdf_bind_rows()` with `dplyr 0.7.5` and prepend id column instead of appending it to match
  behavior.

- `broom::tidy()` for linear regression and generalized linear regression models now give correct results (#1501).

# Sparklyr 0.8.3

- Support for Spark 2.3 in local windows clusters (#1473).

# Sparklyr 0.8.2

- Support for resource managers using `https` in `yarn-cluster` mode (#1459).

- Fixed regression for connections using Livy and Spark 1.6.X.

# Sparklyr 0.8.1

- Fixed regression for connections using `mode` with `databricks`.

# Sparklyr 0.8.0

### Spark ML

- Added `ml_validation_metrics()` to extract validation metrics from cross validator and train split validator models.

- `ml_transform()` now also takes a list of transformers, e.g. the result of `ml_stages()` on a `PipelineModel` (#1444).

- Added `collect_sub_models` parameter to `ml_cross_validator()` and `ml_train_validation_split()` and helper function `ml_sub_models()` to allow inspecting models trained for each fold/parameter set (#1362).

- Added `parallelism` parameter to `ml_cross_validator()` and `ml_train_validation_split()` to allow tuning in parallel (#1446).

- Added support for `feature_subset_strategy` parameter in GBT algorithms (#1445).

- Added `string_order_type` to `ft_string_indexer()` to allow control over how strings are indexed (#1443).

- Added `ft_string_indexer_model()` constructor for the string indexer transformer (#1442).

- Added `ml_feature_importances()` for extracing feature importances from tree-based models (#1436). `ml_tree_feature_importance()` is maintained as an alias.

- Added `ml_vocabulary()` to extract vocabulary from count vectorizer model and `ml_topics_matrix()` to extract matrix from LDA model.

- `ml_tree_feature_importance()` now works properly with decision tree classification models (#1401).

- Added `ml_corr()` for calculating correlation matrices and `ml_chisquare_test()` for performing chi-square hypothesis testing (#1247).

- `ml_save()` outputs message when model is successfully saved (#1348).

- `ml_` routines no longer capture the calling expression (#1393).

- Added support for `offset` argument in `ml_generalized_linear_regression()` (#1396).

- Fixed regression blocking use of response-features syntax in some `ml_`functions (#1302).

- Added support for Huber loss for linear regression (#1335).

- `ft_bucketizer()` and `ft_quantile_discretizer()` now support
  multiple input columns (#1338, #1339).

- Added `ft_feature_hasher()` (#1336).

- Added `ml_clustering_evaluator()` (#1333).

- `ml_default_stop_words()` now returns English stop words by default (#1280).

- Support the `sdf_predict(ml_transformer, dataset)` signature with a deprecation warning. Also added a deprecation warning to the usage of `sdf_predict(ml_model, dataset)`. (#1287)

- Fixed regression blocking use of `ml_kmeans()` in Spark 1.6.x.

### Extensions

- `invoke*()` method dispatch now supports `Char` and `Short` parameters. Also, `Long` parameters now allow numeric arguments, but integers are supported for backwards compatibility (#1395).

- `invoke_static()` now supports calling Scala's package objects (#1384).

- `spark_connection` and `spark_jobj` classes are now exported (#1374).

### Distributed R

- Added support for `profile` parameter in `spark_apply()` that collects a
  profile to measure perpformance that can be rendered using the `profvis`
  package.

- Added support for `spark_apply()` under Livy connections.

- Fixed file not found error in `spark_apply()` while working under low
  disk space.

- Added support for `sparklyr.apply.options.rscript.before` to run a custom
  command before launching the R worker role.

- Added support for `sparklyr.apply.options.vanilla` to be set to `FALSE`
  to avoid using `--vanilla` while launching R worker role.

- Fixed serialization issues most commonly hit while using `spark_apply()` with NAs (#1365, #1366).

- Fixed issue with dates or date-times not roundtripping with `spark_apply() (#1376).

- Fixed data frame provided by `spark_apply()` to not provide characters not factors (#1313).

### Miscellaneous

- Fixed typo in `sparklyr.yarn.cluster.hostaddress.timeot` (#1318).

- Fixed regression blocking use of `livy.session.start.timeout` parameter
  in Livy connections.

- Added support for Livy 0.4 and Livy 0.5.

- Livy now supports Kerberos authentication.

- Default to Spark 2.3.0 for installation and local connections (#1449).

- `yarn-cluster` now supported by connecting with `master="yarn"` and
  `config` entry `sparklyr.shell.deploy-mode` set to `cluster` (#1404).

- `sample_frac()` and `sample_n()` now work properly in nontrivial queries (#1299)

- `sdf_copy_to()` no longer gives a spurious warning when user enters a multiline expression for `x` (#1386).

- `spark_available_versions()` was changed to only return available Spark versions, Hadoop versions
  can be still retrieved using `hadoop = TRUE`.

- `spark_installed_versions()` was changed to retrieve the full path to the installation folder.

- `cbind()` and `sdf_bind_cols()` don't use NSE internally anymore and no longer output names of mismatched data frames on error (#1363).

# Sparklyr 0.7.0

- Added support for Spark 2.2.1.

- Switched `copy_to` serializer to use Scala implementation, this change can be
  reverted by setting the `sparklyr.copy.serializer` option to `csv_file`.

- Added support for `spark_web()` for Livy and Databricks connections when
  using Spark 2.X.

- Fixed `SIGPIPE` error under `spark_connect()` immediately after
  a `spark_disconnect()` operation.

- `spark_web()` is is more reliable under Spark 2.X by making use of a new API
  to programmatically find the right address.

- Added support in `dbWriteTable()` for `temporary = FALSE` to allow persisting
  table across connections. Changed default value for `temporary` to `TRUE` to match
  `DBI` specification, for compatibility, default value can be reverted back to
  `FALSE` using the `sparklyr.dbwritetable.temp` option.

- `ncol()` now returns the number of columns instead of `NA`, and `nrow()` now
  returns `NA_real_`.

- Added support to collect `VectorUDT` column types with nested arrays.

- Fixed issue in which connecting to Livy would fail due to long user names
  or long passwords.

- Fixed error in the Spark connection dialog for clusters using a proxy.

- Improved support for Spark 2.X under Cloudera clusters by prioritizing
use of `spark2-submit` over `spark-submit`.

- Livy new connection dialog now prompts for password using
`rstudioapi::askForPassword()`.

- Added `schema` parameter to `spark_read_parquet()` that enables reading
a subset of the schema to increase performance.

- Implemented `sdf_describe()` to easily compute summary statistics for
data frames.

- Fixed data frames with dates in `spark_apply()` retrieved as `Date` instead
  of doubles.

- Added support to use `invoke()` with arrays of POSIXlt and POSIXct.

- Added support for `context` parameter in `spark_apply()` to allow callers to
  pass additional contextual information to the `f()` closure.

- Implemented workaround to support in `spark_write_table()` for
  `mode = 'append'`.

- Various ML improvements, including support for pipelines, additional algorithms,
  hyper-parameter tuning, and better model persistence.

- Added `spark_read_libsvm()` for reading libsvm files.

- Added support for separating struct columns in `sdf_separate_column()`.

- Fixed collection of `short`, `float` and `byte` to properly return NAs.

- Added `sparklyr.collect.datechars` option to enable collecting `DateType` and
  `TimestampTime` as `characters` to support compatibility with previos versions.

- Fixed collection of `DateType` and `TimestampTime` from `character` to
  proper `Date` and `POSIXct` types.

# Sparklyr 0.6.4

- Added support for HTTPS for `yarn-cluster` which is activated by setting
  `yarn.http.policy` to `HTTPS_ONLY` in `yarn-site.xml`.

- Added support for `sparklyr.yarn.cluster.accepted.timeout` under `yarn-cluster`
  to allow users to wait for resources under cluster with high waiting times.

- Fix to `spark_apply()` when package distribution deadlock triggers in
  environments where multiple executors run under the same node.

- Added support in `spark_apply()` for specifying  a list of `packages` to
  distribute to each worker node.

- Added support in`yarn-cluster` for `sparklyr.yarn.cluster.lookup.prefix`,
  `sparklyr.yarn.cluster.lookup.username` and `sparklyr.yarn.cluster.lookup.byname`
  to control the new application lookup behavior.

# Sparklyr 0.6.3

- Enabled support for Java 9 for clusters configured with
  Hadoop 2.8. Java 9 blocked on 'master=local' unless
  'options(sparklyr.java9 = TRUE)' is set.

- Fixed issue in `spark_connect()` where using `set.seed()`
  before connection would cause session ids to be duplicates
  and connections to be reused.

- Fixed issue in `spark_connect()` blocking gateway port when
  connection was never started to the backend, for isntasnce,
  while interrupting the r session while connecting.

- Performance improvement for quering field names from tables
  impacting tables and `dplyr` queries, most noticeable in
  `na.omit` with several columns.

- Fix to `spark_apply()` when closure returns a `data.frame`
  that contains no rows and has one or more columns.

- Fix to `spark_apply()` while using `tryCatch()` within
  closure and increased callstack printed to logs when
  error triggers within closure.

- Added support for the `SPARKLYR_LOG_FILE` environment
  variable to specify the file used for log output.

- Fixed regression for `union_all()` affecting Spark 1.6.X.

- Added support for `na.omit.cache` option that when set to
  `FALSE` will prevent `na.omit` from caching results when
  rows are dropped.

- Added support in `spark_connect()` for `yarn-cluster` with
  hight-availability enabled.

- Added support for `spark_connect()` with `master="yarn-cluster"`
  to query YARN resource manager API and retrieve the correct
  container host name.

- Fixed issue in `invoke()` calls while using integer arrays
  that contain `NA` which can be commonly experienced
  while using `spark_apply()`.

- Added `topics.description` under `ml_lda()` result.

- Added support for `ft_stop_words_remover()` to strip out
  stop words from tokens.

- Feature transformers (`ft_*` functions) now explicitly
  require `input.col` and `output.col` to be specified.

- Added support for `spark_apply_log()` to enable logging in
  worker nodes while using `spark_apply()`.

- Fix to `spark_apply()` for `SparkUncaughtExceptionHandler`
  exception while running over large jobs that may overlap
  during an, now unnecesary, unregister operation.

- Fix race-condition first time `spark_apply()` is run when more
  than one partition runs in a worker and both processes try to
  unpack the packages bundle at the same time.

- `spark_apply()` now adds generic column names when needed and
  validates `f` is a `function`.

- Improved documentation and error cases for `metric` argument in
  `ml_classification_eval()` and `ml_binary_classification_eval()`.

- Fix to `spark_install()` to use the `/logs` subfolder to store local
  `log4j` logs.

- Fix to `spark_apply()` when R is used from a worker node since worker
  node already contains packages but still might be triggering different
  R session.

- Fix connection from closing when `invoke()` attempts to use a class
  with a method that contains a reference to an undefined class.

- Implemented all tuning options from Spark ML for `ml_random_forest()`,
  `ml_gradient_boosted_trees()`, and `ml_decision_tree()`.

- Avoid tasks failing under `spark_apply()` and multiple  concurrent
  partitions running while selecting backend port.

- Added support for numeric arguments for `n` in `lead()` for dplyr.

- Added unsupported error message to `sample_n()` and `sample_frac()`
  when Spark is not 2.0 or higher.

- Fixed `SIGPIPE` error under `spark_connect()` immediately after
  a `spark_disconnect()` operation.

- Added support for `sparklyr.apply.env.` under `spark_config()` to
  allow `spark_apply()` to initializae environment varaibles.

- Added support for `spark_read_text()` and `spark_write_text()` to
  read from and to plain text files.

- Addesd support for RStudio project templates to create an
  "R Package using sparklyr".

- Fix `compute()` to trigger refresh of the connections view.

- Added a `k` argument to `ml_pca()` to enable specification of number of
  principal components to extract. Also implemented `sdf_project()` to project
  datasets using the results of `ml_pca()` models.

- Added support for additional livy session creation parameters using
  the `livy_config()` function.

# Sparklyr 0.6.2

- Fix connection_spark_shinyapp() under RStudio 1.1 to avoid error while
  listing Spark installation options for the first time.

# Sparklyr 0.6.1

- Fixed error in `spark_apply()` that may triggered when multiple CPUs
  are used in a single node due to race conditions while accesing the
  gateway service and another in the `JVMObjectTracker`.

- `spark_apply()` now supports explicit column types using the `columns`
  argument to avoid sampling types.

- `spark_apply()` with `group_by` no longer requires persisting to disk
  nor memory.

- Added support for Spark 1.6.3 under `spark_install()`.

- Added support for Spark 1.6.3 under `spark_install()`

- `spark_apply()` now logs the current callstack when it fails.

- Fixed error triggered while processing empty partitions in `spark_apply()`.

- Fixed slow printing issue caused by `print` calculating the total row count,
  which is expensive for some tables.

- Fixed `sparklyr 0.6` issue blocking concurrent `sparklyr` connections, which
 required to set `config$sparklyr.gateway.remote = FALSE` as workaround.

# Sparklyr 0.6.0

### Distributed R

- Added `packages` parameter to `spark_apply()` to distribute packages
  across worker nodes automatically.

- Added `sparklyr.closures.rlang` as a `spark_config()` value to support
  generic closures provided by the `rlang` package.

- Added config options `sparklyr.worker.gateway.address` and
  `sparklyr.worker.gateway.port` to configure gateway used under
  worker nodes.

- Added `group_by` parameter to `spark_apply()`, to support operations
  over groups of dataframes.

- Added `spark_apply()`, allowing users to use R code to directly
  manipulate and transform Spark DataFrames.

### External Data

- Added `spark_write_source()`. This function writes data into a
  Spark data source which can be loaded through an Spark package.

- Added `spark_write_jdbc()`. This function writes from a Spark DataFrame
  into a JDBC connection.

- Added `columns` parameter to `spark_read_*()` functions to load data with
  named columns or explicit column types.

- Added `partition_by` parameter to `spark_write_csv()`, `spark_write_json()`,
  `spark_write_table()` and `spark_write_parquet()`.

- Added `spark_read_source()`. This function reads data from a
  Spark data source which can be loaded through an Spark package.

- Added support for `mode = "overwrite"` and `mode = "append"` to
  `spark_write_csv()`.

- `spark_write_table()` now supports saving to default Hive path.

- Improved performance of `spark_read_csv()` reading remote data when
  `infer_schema = FALSE`.

- Added `spark_read_jdbc()`. This function reads from a JDBC connection
  into a Spark DataFrame.

- Renamed `spark_load_table()` and `spark_save_table()` into `spark_read_table()`
  and `spark_write_table()` for consistency with existing `spark_read_*()` and
  `spark_write_*()` functions.

- Added support to specify a vector of column names in `spark_read_csv()` to
  specify column names without having to set the type of each column.

- Improved `copy_to()`, `sdf_copy_to()` and `dbWriteTable()` performance under
  `yarn-client` mode.

### dplyr

- Support for `cumprod()` to calculate cumulative products.

- Support for `cor()`, `cov()`, `sd()` and `var()` as window functions.

- Support for Hive built-in operators `%like%`, `%rlike%`, and
  `%regexp%` for matching regular expressions in `filter()` and `mutate()`.

- Support for dplyr (>= 0.6) which among many improvements, increases
  performance in some queries by making use of a new query optimizer.

- `sample_frac()` takes a fraction instead of a percent to match dplyr.

- Improved performance of `sample_n()` and `sample_frac()` through the use of
  `TABLESAMPLE` in the generated query.

### Databases

- Added `src_databases()`. This function list all the available databases.

- Added `tbl_change_db()`. This function changes current database.

### DataFrames

- Added `sdf_len()`, `sdf_seq()` and `sdf_along()` to help generate numeric
  sequences as Spark DataFrames.

- Added `spark_set_checkpoint_dir()`, `spark_get_checkpoint_dir()`, and
  `sdf_checkpoint()` to enable checkpointing.

- Added `sdf_broadcast()` which can be used to hint the query
  optimizer to perform a broadcast join in cases where a shuffle
  hash join is planned but not optimal.

- Added `sdf_repartition()`, `sdf_coalesce()`, and `sdf_num_partitions()`
  to support repartitioning and getting the number of partitions of Spark
  DataFrames.

- Added `sdf_bind_rows()` and `sdf_bind_cols()` -- these functions
  are the `sparklyr` equivalent of `dplyr::bind_rows()` and
  `dplyr::bind_cols()`.

- Added `sdf_separate_column()` -- this function allows one to separate
  components of an array / vector column into separate scalar-valued
  columns.

- `sdf_with_sequential_id()` now supports `from` parameter to choose the
  starting value of the id column.

- Added `sdf_pivot()`. This function provides a mechanism for constructing
  pivot tables, using Spark's 'groupBy' + 'pivot' functionality, with a
  formula interface similar to that of `reshape2::dcast()`.

### MLlib

- Added `vocabulary.only` to `ft_count_vectorizer()` to retrieve the
  vocabulary with ease.

- GLM type models now support `weights.column` to specify weights in model
  fitting. (#217)

- `ml_logistic_regression()` now supports multinomial regression, in
  addition to binomial regression [requires Spark 2.1.0 or greater]. (#748)

- Implemented `residuals()` and `sdf_residuals()` for Spark linear
  regression and GLM models. The former returns a R vector while
  the latter returns a `tbl_spark` of training data with a `residuals`
  column added.

- Added `ml_model_data()`, used for extracting data associated with
  Spark ML models.

- The `ml_save()` and `ml_load()` functions gain a `meta` argument, allowing
  users to specify where R-level model metadata should be saved independently
  of the Spark model itself. This should help facilitate the saving and loading
  of Spark models used in non-local connection scenarios.

- `ml_als_factorization()` now supports the implicit matrix factorization
   and nonnegative least square options.

- Added `ft_count_vectorizer()`. This function can be used to transform
  columns of a Spark DataFrame so that they might be used as input to `ml_lda()`.
  This should make it easier to invoke `ml_lda()` on Spark data sets.

### Broom

- Implemented `tidy()`, `augment()`, and `glance()` from tidyverse/broom for
  `ml_model_generalized_linear_regression` and `ml_model_linear_regression`
  models.

### R Compatibility

- Implemented `cbind.tbl_spark()`. This method works by first generating
  index columns using `sdf_with_sequential_id()` then performing `inner_join()`.
  Note that dplyr `_join()` functions should still be used for DataFrames
  with common keys since they are less expensive.

### Connections

- Increased default number of concurrent connections by setting default for
  `spark.port.maxRetries` from 16 to 128.

- Support for gateway connections `sparklyr://hostname:port/session` and using
  `spark-submit --class sparklyr.Shell sparklyr-2.1-2.11.jar <port> <id> --remote`.

- Added support for `sparklyr.gateway.service` and `sparklyr.gateway.remote` to
  enable/disable the gateway in service and to accept remote connections required
  for Yarn Cluster mode.

- Added support for Yarn Cluster mode using `master = "yarn-cluster"`. Either,
  explicitly set `config = list(sparklyr.gateway.address = "<driver-name>")` or
  implicitly `sparklyr` will read the `site-config.xml` for the `YARN_CONF_DIR`
  environment variable.

- Added `spark_context_config()` and `hive_context_config()` to retrieve
  runtime configurations for the Spark and Hive contexts.

- Added `sparklyr.log.console` to redirect logs to console, useful
  to troubleshooting `spark_connect`.

- Added `sparklyr.backend.args` as config option to enable passing
  parameters to the `sparklyr` backend.

- Improved logging while establishing connections to `sparklyr`.

- Improved `spark_connect()` performance.

- Implemented new configuration checks to proactively report connection errors
  in Windows.

- While connecting to spark from Windows, setting the `sparklyr.verbose` option
  to `TRUE` prints detailed configuration steps.

- Added `custom_headers` to `livy_config()` to add custom headers to the REST call
  to the Livy server

### Compilation

- Added support for `jar_dep` in the compilation specification to
  support additional `jars` through `spark_compile()`.

- `spark_compile()` now prints deprecation warnings.

- Added `download_scalac()` to assist downloading all the Scala compilers
  required to build using `compile_package_jars` and provided support for
  using any `scalac` minor versions while looking for the right compiler.

### Backend

- Improved backend logging by adding type and session id prefix.

### Miscellaneous

- `copy_to()` and `sdf_copy_to()` auto generate a `name` when an expression
  can't be transformed into a table name.

- Implemented `type_sum.jobj()` (from tibble) to enable better printing of jobj
  objects embedded in data frames.

- Added the `spark_home_set()` function, to help facilitate the setting of the
  `SPARK_HOME` environment variable. This should prove useful in teaching
  environments, when teaching the basics of Spark and sparklyr.

- Added support for the `sparklyr.ui.connections` option, which adds additional
  connection options into the new connections dialog. The
  `rstudio.spark.connections` option is now deprecated.

- Implemented the "New Connection Dialog" as a Shiny application to be able to
  support newer versions of RStudio that deprecate current connections UI.

### Bug Fixes

- When using `spark_connect()` in local clusters, it validates that `java` exists
  under `JAVA_HOME` to help troubleshoot systems that have an incorrect `JAVA_HOME`.

- Improved `argument is of length zero` error triggered while retrieving data
  with no columns to display.

- Fixed `Path does not exist` referencing `hdfs` exception during `copy_to` under
  systems configured with `HADOOP_HOME`.

- Fixed session crash after "No status is returned" error by terminating
  invalid connection and added support to print log trace during this error.

- `compute()` now caches data in memory by default. To revert this beavior use
  `sparklyr.dplyr.compute.nocache` set to `TRUE`.

- `spark_connect()` with `master = "local"` and a given `version` overrides
  `SPARK_HOME` to avoid existing installation mismatches.

- Fixed `spark_connect()` under Windows issue when `newInstance0` is present in
  the logs.

- Fixed collecting `long` type columns when NAs are present (#463).

- Fixed backend issue that affects systems where `localhost` does
  not resolve properly to the loopback address.

- Fixed issue collecting data frames containing newlines `\n`.

- Spark Null objects (objects of class NullType) discovered within numeric
  vectors are now collected as NAs, rather than lists of NAs.

- Fixed warning while connecting with livy and improved 401 message.

- Fixed issue in `spark_read_parquet()` and other read methods in which
  `spark_normalize_path()` would not work in some platforms while loading
  data using custom protocols like `s3n://` for Amazon S3.

- Resolved issue in `spark_save()` / `load_table()` to support saving / loading
  data and added path parameter in `spark_load_table()` for consistency with
  other functions.

# Sparklyr 0.5.5

- Implemented support for `connectionViewer` interface required in RStudio 1.1
  and `spark_connect` with `mode="databricks"`.

# Sparklyr 0.5.4

- Implemented support for `dplyr 0.6` and Spark 2.1.x.

# Sparklyr 0.5.3

- Implemented support for `DBI 0.6`.

# Sparklyr 0.5.2

- Fix to `spark_connect` affecting Windows users and Spark 1.6.x.

- Fix to Livy connections which would cause connections to fail while connection
  is on 'waiting' state.

# Sparklyr 0.5.0

- Implemented basic authorization for Livy connections using
  `livy_config_auth()`.

- Added support to specify additional `spark-submit` parameters using the
  `sparklyr.shell.args` environment variable.

- Renamed `sdf_load()` and `sdf_save()` to `spark_read()` and `spark_write()`
  for consistency.

- The functions `tbl_cache()` and `tbl_uncache()` can now be using without
  requiring the `dplyr` namespace to be loaded.

- `spark_read_csv(..., columns = <...>, header = FALSE)` should now work as
  expected -- previously, `sparklyr` would still attempt to normalize the
  column names provided.

- Support to configure Livy using the `livy.` prefix in the `config.yml` file.

- Implemented experimental support for Livy through: `livy_install()`,
  `livy_service_start()`, `livy_service_stop()` and
  `spark_connect(method = "livy")`.

- The `ml` routines now accept `data` as an optional argument, to support
  calls of the form e.g. `ml_linear_regression(y ~ x, data = data)`. This
  should be especially helpful in conjunction with `dplyr::do()`.

- Spark `DenseVector` and `SparseVector` objects are now deserialized as
  R numeric vectors, rather than Spark objects. This should make it easier
  to work with the output produced by `sdf_predict()` with Random Forest
  models, for example.

- Implemented `dim.tbl_spark()`. This should ensure that `dim()`, `nrow()`
  and `ncol()` all produce the expected result with `tbl_spark`s.

- Improved Spark 2.0 installation in Windows by creating `spark-defaults.conf`
  and configuring `spark.sql.warehouse.dir`.

- Embedded Apache Spark package dependencies to avoid requiring internet
  connectivity while connecting for the first through `spark_connect`. The
  `sparklyr.csv.embedded` config setting was added to configure a regular
  expression to match Spark versions where the embedded package is deployed.

- Increased exception callstack and message length to include full
  error details when an exception is thrown in Spark.

- Improved validation of supported Java versions.

- The `spark_read_csv()` function now accepts the `infer_schema` parameter,
  controlling whether the columns schema should be inferred from the underlying
  file itself. Disabling this should improve performance when the schema is
  known beforehand.

- Added a `do_.tbl_spark` implementation, allowing for the execution of
  `dplyr::do` statements on Spark DataFrames. Currently, the computation is
  performed in serial across the different groups specified on the Spark
  DataFrame; in the future we hope to explore a parallel implementation.
  Note that `do_` always returns a `tbl_df` rather than a `tbl_spark`, as
  the objects produced within a `do_` query may not necessarily be Spark
  objects.

- Improved errors, warnings and fallbacks for unsupported Spark versions.

- `sparklyr` now defaults to `tar = "internal"` in its calls to `untar()`.
  This should help resolve issues some Windows users have seen related to
  an inability to connect to Spark, which ultimately were caused by a lack
  of permissions on the Spark installation.

- Resolved an issue where `copy_to()` and other R => Spark data transfer
  functions could fail when the last column contained missing / empty values.
  (#265)

- Added `sdf_persist()` as a wrapper to the Spark DataFrame `persist()` API.

- Resolved an issue where `predict()` could produce results in the wrong
  order for large Spark DataFrames.

- Implemented support for `na.action` with the various Spark ML routines. The
  value of `getOption("na.action")` is used by default. Users can customize the
  `na.action` argument through the `ml.options` object accepted by all ML
  routines.

- On Windows, long paths, and paths containing spaces, are now supported within
  calls to `spark_connect()`.

- The `lag()` window function now accepts numeric values for `n`. Previously,
  only integer values were accepted. (#249)

- Added support to configure Ppark environment variables using `spark.env.*` config.

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

- The `spark_log()` function now accepts the `filter` parameter. This can be used
  to filter entries within the Spark log.

- Increased network timeout for `sparklyr.backend.timeout`.

- Moved `spark.jars.default` setting from options to Spark config.

- `sparklyr` now properly respects the Hive metastore directory with the
  `sdf_save_table()` and `sdf_load_table()` APIs for Spark < 2.0.0.

- Added `sdf_quantile()` as a means of computing (approximate) quantiles
  for a column of a Spark DataFrame.

- Added support for `n_distinct(...)` within the `dplyr` interface, based on
  call to Hive function `count(DISTINCT ...)`. (#220)

# Sparklyr 0.4.0

- First release to CRAN.
