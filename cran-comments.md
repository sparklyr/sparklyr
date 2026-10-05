## Resubmission

This is a resubmission. In this version I have:

- Fixed the "Rd files without \usage" NOTE from the Debian r-devel pre-test.
Removed the parameter-only Rd topics `ensure`, `generic_call_interface`,
`ml_kmeans_cluster_eval`, and `spark_statistical_routines`. Their parameters
are now documented on exported functions that have a `\usage` section.

## Submission

- `spark_read_jdbc()` is now an S3 generic, so extension packages such as
`pysparklyr` can provide their own method.

- The JARs are rebuilt against Spark 3.5.9 and 4.0.4.

- Corrects docs in ml_generalized_linear_regression() for Binomial, switching
LogLog to CLogLog (https://spark.apache.org/docs/latest/ml-classification-regression.htm)
(@000wahab000 / #3530)

- Fixed the Spark 4 (Scala 2.13) backend returning `java.util.List` results that
wrap a Scala `Seq`, such as the output of `JavaRDD.take()` or
`JavaRDD.collect()` reached through `invoke()`, as a single Java object
reference. They are now unwrapped into an R list, as they are on the Spark 3
backend, which had a `SeqWrapper` case that the Scala 2.13 port dropped
(Scala 2.13 keeps its wrapper classes `private[collection]`, so the backend
now recognises them by name) (@jiayuasu / #3531).

- Fixed `download_scalac()`, which failed because Lightbend no longer hosts the
Scala downloads. It now uses the Scala GitHub releases, and a new `urls`
argument lets users point to a different location (#3532).

- Fixed "argument is of length zero" errors in two places. `spark_config_packages()`
failed for `"rapids"` when `method` was not given. ML functions failed on older
Spark versions when a version-gated argument with a `NULL` default, such as
`variance_col` or `offset_col`, was set (@sims1253 / #3528).

- Fixed a spurious "one argument not used by format" warning from
`spark_require_version()` when a Spark version requirement isn't met.

- Fixed a spurious "NAs introduced by coercion to integer range" warning when
collecting with Arrow and `n = Inf`, such as `collect()` of all rows or printing
a large `tbl_spark`.

- Fixed `augment()` on linear and generalized linear regression models, which
errored with "Can't rename columns that don't exist" when `type.residuals` is
not `"working"` and no `newdata` is supplied.

- Fixed `names<-()` on a `tbl_spark`, which errored with "Can't escape back
tick from string" on recent `dbplyr`.

- Fixed `ml_gbt_classifier()` on Spark < 2.2, which classed fitted models as
`ml_multilayer_perceptron_classification_model` instead of
`ml_gbt_classification_model`.

- Fixed `ft_robust_scaler()`, which returned a generic `ml_transformer` instead
of an `ml_robust_scaler_model` when fitted.

- Fixed the `sparklyr.stream.collect.timeout` and
`sparklyr.stream.validate.timeout` options, which were silently ignored.

- Fixed `sdf_pivot()` so a multi-column pivot, such as `a ~ b + c`, fails with a
clear "pivot column is not length one" error instead of a confusing "missing
variables in dataset" error.

- Fixed `spark_write()` and `spark_write_table()`, which always errored when
passed a `spark_jobj`.

- `spark_connect()` now loads `pysparklyr` for `method = "snowpark_connect"`
and `method = "sail"`, as it already did for `"spark_connect"` and
`"databricks_connect"`. It errors with a clear message if `pysparklyr` is not
installed.

- Internal reorganization of the package's R source files, consolidating
related functions into a smaller, more cohesive set of scripts. No exported
functions, behavior, or APIs change. Functions that the R worker code needs,
such as those used by `spark_apply()`, stay in the files embedded in the JARs,
so the worker continues to run.

- Reorganized the test suite to follow a strict 1:1 correspondence between each
test file and its R source file. No tests were added, removed, or altered.

## Test environments

- Spark 4.1: Ubuntu 24.04.5 LTS (x86_64, linux-gnu), R version 4.6.1 (2026-06-24)
- Spark 4.1 w Arrow: Ubuntu 24.04.5 LTS (x86_64, linux-gnu), R version 4.6.1 (2026-06-24)

## R CMD check environments

- Ubuntu 24.04.5 LTS (x86_64, linux-gnu), R version 4.6.1 (2026-06-24)
- Ubuntu 24.04.5 LTS (x86_64, linux-gnu), R Under development (unstable) (2026-09-30 r90605)
- Windows Server 2022 x64 (build 26100) (x86_64, mingw32), R version 4.6.1 (2026-06-24 ucrt)
- macOS Tahoe 26.6.2 (aarch64, darwin23), R version 4.6.1 (2026-06-24)

## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked 35 reverse dependencies (34 from CRAN + 1 from Bioconductor),
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
