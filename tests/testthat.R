## To run tests use:
## devtools::test()

## To run tests using the custom testthat reporter:
## devtools::test(reporter = sparklyr_reporter())

## To run tests a custom subset of tests use filter:
## devtools::test(filter = "^dbi$", reporter = sparklyr_reporter())

## To change defaults, change these specific environment variables, before
## running the tests:
## Sys.setenv("SPARK_VERSION" = "2.4.0")
## Sys.setenv("LIVY_VERSION" = "0.6.0")
## Sys.setenv("ARROW_VERSION" = "release") # Or "devel"
## devtools::test(reporter = sparklyr_reporter())

## To test Spark Connect:
## Sys.setenv("TEST_SPARKLYR_METHOD" = "spark_connect")
## Sys.setenv("TEST_SPARKLYR_MASTER" = "sc://localhost")
## Sys.setenv("TEST_SPARKLYR_LIBRARIES" = "dplyr; pysparklyr;")

## To test Databricks Connect v2
## Sys.setenv("DATABRICKS_CLUSTER_ID" = "[my cluster id]")
## Sys.setenv("TEST_SPARKLYR_METHOD" = "databricks_connect")
## Sys.setenv("TEST_SPARKLYR_MASTER" = Sys.getenv("DATABRICKS_HOST"))
## Sys.setenv("TEST_SPARKLYR_LIBRARIES" = "dbplyr; dplyr; pysparklyr;")


## For testing new versions of Spark, and need to prioritize the
## local versions.json file over the one in the GH repo use:
## Sys.setenv("R_SPARKINSTALL_INSTALL_INFO_PATH" = here::here("inst/extdata/versions.json"))

## For Coverage us: Sys.setenv("CODE_COVERAGE" = "true")

if(identical(Sys.getenv("CODE_COVERAGE"), "true")) {
  library(testthat)
  library(sparklyr)
  test_check("sparklyr")
}
