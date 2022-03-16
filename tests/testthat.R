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


if(identical(Sys.getenv("CODE_COVERAGE"), "true")) {
  library(testthat)
  library(sparklyr)
  test_check("sparklyr")
}
