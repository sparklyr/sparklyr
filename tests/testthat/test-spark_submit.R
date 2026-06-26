skip_connection("spark_submit")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("spark_submit() can submit batch jobs", {
  skip_if_dbplyr_dev()
  skip_databricks_connect()
  if (.Platform$OS.type == "windows") {
    skip("spark_submit() not yet implemented for windows")
  }

  batch_file <- dir(
    getwd(),
    recursive = TRUE,
    pattern = "batch.R",
    full.names = TRUE
  )

  if (dir.exists("batch.csv")) {
    unlink("batch.csv", recursive = TRUE)
  }

  sc <- testthat_spark_connection()

  withr::with_options(
    list(
      sparklyr.log.console = FALSE,
      sparklyr.verbose = FALSE
    ),
    {
      spark_submit(
        master = "local",
        file = batch_file,
        version = spark_version(sc)
      )
    }
  )

  retries <- 60
  while (!dir.exists("batch.csv") && retries > 0) {
    Sys.sleep(1)
    retries <- retries - 1
  }

  expect_gt(retries, 0)
})

test_that("spark_dependency_fallback() works correctly", {
  expect_equal(
    spark_dependency_fallback("2.3", c("2.1", "2.2")),
    "2.2"
  )

  expect_equal(
    spark_dependency_fallback("2.2", c("2.1", "2.2")),
    "2.2"
  )

  expect_equal(
    spark_dependency_fallback("2.2", c("2.1", "2.3")),
    "2.1"
  )
})

test_that("sparklyr.nested can query nested columns", {
  if (!"sparklyr.nested" %in% installed.packages()) {
    skip("sparklyr.nested not installed.")
  }

  iris_tbl <- testthat_tbl("iris")
  iris_nst <- iris_tbl %>% sparklyr.nested::sdf_nest(Species, Sepal_Width)

  expect_equal(
    iris_nst %>%
      filter(data[["Species"]] == "setosa") %>%
      count() %>%
      pull(n) %>%
      as.integer(),
    50
  )
})

test_clear_cache()
