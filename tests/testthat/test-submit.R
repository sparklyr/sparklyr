# CI k8s POD is crashing when running these test with
# dbplyr dev. Need to investigate.
skip_on_ci()
skip_if_dbplyr_dev()
skip_connection("submit")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("spark_submit() can submit batch jobs", {
  if (.Platform$OS.type == "windows") {
    skip("spark_submit() not yet implemented for windows")
  }

  batch_file <- dir(getwd(), recursive = TRUE, pattern = "batch.R", full.names = TRUE)

  if (dir.exists("batch.csv")) unlink("batch.csv", recursive = TRUE)

  sc <- testthat_spark_connection()

  withr::with_options(
    list(
      sparklyr.log.console = FALSE,
      sparklyr.verbose = FALSE
    ),
    {
      spark_submit(master = "local",
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
