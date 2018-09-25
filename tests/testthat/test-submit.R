context("submit")

test_that("spark_submit() can submit batch jobs", {
  skip("NYI")

  batch_file <- dir(getwd(), recursive = TRUE, pattern = "batch.R", full.names = TRUE)

  spark_submit(master = "local", file = batch_file)
})
