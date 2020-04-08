context("describe")

sc <- testthat_spark_connection()

test_that("sdf_collect() works properly", {
  mtcars_tbl <- testthat_tbl("mtcars")
  mtcars_data <- sdf_collect(mtcars_tbl)

  expect_equivalent(mtcars, mtcars_data)
})

test_that("sdf_collect() works with nested lists", {
  if (spark_version(sc) < "2.4")
    skip("nested list is only supported with Spark 2.4+")

  df <- tibble::tibble(
    a = list(c(1, 2, 3), c(4, 5), c(6)),
    b = list(c("foo"), c("bar", "foobar"), c("a", "b", "c"))
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  res <- sdf_collect(sdf)

  expect_equivalent(df$a, res$a)
  expect_equivalent(df$b, sapply(res$b, function(e) do.call(c, e)))
})

test_that("sdf_collect() supports callback", {
  if (spark_version(sc) < "2.0") skip("batch collection requires Spark 2.0")

  batch_count <- 0
  row_count <- 0

  sdf_len(sc, 10, repartition = 2) %>%
    sdf_collect(callback = function(df) {
      batch_count <<- batch_count + 1
      row_count <<- row_count + nrow(df)
    })

  expect_equal(
    batch_count,
    ifelse("arrow" %in% .packages(), 2, 1)
  )

  expect_equal(
    row_count,
    10
  )

  last_idx <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    sdf_collect(callback = function(df, idx) {
      last_idx <<- idx
    })

  expect_equal(
    last_idx,
    ifelse("arrow" %in% .packages(), 2, 1)
  )
})

test_that("sdf_collect() supports callback expression", {
  if (spark_version(sc) < "2.0") skip("batch collection requires Spark 2.0")

  row_count <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    collect(callback = ~(row_count <<- row_count + nrow(.x)))

  expect_equal(
    10,
    row_count
  )
})
