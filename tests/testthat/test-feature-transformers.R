context("feature transformers")

sc <- testthat_spark_connection()
mtcars_tbl <- testthat_tbl("mtcars")

test_that("ft_binarizer() works as expected", {

  threshold <- 3.5
  mutated <- mtcars_tbl %>%
    sdf_mutate(drat_binary = ft_binarizer(drat, threshold = threshold))

  expect_identical(
    sdf_read_column(mutated, "drat_binary"),
    as.numeric(mtcars$drat > 3.5)
  )

})

test_that("ft_bucketizer() works as expected", {

  splits <- c(-Inf, 2, 4, Inf)
  mutated <- mtcars_tbl %>%
    sdf_mutate(buckets = ft_bucketizer(drat, splits))

  buckets   <- sdf_read_column(mutated, "buckets")
  cutpoints <- as.numeric(cut(mtcars$drat, c(-Inf, 2, 4, Inf))) - 1

  expect_identical(buckets, cutpoints)
})
