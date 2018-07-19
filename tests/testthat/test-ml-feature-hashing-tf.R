context("ml feature hashing tf")

test_that("ft_hashing_tf() works", {
  sc <- testthat_spark_connection()
  args <- list(x = sc, input_col = "in", output_col = "out", num_features = 1024) %>%
    param_add_version("2.0.0", binary = TRUE)

  transformer <- do.call(ft_hashing_tf, args)

  expect_equal(ml_params(transformer, names(args)[-1]), args[-1])
})

test_that('ft_hashing_tf() input checking', {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0", "binary arg requires spark 2.0+")
  expect_error(ft_hashing_tf(sc, "in", "out", binary = 1),
               "length-one logical vector")
})
