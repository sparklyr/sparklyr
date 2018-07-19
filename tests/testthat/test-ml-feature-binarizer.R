context("ml feature binarizer")

sc <- testthat_spark_connection()

test_that("ft_binarizer() returns params of transformer", {
  binarizer <- ft_binarizer(sc, "x", "y", threshold = 0.5)
  params <- list("x", "y", threshold = 0.5)
  expect_true(dplyr::setequal(ml_param_map(binarizer), params))
})

test_that("ft_binarizer.tbl_spark() works as expected", {
  test_requires("dplyr")
  df <- data.frame(id = 0:2L, feature = c(0.1, 0.8, 0.2))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  expect_equal(
    df_tbl %>%
      ft_binarizer("feature", "binarized_feature", threshold = 0.5) %>%
      collect(),
    df %>%
      mutate(binarized_feature = c(0.0, 1.0, 0.0))
  )
})

test_that("ft_binarizer() threshold defaults to 0", {
  expect_identical(ft_binarizer(sc, "in", "out") %>%
                     ml_param("threshold"),
                   0)
})

test_that("ft_binarizer() input checking works", {
  expect_identical(ft_binarizer(sc, "in", "out", 1L) %>%
                     ml_param("threshold") %>%
                     class(),
                   "numeric")
  expect_error(ft_binarizer(sc, "in", "out", "foo"),
               "length-one numeric vector")

  bin <- ft_binarizer(sc, "in", "out", threshold = 10)
  expect_equal(ml_params(bin, list("input_col", "output_col", "threshold")),
               list(input_col = "in", output_col = "out", threshold = 10))
})
