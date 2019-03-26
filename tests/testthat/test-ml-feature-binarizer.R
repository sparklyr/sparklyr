context("ml feature binarizer")

test_that("ft_binarizer() default params", {
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_binarizer)
})

test_that("ft_binarizer() param setting", {
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "x",
    output_col = "y",
    threshold = 0.5
  )
  test_param_setting(sc, ft_binarizer, test_args)
})

test_that("ft_binarizer.tbl_spark() works", {
  sc <- testthat_spark_connection()
  df <- data_frame(id = 0:2L, feature = c(0.1, 0.8, 0.2))
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  expect_equal(
    df_tbl %>%
      ft_binarizer("feature", "binarized_feature", threshold = 0.5) %>%
      collect(),
    df %>%
      mutate(binarized_feature = c(0.0, 1.0, 0.0))
  )
})

test_that("ft_binarizer() input checking", {
  sc <- testthat_spark_connection()
  expect_error(ft_binarizer(sc, threshold = "foo"), "")
})
