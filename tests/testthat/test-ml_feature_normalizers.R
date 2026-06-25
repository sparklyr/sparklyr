skip_connection("ml_feature_normalizers")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

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
  df <- tibble(id = 0:2L, feature = c(0.1, 0.8, 0.2))
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

test_that("ft_bucketizer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_bucketizer)
})

test_that("ft_bucketizer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "x",
    output_col = "y",
    splits = c(-1, 34, 100),
    handle_invalid = "keep"
  )
  test_param_setting(sc, ft_bucketizer, test_args)

  test_args2 <- list(
    input_cols = c("x1", "x2"),
    output_cols = c("y1", "y2"),
    splits_array = list(c(-1, 34, 100), c(-5, 0, 2)),
    handle_invalid = "keep"
  )
  test_param_setting(sc, ft_bucketizer, test_args2)
})

test_that("ft_bucketizer() works properly", {
  mtcars_tbl <- testthat_tbl("mtcars")
  expect_identical(
    mtcars_tbl %>%
      select(drat) %>%
      ft_bucketizer(
        "drat",
        "drat_out",
        splits = c(-Inf, 2, 4, Inf)
      ) %>%
      colnames(),
    c("drat", "drat_out")
  )
})

test_that("ft_bucketizer() can mutate multiple columns", {
  test_requires_version("2.3.0", "multiple column support requires 2.3+")
  mtcars_tbl <- testthat_tbl("mtcars")
  expect_identical(
    mtcars_tbl %>%
      select(drat, hp) %>%
      ft_bucketizer(
        input_cols = c("drat", "hp"),
        output_cols = c("drat_out", "hp_out"),
        splits_array = list(c(-Inf, 2, 4, Inf), c(-Inf, 90, 120, Inf))
      ) %>%
      colnames(),
    c("drat", "hp", "drat_out", "hp_out")
  )
})

test_that("ft_normalizer() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_normalizer)
})

test_that("ft_normalizer() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    p = 2
  )
  test_param_setting(sc, ft_normalizer, test_args)
})

test_that("ft_normalizer works properly", {
  sc <- testthat_spark_connection()
  df <- tribble(
    ~id , ~V1 , ~V2  , ~V3 ,
      0 ,   1 ,  0.5 ,  -1 ,
      1 ,   2 ,  1   ,   1 ,
      2 ,   4 , 10   ,   2
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:3), "features")

  expect_warning_on_arrow(
    norm_data1 <- df_tbl %>%
      ft_normalizer("features", "normFeatures", p = 1) %>%
      pull(normFeatures)
  )

  expect_equal(
    norm_data1,
    list(
      c(0.4, 0.2, -0.4),
      c(0.5, 0.25, 0.25),
      c(0.25, 0.625, 0.125)
    )
  )

  expect_warning_on_arrow(
    norm_data2 <- df_tbl %>%
      ft_normalizer("features", "normFeatures", p = Inf) %>%
      pull(normFeatures)
  )

  expect_equal(
    norm_data2,
    list(
      c(1, 0.5, -1),
      c(1, 0.5, 0.5),
      c(0.4, 1, 0.2)
    )
  )
})

test_that("ft_normalizer errors for bad p", {
  sc <- testthat_spark_connection()
  expect_error(
    ft_normalizer(sc, "features", "normFeatures", p = 0.5),
    "`p` must be at least 1\\."
  )
})

test_that("ft_polynomial_expansion() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_polynomial_expansion)
})

test_that("ft_polynomial_expansion() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    degree = 4
  )
  test_param_setting(sc, ft_polynomial_expansion, test_args)
})

test_that("ft_polynomial_expansion() works properly", {
  sc <- testthat_spark_connection()
  df <- data.frame(V1 = 2, V2 = 1)

  expect_warning_on_arrow(
    poly_features <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
      ft_vector_assembler(c("V1", "V2"), "features") %>%
      ft_polynomial_expansion("features", "polyFeatures", degree = 3) %>%
      pull(polyFeatures)
  )

  expect_equal(
    poly_features,
    list(c(2, 4, 8, 1, 2, 4, 1, 2, 1))
  )
})

test_clear_cache()
