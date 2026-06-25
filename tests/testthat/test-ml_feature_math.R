skip_connection("ml_feature_math")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

test_that("ft_dct() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_dct)
})

test_that("ft_dct() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    inverse = TRUE
  )
  test_param_setting(sc, ft_dct, test_args)
})

test_that("ft_dct() works", {
  sc <- testthat_spark_connection()
  df <- data.frame(
    f1 = c(0, -1, 14),
    f2 = c(1, 2, -2),
    f3 = c(-2, 4, -5),
    f4 = c(3, -7, 1)
  )
  df_tbl <- copy_to(sc, df, overwrite = TRUE)
  expected_out <- list(
    c(1.0, -1.1480502970952693, 2.0000000000000004, -2.7716385975338604),
    c(-1.0, 3.378492794482933, -7.000000000000001, 2.9301512653149677),
    c(4.0, 9.304453421915744, 11.000000000000002, 1.5579302036357163)
  )

  expect_warning_on_arrow(
    out1 <- df_tbl %>%
      ft_vector_assembler(as.list(paste0("f", 1:4)), "features") %>%
      ft_dct("features", "featuresDCT") %>%
      pull(featuresDCT)
  )

  expect_equal(out1, expected_out)
})

test_that("ft_discrete_cosine_transform() backwards compat", {
  sc <- testthat_spark_connection()
  expected_out <- list(
    c(1.0, -1.1480502970952693, 2.0000000000000004, -2.7716385975338604),
    c(-1.0, 3.378492794482933, -7.000000000000001, 2.9301512653149677),
    c(4.0, 9.304453421915744, 11.000000000000002, 1.5579302036357163)
  )
  df_tbl <- testthat_tbl("df")
  # check backwards compatibility

  expect_warning_on_arrow(
    out2 <- df_tbl %>%
      ft_vector_assembler(as.list(paste0("f", 1:4)), "features") %>%
      ft_discrete_cosine_transform("features", "featuresDCT") %>%
      dplyr::pull(featuresDCT)
  )

  expect_equal(out2, expected_out)
})

test_that("ft_elementwise_product() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    scaling_vec = 1:5
  )
  test_param_setting(sc, ft_elementwise_product, test_args)
})

test_that("ft_elementwise_product() works", {
  sc <- testthat_spark_connection()
  test_requires_version("2.0.0", "elementwise product requires spark 2.0+")
  df <- data.frame(a = 1, b = 3, c = 5)
  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    nums <- df_tbl %>%
      ft_vector_assembler(list("a", "b", "c"), output_col = "features") %>%
      ft_elementwise_product("features", "multiplied", c(2, 4, 6)) %>%
      pull(multiplied) %>%
      purrr::list_c(ptype = numeric())
  )

  expect_identical(
    nums,
    c(1, 3, 5) * c(2, 4, 6)
  )

  ewp <- ft_elementwise_product(
    sc,
    "features",
    "multiplied",
    scaling_vec = c(1, 3, 5)
  )

  expect_equal(
    ml_params(
      ewp,
      list(
        "input_col",
        "output_col",
        "scaling_vec"
      )
    ),
    list(
      input_col = "features",
      output_col = "multiplied",
      scaling_vec = c(1, 3, 5)
    )
  )
})

test_that("ft_interaction() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_cols = c("foo1", "foo2"),
    output_col = "bar"
  )
  test_param_setting(sc, ft_interaction, test_args)
})

test_that("ft_interaction() works properly", {
  sc <- testthat_spark_connection()
  df <- data.frame(
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 8,
    V5 = 4,
    V6 = 5
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    interacted <- df_tbl %>%
      ft_vector_assembler(paste0("V", 1:3), "vec1") %>%
      ft_vector_assembler(paste0("V", 4:6), "vec2") %>%
      ft_interaction(c("V1", "vec1", "vec2"), "interactedCol") %>%
      pull(interactedCol)
  )

  expect_equal(
    interacted,
    list(c(8, 4, 5, 16, 8, 10, 24, 12, 15))
  )
})

test_clear_cache()
