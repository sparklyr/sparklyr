context("ml feature dct")

test_that("ft_dct() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_dct)
})

test_that("ft_dct() param setting", {
  test_requires_latest_spark()
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
    c(1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604),
    c(-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677),
    c(4.0,9.304453421915744,11.000000000000002,1.5579302036357163)
  )
  out1 <- df_tbl %>%
    ft_vector_assembler(as.list(paste0("f", 1:4)), "features") %>%
    ft_dct("features", "featuresDCT") %>%
    pull(featuresDCT)
  expect_equal(out1, expected_out)
})

test_that("ft_discrete_cosine_transform() backwards compat", {
  sc <- testthat_spark_connection()
  expected_out <- list(
    c(1.0,-1.1480502970952693,2.0000000000000004,-2.7716385975338604),
    c(-1.0,3.378492794482933,-7.000000000000001,2.9301512653149677),
    c(4.0,9.304453421915744,11.000000000000002,1.5579302036357163)
  )
  df_tbl <- testthat_tbl("df")
  # check backwards compatibility
  out2 <- df_tbl %>%
    ft_vector_assembler(as.list(paste0("f", 1:4)), "features") %>%
    ft_discrete_cosine_transform("features", "featuresDCT") %>%
    dplyr::pull(featuresDCT)
  expect_equal(out2, expected_out)
})
