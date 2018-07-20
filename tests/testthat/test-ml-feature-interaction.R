context("ml feature interaction")

test_that("ft_interaction() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_interaction)
})

test_that("ft_interaction() param setting", {
  test_requires_latest_spark()
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
    V1 = 1, V2 = 2, V3 = 3, V4 = 8, V5 = 4, V6 = 5
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)
  interacted <- df_tbl %>%
    ft_vector_assembler(paste0("V", 1:3), "vec1") %>%
    ft_vector_assembler(paste0("V", 4:6), "vec2") %>%
    ft_interaction(c("V1", "vec1", "vec2"), "interactedCol") %>%
    pull(interactedCol)
  expect_equal(
    interacted,
    list(c(8, 4, 5, 16, 8, 10, 24, 12, 15))
  )
})
