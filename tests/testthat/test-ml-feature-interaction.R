skip_connection("ml-feature-interaction")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
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
    V1 = 1, V2 = 2, V3 = 3, V4 = 8, V5 = 4, V6 = 5
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

