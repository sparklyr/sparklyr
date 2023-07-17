skip_connection("ml-feature-robust-scaler")
skip_on_livy()
skip_on_arrow_devel()

test_that("ft_robust_scaler() works properly", {
  sc <- testthat_spark_connection()
  test_requires_version("3.0.0", "ft_robust_scaler requires Spark 3.0.0+")
  df <- data.frame(
    id = 1:5,
    V1 = c(0.0, 1.0, 2.0, 3.0, 4.0),
    V2 = c(0.0, -1.0, -2.0, -3.0, -4.0)
  )

  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    r_s <- df_tbl %>%
      ft_vector_assembler(paste0("V", 1:2), "features") %>%
      ft_robust_scaler("features", "scaled") %>%
      pull(scaled)
  )

  expect_equal(
    r_s,
    list(
      c(-1, 1),
      c(-0.5, 0.5),
      c(0, 0),
      c(0.5, -0.5),
      c(1, -1)
    )
  )
})
