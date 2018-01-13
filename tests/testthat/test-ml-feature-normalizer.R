context("ml feature normalizer")

sc <- testthat_spark_connection()

test_that("ft_normalizer works properly", {
  df <- dplyr::tribble(
    ~id, ~V1, ~V2, ~V3,
    0,   1,   0.5, -1,
    1,   2,   1,   1,
    2,   4,   10,  2
  )
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:3), "features")

  norm_data1 <- df_tbl %>%
    ft_normalizer("features", "normFeatures", p = 1) %>%
    dplyr::pull(normFeatures)

  expect_equal(
    norm_data1,
    list(c(0.4, 0.2, -0.4),
         c(0.5, 0.25, 0.25),
         c(0.25, 0.625, 0.125))
  )

  norm_data2 <- df_tbl %>%
    ft_normalizer("features", "normFeatures", p = Inf) %>%
    dplyr::pull(normFeatures)

  expect_equal(
    norm_data2,
    list(c(1, 0.5, -1),
         c(1, 0.5, 0.5),
         c(0.4, 1, 0.2))
  )
})

test_that("ft_normalizer errors for bad p", {
  expect_error(
    ft_normalizer(sc, "features", "normFeatures", p = 0.5),
    "'p' must be greater than or equal to 1")
})
