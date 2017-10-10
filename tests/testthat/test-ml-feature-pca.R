context("feature - pca")

sc <- testthat_spark_connection()

mat <- data_frame(
  V1 = c(0, 2, 4),
  V2 = c(1, 0, 0),
  V3 = c(0, 3, 0),
  V4 = c(7, 4, 6),
  V5 = c(0, 5, 7))

test_that("ft_pca() works", {
  test_requires("dplyr")

  s <- data_frame(
    PC1 = c(1.6485728230883807, -4.645104331781534, -6.428880535676489),
    PC2 = c(-4.013282700516296, -1.1167972663619026, -5.337951427775355),
    PC3 = c(-5.524543751369388, -5.524543751369387, -5.524543751369389)
  )

  mat_tbl <- testthat_tbl("mat")

  r <- mat_tbl %>%
    ft_vector_assembler(paste0("V", 1:5), "v") %>%
    ft_pca("v", "pc", k = 3) %>%
    sdf_separate_column("pc", into = paste0("PC", 1:3)) %>%
    select(starts_with("PC", ignore.case = FALSE)) %>%
    collect()

  expect_equal(s, r)
})
