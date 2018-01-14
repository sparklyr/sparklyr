context("ml feature polynomial expansion")

sc <- testthat_spark_connection()

test_that("ft_polynoial_expansion() works properly", {
  df <- data.frame(V1 = 2, V2 = 1)
  poly_features <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(c("V1", "V2"), "features") %>%
    ft_polynomial_expansion("features", "polyFeatures", degree = 3) %>%
    dplyr::pull(polyFeatures)
  expect_equal(
    poly_features,
    list(c(2, 4, 8, 1, 2, 4, 1, 2, 1))
  )
})
