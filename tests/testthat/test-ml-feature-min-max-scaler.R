context("ml feature min max scaler")

sc <- testthat_spark_connection()

test_that("ft_min_max_scaler() works properly", {
  df <- data.frame(
    id = 0:2,
    V1 = c(1, 2, 3),
    V2 = c(0.1, 1.1, 10.1),
    V3 = c(-1, 1, 3)
  )

  scaled_features <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 1:3), "features") %>%
    ft_min_max_scaler("features", "scaledFeatures") %>%
    dplyr::pull(scaledFeatures)

  expect_equal(
    scaled_features,
    list(c(0, 0, 0),
         c(0.5, 0.1, 0.5),
         c(1, 1, 1))
  )
})

