context("ml feature lsh")

sc <- testthat_spark_connection()

dfA <- data.frame(
  id = 1:4,
  V1 = c(1, 1, -1, -1),
  V2 = c(1, -1, -1, 1)
)

dfB <- data.frame(
  id = 4:7,
  V1 = c(1, -1, 0, 0),
  V2 = c(0, 0, 1, -1)
)

dfA_tbl <- sdf_copy_to(sc, dfA) %>%
  ft_vector_assembler(c("V1", "V2"), "features")
dfB_tbl <- sdf_copy_to(sc, dfB) %>%
  ft_vector_assembler(c("V1", "V2"), "features")

test_that("ft_bucketed_random_projection_lsh() works properly", {
  test_requires_version("2.1.0", "LSH requires 2.1+")
  lsh <- ft_bucketed_random_projection_lsh(
    sc, input_col = "features", output_col = "hashes",
    bucket_length = 2, num_hash_tables = 3, dataset = dfA_tbl)
  transformed <- lsh %>%
    ml_transform(dfA_tbl) %>%
    collect()
  expect_equal(
    colnames(transformed),
    c("id", "V1", "V2", "features", "hashes")
  )
  expect_equal(
    sdf_nrow(transformed),
    4
  )
})


test_that("ft_bucketed_random_projection_lsh() param setting", {
  test_requires_version("2.1.0", "LSH requires 2.1+")
  args <- list(
    x = sc, input_col = "input", output_col = "output",
    bucket_length = 3, num_hash_tables = 2, seed = 56
  )
  estimator <- do.call(ft_bucketed_random_projection_lsh, args)
  expect_equal(ml_params(estimator, names(args)[-1]), args[-1])
})
