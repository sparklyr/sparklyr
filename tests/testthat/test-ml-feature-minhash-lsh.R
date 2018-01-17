context("ml feature minhash lsh")

sc <- testthat_spark_connection()

dfA <- dplyr::tribble(
  ~id, ~V0, ~V1, ~V2, ~V3, ~V4, ~V5,
  0,   1,   1,   1,   0,   0,   0,
  1,   0,   0,   1,   1,   1,   0,
  2,   1,   0,   1,   0,   1,   0
)

dfB <- dplyr::tribble(
  ~id, ~V0, ~V1, ~V2, ~V3, ~V4, ~V5,
  3,   0,   1,   0,   1,   0,   1,
  4,   0,   0,   1,   1,   0,   1,
  5,   0,   1,   1,   0,   1,   0
)

dfA_tbl <- sdf_copy_to(sc, dfA, overwrite = TRUE) %>%
  ft_vector_assembler(paste0("V", 0:5), "features")
dfB_tbl <- sdf_copy_to(sc, dfB, overwrite = TRUE) %>%
  ft_vector_assembler(paste0("V", 0:5), "features")

test_that("ft_minhash_lsh() works properly", {
  test_requires_version("2.1.0", "LSH requires 2.1+")
  lsh <- ft_minhash_lsh(
    sc, input_col = "features", output_col = "hashes",
    num_hash_tables = 5, dataset = dfA_tbl, seed = 666)
  transformed <- lsh %>%
    ml_transform(dfA_tbl) %>%
    dplyr::collect()
  expect_equal(
    transformed %>% dplyr::pull(hashes) %>% head(1) %>% unlist() %>% length(),
    5
  )
  expect_equal(
    nrow(transformed),
    3
  )
  expect_equal(
    ml_approx_nearest_neighbors(lsh, dfA_tbl, c(0, 1, 0, 1, 0, 0), num_nearest_neighbors = 2) %>%
      dplyr::pull(distCol),
    c(0.75, 0.75)
  )
  expect_equal(
    ml_approx_similarity_join(lsh, dfA_tbl, dfB_tbl, 0.6) %>%
      dplyr::arrange(id_a, id_b) %>%
      ft_vector_assembler(c("id_a", "id_b"), "joined_pairs") %>%
      dplyr::pull(joined_pairs),
    list(c(0, 5), c(1, 4), c(1, 5), c(2, 5))
  )
})


test_that("ft_minhash_lsh() param setting", {
  test_requires_version("2.1.0", "LSH requires 2.1+")
  args <- list(
    x = sc, input_col = "input", output_col = "output",
    num_hash_tables = 2, seed = 56
  )
  estimator <- do.call(ft_minhash_lsh, args)
  expect_equal(ml_params(estimator, names(args)[-1]), args[-1])
})
