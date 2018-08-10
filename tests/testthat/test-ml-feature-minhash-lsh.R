context("ml feature minhash lsh")

test_that("ft_minhash_lsh() default params", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_minhash_lsh)
})

test_that("ft_minhash_lsh() param setting", {
  test_requires_latest_spark()
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "foo",
    output_col = "bar",
    num_hash_tables = 2,
    seed = 123
  )
  test_param_setting(sc, ft_minhash_lsh, test_args)
})

test_that("ft_minhash_lsh() works properly", {
  test_requires_version("2.1.0", "LSH requires 2.1+")
  sc <- testthat_spark_connection()

  dfA <- tribble(
    ~id, ~V0, ~V1, ~V2, ~V3, ~V4, ~V5,
    0,   1,   1,   1,   0,   0,   0,
    1,   0,   0,   1,   1,   1,   0,
    2,   1,   0,   1,   0,   1,   0
  )

  dfB <- tribble(
    ~id, ~V0, ~V1, ~V2, ~V3, ~V4, ~V5,
    3,   0,   1,   0,   1,   0,   1,
    4,   0,   0,   1,   1,   0,   1,
    5,   0,   1,   1,   0,   1,   0
  )

  dfA_tbl <- sdf_copy_to(sc, dfA, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 0:5), "features")
  dfB_tbl <- sdf_copy_to(sc, dfB, overwrite = TRUE) %>%
    ft_vector_assembler(paste0("V", 0:5), "features")
  lsh <- ft_minhash_lsh(
    sc, input_col = "features", output_col = "hashes",
    num_hash_tables = 5, dataset = dfA_tbl, seed = 666)
  transformed <- lsh %>%
    ml_transform(dfA_tbl) %>%
    collect()
  expect_equal(
    transformed %>% pull(hashes) %>% head(1) %>% unlist() %>% length(),
    5
  )
  expect_equal(
    nrow(transformed),
    3
  )
  expect_equal(
    ml_approx_nearest_neighbors(lsh, dfA_tbl, c(0, 1, 0, 1, 0, 0), num_nearest_neighbors = 2) %>%
      pull(distCol),
    c(0.75, 0.75)
  )
  expect_equal(
    ml_approx_similarity_join(lsh, dfA_tbl, dfB_tbl, 0.6) %>%
      arrange(id_a, id_b) %>%
      ft_vector_assembler(c("id_a", "id_b"), "joined_pairs") %>%
      pull(joined_pairs),
    list(c(0, 5), c(1, 4), c(1, 5), c(2, 5))
  )
})
