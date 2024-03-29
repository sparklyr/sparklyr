skip_connection("ml-feature-random-projection-lsh")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_bucketed_random_projection_lsh() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_bucketed_random_projection_lsh)
})

test_that("ft_bucketed_random_projection_lsh() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    input_col = "x",
    output_col = "y",
    bucket_length = 4,
    num_hash_tables = 2,
    seed = 42
  )
  test_param_setting(sc, ft_bucketed_random_projection_lsh, test_args)
})

test_that("ft_bucketed_random_projection_lsh() works properly", {
  test_requires_version("2.1.0", "LSH requires 2.1+")
  sc <- testthat_spark_connection()

  dfA <- data.frame(
    id = 0:3,
    V1 = c(1, 1, -1, -1),
    V2 = c(1, -1, -1, 1)
  )

  dfB <- data.frame(
    id = 4:7,
    V1 = c(1, -1, 0, 0),
    V2 = c(0, 0, 1, -1)
  )

  dfA_tbl <- sdf_copy_to(sc, dfA, overwrite = TRUE) %>%
    ft_vector_assembler(c("V1", "V2"), "features")
  dfB_tbl <- sdf_copy_to(sc, dfB, overwrite = TRUE) %>%
    ft_vector_assembler(c("V1", "V2"), "features")

  lsh <- ft_bucketed_random_projection_lsh(
    sc,
    input_col = "features", output_col = "hashes",
    bucket_length = 2, num_hash_tables = 3, seed = 666
  ) %>%
    ml_fit(dfA_tbl)

  expect_warning_on_arrow(
    transformed <- lsh %>%
      ml_transform(dfA_tbl) %>%
      dplyr::collect()
  )

  expect_equal(
    transformed %>% dplyr::pull(hashes) %>% head(1) %>% unlist() %>% length(),
    3
  )
  expect_equal(
    nrow(transformed),
    4
  )
  expect_equal(
    ml_approx_nearest_neighbors(lsh, dfA_tbl, c(1, 0), num_nearest_neighbors = 2) %>%
      dplyr::pull(id),
    c(0, 1)
  )

  expect_warning_on_arrow(
    s_j <- ml_approx_similarity_join(lsh, dfA_tbl, dfB_tbl, 2) %>%
      dplyr::arrange(id_a, id_b) %>%
      ft_vector_assembler(c("id_a", "id_b"), "joined_pairs") %>%
      dplyr::pull(joined_pairs)
  )

  expect_equal(
    s_j,
    list(c(0, 4), c(0, 6), c(1, 4), c(1, 7), c(2, 5), c(2, 7), c(3, 5), c(3, 6))
  )
})

test_clear_cache()

