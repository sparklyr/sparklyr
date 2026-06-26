skip_connection("ml_fpm")
skip_on_livy()
skip_on_arrow_devel()

test_that("ml_fpgrowth() default params", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_fpgrowth)
})

test_that("ml_fpgrowth() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    items_col = "wefwef",
    min_confidence = 0.7,
    min_support = 0.4,
    prediction_col = "waef"
  )
  test_param_setting(sc, ml_fpgrowth, test_args)
})

test_that("ml_fpgrowth() works properly", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  test_requires_version("2.2.0", "fpgrowth requires spark 2.2.0+")

  df <- data.frame(items = c("1 2 5", "1 2 3 5", "1 2"))
  df_tbl <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    mutate(items = split(items, " "))
  fp_model <- df_tbl %>%
    ml_fpgrowth(min_support = 0.5, min_confidence = 0.6)
  expect_equal(
    ml_freq_itemsets(fp_model) %>%
      sdf_nrow(),
    7
  )

  expect_equal(
    ml_association_rules(fp_model) %>%
      sdf_nrow(),
    9
  )

  expect_identical(
    fp_model %>%
      ml_transform(df_tbl) %>%
      pull(prediction) %>%
      lapply(as.list),
    list(list(), list(), list("5"))
  )
})

test_that("ml_prefixspan() works as expected", {
  test_requires_version("2.4.0", "prefixspan requires spark 2.4.0+")
  skip_on_arrow()

  sc <- testthat_spark_connection()

  df <- dplyr::tibble(
    seq = list(
      list(list(1, 2), list(3)),
      list(list(1), list(3, 2), list(1, 2)),
      list(list(1, 2), list(5)),
      list(list(6))
    )
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  prefix_span_model <- ml_prefixspan(
    sc,
    seq_col = "seq",
    min_support = 0.5,
    max_pattern_length = 5,
    max_local_proj_db_size = 32000000
  )
  res <- prefix_span_model$frequent_sequential_patterns(sdf) %>%
    collect()

  expect_equal(nrow(res), 5)
  expect_setequal(
    lapply(seq(nrow(res)), function(i) as.list(res[i, ])),
    list(
      list(sequence = list(list(list(3))), freq = 2),
      list(sequence = list(list(list(2))), freq = 3),
      list(sequence = list(list(list(1))), freq = 3),
      list(sequence = list(list(list(1, 2))), freq = 3),
      list(sequence = list(list(list(1), list(3))), freq = 2)
    )
  )
})

test_clear_cache()
