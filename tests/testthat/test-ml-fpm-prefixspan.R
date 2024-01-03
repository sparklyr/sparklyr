skip_connection("ml-fpm-prefixspan")
skip_on_livy()
skip_on_arrow_devel()

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
    lapply(seq(nrow(res)), function(i) as.list(res[i,])),
    list(
      list(sequence = list(list(list(3))), freq = 2),
      list(sequence = list(list(list(2))), freq = 3),
      list(sequence = list(list(list(1))), freq = 3),
      list(sequence = list(list(list(1, 2))), freq = 3),
      list(sequence = list(list(list(1), list(3))), freq = 2)
    )
  )
})
