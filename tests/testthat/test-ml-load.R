skip_connection("ml-load")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ml_load prefers Spark JSON glob and loads the model", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()

  df <- data.frame(iris)
  model <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ml_logistic_regression(Species ~ .)

  dir <- tempfile("mlp_"); dir.create(dir)
  ml_save(model, dir, overwrite = TRUE)

  loaded <- ml_load(sc, dir)
  expect_s3_class(loaded, "ml_transformer")

  out <- sdf_copy_to(sc, head(df, 1), overwrite = TRUE) %>%
    ml_transform(loaded, .) %>%
    collect()
  expect_equal(nrow(out), 1)
})

test_that("ml_load falls back to legacy textFile when Spark JSON glob fails", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()

  df <- data.frame(iris)
  model <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    ml_logistic_regression(Species ~ .)

  dir <- tempfile("mlp_"); dir.create(dir)
  ml_save(model, dir, overwrite = TRUE)

  orig <- getFromNamespace("spark_read_json", "sparklyr")
  unlockBinding("spark_read_json", asNamespace("sparklyr"))
  assignInNamespace("spark_read_json", function(...) stop("forced failure for fallback test"), "sparklyr")
  on.exit({
    assignInNamespace("spark_read_json", orig, "sparklyr")
    lockBinding("spark_read_json", asNamespace("sparklyr"))
  }, add = TRUE)

  loaded <- ml_load(sc, dir)
  expect_s3_class(loaded, "ml_transformer")

  out <- sdf_copy_to(sc, head(df, 1), overwrite = TRUE) %>%
    ml_transform(loaded, .) %>%
    collect()
  expect_equal(nrow(out), 1)
})

test_clear_cache()
