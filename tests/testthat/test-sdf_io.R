skip_connection("sdf_io")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()

test_that("sdf_distinct works properly", {
  test_requires_version("1.3.0")
  skip_databricks_connect()

  sdf <- sdf_copy_to(sc, datasets::mtcars, overwrite = TRUE)

  expect_equal(
    dim(datasets::mtcars),
    sdf %>% sdf_distinct() %>% sdf_dim()
  )

  expect_equal(
    c(8L, 2L),
    sdf %>% sdf_distinct(cyl, gear) %>% sdf_dim()
  )

  expect_equal(
    c(3L, 1L),
    sdf %>% sdf_distinct(cyl) %>% sdf_dim()
  )

  expect_equal(
    c(8L, 2L),
    sdf %>% sdf_distinct(c("cyl", "gear")) %>% sdf_dim()
  )

  expect_equal(
    c(8L, 2L),
    sdf %>% sdf_distinct("cyl", "gear") %>% sdf_dim()
  )

  expect_equal(
    c(3L, 1L),
    sdf %>% sdf_distinct("cyl") %>% sdf_dim()
  )

  expect_equal(
    data.frame(
      vs = c(0, 0, 0, 1, 1, 1, 1),
      gear = c(3, 4, 5, 3, 4, 4, 5),
      am = c(0, 1, 1, 0, 0, 1, 1)
    ),
    sdf %>%
      sdf_distinct(vs, c("gear", "am")) %>%
      arrange_all() %>%
      collect() %>%
      as.data.frame()
  )
})

test_that("sdf_distinct() works on a spark_jobj", {
  jobj <- sdf_copy_to(sc, data.frame(x = c(1, 1, 2)), overwrite = TRUE) %>%
    spark_dataframe()
  res <- sdf_distinct(jobj)
  expect_true(inherits(res, "spark_jobj"))
  expect_equal(invoke(res, "count"), 2)
})

test_that("sdf_nrow() and sdf_ncol() report dimensions", {
  sdf <- sdf_copy_to(sc, data.frame(x = 1:3, y = 4:6), overwrite = TRUE)
  expect_equal(sdf_nrow(sdf), 3)
  expect_equal(sdf_ncol(sdf), 2)
})

test_that("sdf_seq(), sdf_len() and sdf_along() build index DataFrames", {
  # integer type, default repartition
  expect_equal(sdf_nrow(sdf_seq(sc, 1, 5)), 5)
  # integer64 type, explicit repartition
  expect_equal(
    sdf_nrow(sdf_seq(sc, 1, 5, type = "integer64", repartition = 2)),
    5
  )
  # sdf_len() delegates to sdf_seq()
  expect_equal(sdf_nrow(sdf_len(sc, 4)), 4)
  # sdf_along() takes its length from the object
  expect_equal(sdf_nrow(sdf_along(sc, letters[1:3])), 3)
})

test_that("sdf_sql() and df_from_sql() build/collect from a query", {
  expect_equal(
    sdf_sql(sc, "SELECT 1 AS a, 2 AS b") %>% collect(),
    dplyr::tibble(a = 1L, b = 2L)
  )
  expect_equal(df_from_sql(sc, "SELECT 1 AS a")$a, 1L)
})

test_that("sdf_save_table()/sdf_load_table() round-trip (deprecated)", {
  skip_databricks_connect()
  sdf <- sdf_copy_to(sc, data.frame(x = 1:3), overwrite = TRUE)
  name <- random_string("sdf_io_tbl_")
  on.exit(
    DBI::dbExecute(sc, paste0("DROP TABLE IF EXISTS `", name, "`")),
    add = TRUE
  )

  expect_warning(sdf_save_table(sdf, name, overwrite = TRUE), "deprecated")
  # append path adds another 3 rows
  suppressWarnings(sdf_save_table(sdf, name, append = TRUE))

  loaded <- suppressWarnings(sdf_load_table(sc, name))
  expect_equal(sdf_nrow(loaded), 6)
})

test_that("sdf_save_parquet()/sdf_load_parquet() round-trip (deprecated)", {
  path <- file.path(tempdir(), random_string("sdf_io_parq_"))
  on.exit(unlink(path, recursive = TRUE), add = TRUE)
  sdf <- sdf_copy_to(sc, data.frame(x = 1:3), overwrite = TRUE)

  expect_warning(sdf_save_parquet(sdf, path, overwrite = TRUE), "deprecated")
  # append path adds another 3 rows
  suppressWarnings(sdf_save_parquet(sdf, path, append = TRUE))

  loaded <- suppressWarnings(sdf_load_parquet(sc, path))
  expect_equal(sdf_nrow(loaded), 6)
})

test_clear_cache()
