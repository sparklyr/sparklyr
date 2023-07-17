skip_connection("sdf-collect")
skip_on_livy()

sc <- testthat_spark_connection()

test_that("sdf_collect() works properly", {
  mtcars_tbl <- testthat_tbl("mtcars")
  mtcars_data <- sdf_collect(mtcars_tbl)

  expect_equivalent(mtcars, mtcars_data)
})

test_that("sdf_collect() can collect the first n rows of a Spark dataframe", {
  mtcars_tbl <- testthat_tbl("mtcars", repartition = 5)

  mtcars_data <- sdf_collect(mtcars_tbl, n = 10)

  expect_equivalent(mtcars[1:10,], mtcars_data)
})

test_that("sdf_collect() works properly with impl = \"row-wise-iter\"", {
  mtcars_tbl <- testthat_tbl("mtcars")
  mtcars_data <- sdf_collect(mtcars_tbl, impl = "row-wise-iter")

  expect_equivalent(mtcars, mtcars_data)
})

test_that("sdf_collect() works properly with impl = \"column-wise\"", {
  mtcars_tbl <- testthat_tbl("mtcars")
  mtcars_data <- sdf_collect(mtcars_tbl, impl = "column-wise")

  expect_equivalent(mtcars, mtcars_data)
})

test_that("sdf_collect() works with nested lists", {
  if (spark_version(sc) < "2.4") {
    skip("serializing nested list into Spark StructType is only supported in Spark 2.4+")
  }

  df <- tibble::tibble(
    a = list(c(1, 2, 3), c(4, 5), c(6)),
    b = list(c("foo"), c("bar", "foobar"), c("a", "b", "c"))
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  res <- sdf_collect(sdf)

  expect_equivalent(df$a, res$a)
  expect_equivalent(df$b, sapply(res$b, function(x) do.call(c, as.list(x))))
})

test_that("sdf_collect() works with nested named lists", {
  if (spark_version(sc) < "2.4") {
    skip("serializing nested named list into Spark StructType is only supported in Spark 2.4+")
  }

  df <- tibble::tibble(
    x = list(c(a = 1, b = 2), c(a = 3, b = 4), c(a = 5, b = 6)),
    y = list(c(a = "foo", b = "bar"), c(a = "a", b = "b"), c(a = "", b = "")),
    z = list(list(a = list(c = "foo", d = "bar", e = list("e")), b = "b"))
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_warning_on_arrow(
    res <- sdf_collect(sdf)
  )

  for (col in colnames(df)) {
    expect_equivalent(lapply(df[[col]], as.list), res[[col]])
  }
})

test_that("sdf_collect() works with boolean array column", {
  skip_on_arrow()

  sdf <- dplyr::tbl(
    sc,
    dplyr::sql("
      SELECT
        *
      FROM
        VALUES (ARRAY(FALSE)),
               (ARRAY(TRUE, FALSE)),
               NULL,
               (ARRAY(TRUE, NULL, FALSE))
      AS TAB(`arr`)
    ")
  )
  expect_equivalent(
    sdf %>% sdf_collect(),
    tibble::tibble(arr = list(FALSE, c(TRUE, FALSE), NA, c(TRUE, NA, FALSE)))
  )
})

test_that("sdf_collect() works with byte array column", {
  skip_on_arrow()

  sdf <- dplyr::tbl(
    sc,
    dplyr::sql("
      SELECT
        *
      FROM
        VALUES ARRAY(CAST(97 AS BYTE), CAST(98 AS BYTE)),
               ARRAY(CAST(98 AS BYTE), CAST(99 AS BYTE), CAST(0 AS BYTE), CAST(100 AS BYTE)),
               NULL,
               ARRAY(CAST(101 AS BYTE))
      AS TAB(`arr`)
    ")
  )
  df <- sdf %>% collect()

  expect_equal(
    df$arr,
    list(
      charToRaw("ab"),
      c(charToRaw("bc"), as.raw(0), charToRaw("d")),
      NA,
      charToRaw("e")
    )
  )
})

test_that("sdf_collect() works with integral array column", {
  skip_on_arrow()

  for (type in c("SHORT", "INT")) {
    sdf <- dplyr::tbl(
      sc,
      dplyr::sql(
        glue::glue(
          "
          SELECT
            *
          FROM
            VALUES ARRAY(CAST(1 AS {type})),
                   ARRAY(CAST(2 AS {type}), CAST(3 AS {type})),
                   NULL,
                   ARRAY(CAST(4 AS {type}), NULL, CAST(5 AS {type}))
          AS TAB(`arr`)
          ",
          type = type
        )
      )
    )

    expect_equivalent(
      sdf %>% sdf_collect(),
      tibble::tibble(arr = list(1L, c(2L, 3L), NA, c(4L, NA, 5L)))
    )
  }
})

test_that("sdf_collect() works with numeric array column", {
  skip_on_arrow()

  for (type in c("FLOAT", "LONG", "DOUBLE")) {
    sdf <- dplyr::tbl(
      sc,
      dplyr::sql(
        glue::glue(
          "
          SELECT
            *
          FROM
            VALUES ARRAY(CAST(1 AS {type})),
                   ARRAY(CAST(2 AS {type}), CAST(3 AS {type})),
                   NULL,
                   ARRAY(CAST(4 AS {type}), NULL, CAST('NaN' AS {type}), CAST(5 AS {type}))
          AS TAB(`arr`)
          ",
          type = type
        )
      )
    )

    expect_equivalent(
      sdf %>% sdf_collect(),
      tibble::tibble(arr = list(1, c(2, 3), NA, c(4, NA, NaN, 5)))
    )
  }
})

test_that("sdf_collect() works with string array column", {
  skip_on_arrow()

  sdf <- dplyr::tbl(
    sc,
    dplyr::sql("
      SELECT
        *
      FROM
        VALUES ARRAY('ab'), ARRAY('bcd', 'e'), NULL, ARRAY('fghi', NULL, 'jk')
      AS TAB(`arr`)
    ")
  )
  expect_equivalent(
    sdf %>% sdf_collect(),
    tibble::tibble(arr = list("ab", c("bcd", "e"), NA, c("fghi", NA, "jk")))
  )
})

test_that("sdf_collect() works with temporal array column", {
  skip_on_arrow()

  for (type in c("DATE", "TIMESTAMP")) {
    sdf <- dplyr::tbl(
      sc,
      dplyr::sql(
        glue::glue(
          "
          SELECT
            *
          FROM
            VALUES ARRAY(CAST('1970-01-01' AS {type})),
                   ARRAY(CAST('1970-01-02' AS {type}), CAST('1970-01-03' AS {type})),
                   NULL,
                   ARRAY(CAST('1970-01-04' AS {type}), NULL, CAST('1970-01-05' AS {type}))
          AS TAB(`arr`)
          ",
          type = type
        )
      )
    )
    cast_fn <- if (type == "DATE") as.Date else as.POSIXct

    expect_equivalent(
      sdf %>% dplyr::pull(arr),
      list(
        cast_fn("1970-01-01", tz = ""),
        cast_fn(c("1970-01-02", "1970-01-03"), tz = ""),
        NA,
        cast_fn(c("1970-01-04", NA, "1970-01-05"), tz = "")
      )
    )
  }
})

test_that("sdf_collect() works with struct array column", {
  if (spark_version(sc) < "2.3") {
    skip("deserializing Spark StructType into named list is only supported in Spark 2.3+")
  }

  jsonFilePath <- get_test_data_path("struct-inside-arrays.json")

  sentences <- spark_read_json(sc, name = "sentences", path = jsonFilePath, overwrite = TRUE)

  expect_warning_on_arrow(
    sentences_local <- sdf_collect(sentences)
  )

  expect_equal(sentences_local$text, c("t e x t"))

  expected <- list(
    list(
      begin = 0L,
      end = 58L,
      metadata = list(embeddings = c(1L, 2L, 3L), sentence = 1L),
      result = "French",
      type = "document1"
    ),
    list(
      begin = 59L,
      end = 118L,
      metadata = list(embeddings = c(4L, 5L, 6L), sentence = 2L),
      result = "English",
      type = "document2"
    )
  )
  expect_equal(sentences_local$sentences, list(expected))
})

test_that("sdf_collect() works with structs inside nested arrays", {
  if (spark_version(sc) < "2.3") {
    skip("deserializing Spark StructType into named list is only supported in Spark 2.3+")
  }
  if (spark_version(sc) < "2.4") {
    skip("to_json on nested arrays is only supported in Spark 2.4+")
  }

  jsonFilePath <- get_test_data_path("struct-inside-nested-arrays.json")

  sentences <- spark_read_json(sc, name = "sentences", path = jsonFilePath, overwrite = TRUE)

  expect_warning_on_arrow(
    sentences_local <- sdf_collect(sentences)
  )

  expect_equal(sentences_local$text, c("t e x t"))

  expected <- list(
    list(list(
      begin = 0,
      end = 58,
      metadata = list(embeddings = c(1, 2, 3), sentence = 1),
      result = "French",
      type = "document1"
    )),
    list(list(
      begin = 59,
      end = 118,
      metadata = list(embeddings = c(4, 5, 6), sentence = 2),
      result = "English",
      type = "document2"
    ))
  )
  expect_equal(sentences_local$sentences, list(expected))
})

test_that("sdf_collect() supports callback", {
  if (spark_version(sc) < "2.0") skip("batch collection requires Spark 2.0")

  batch_count <- 0
  row_count <- 0

  df <- tibble(id = seq(1, 10), val = lapply(seq(1, 10), function(x) list(a = x, b = as.character(x))))
  sdf <- sdf_copy_to(sc, df, repartition = 2, overwrite = TRUE)

  collected <- list()

  expect_warning_on_arrow(
    sdf %>%
      sdf_collect(callback = function(batch_df) {
        batch_count <<- batch_count + 1
        row_count <<- row_count + nrow(batch_df)
        collected <<- append(collected, batch_df$val)
      })
  )

  expect_equal(
    batch_count,
    1
  )

  expect_equal(
    row_count,
    10
  )

  if (spark_version(sc) >= "2.4") {
    expect_equal(
      collected,
      df$val
    )
  }

  if (spark_version(sc) >= "2.4") {
    collected <- list()

    expect_warning_on_arrow(
      sdf %>%
        sdf_collect(callback = function(batch_df, idx) {
          collected <<- append(collected, batch_df$val)
        })
    )

    expect_equal(
      collected,
      df$val
    )
  }

  sdf_len_batch_count <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    sdf_collect(callback = function(df) {
      sdf_len_batch_count <<- sdf_len_batch_count + 1
    })

  expect_equal(
    sdf_len_batch_count,
    ifelse("arrow" %in% .packages(), 2, 1)
  )

  sdf_len_last_idx <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    sdf_collect(callback = function(df, idx) {
      sdf_len_last_idx <<- idx
    })

  expect_equal(
    sdf_len_last_idx,
    ifelse("arrow" %in% .packages(), 2, 1)
  )
})

test_that("sdf_collect() supports callback expression", {
  if (spark_version(sc) < "2.0") skip("batch collection requires Spark 2.0")

  row_count <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    collect(callback = ~ (row_count <<- row_count + nrow(.x)))

  expect_equal(
    10,
    row_count
  )
})

test_that("sdf_collect() preserves NA_real_", {
  df <- tibble::tibble(x = c(NA_real_, 3.14, 0.142857))
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)

  expect_equal(sdf %>% collect(), df)
})
