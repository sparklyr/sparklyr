context("describe")

sc <- testthat_spark_connection()

test_that("sdf_collect() works properly", {
  mtcars_tbl <- testthat_tbl("mtcars")
  mtcars_data <- sdf_collect(mtcars_tbl)

  expect_equivalent(mtcars, mtcars_data)
})

test_that("sdf_collect() works with nested lists", {
  if (spark_version(sc) < "2.4")
    skip("serializing nested list into Spark StructType is only supported in Spark 2.4+")

  df <- tibble::tibble(
    a = list(c(1, 2, 3), c(4, 5), c(6)),
    b = list(c("foo"), c("bar", "foobar"), c("a", "b", "c"))
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  res <- sdf_collect(sdf)

  expect_equivalent(df$a, res$a)
  expect_equivalent(df$b, sapply(res$b, function(e) do.call(c, e)))
})

test_that("sdf_collect() works with nested named lists", {
  if (spark_version(sc) < "2.4")
    skip("serializing nested named list into Spark StructType is only supported in Spark 2.4+")

  df <- tibble::tibble(
    x = list(c(a = 1, b = 2), c(a = 3, b = 4), c(a = 5, b = 6)),
    y = list(c(a = "foo", b = "bar"), c(a = "a", b = "b"), c(a = "", b = "")),
    z = list(list(a = list(c = "foo", d = "bar", e = list("e", NULL)), b = "b"))
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  res <- sdf_collect(sdf)

  for (col in colnames(df))
    expect_equivalent(lapply(df[[col]], as.list), res[[col]])
})

test_that("sdf_collect() works with structs inside arrays", {
  if (spark_version(sc) < "2.3")
    skip("deserializing Spark StructType into named list is only supported in Spark 2.3+")

  json <- file("test.json", "w+")
  cat(paste0("{",
             "\"text\":\"t e x t\",",
             "\"sentences\":[",
             "{\"type\":\"document1\",\"begin\":0,\"end\":58,\"result\":\"French\",\"metadata\":{\"sentence\":1,\"embeddings\":[1,2,3]}},",
             "{\"type\":\"document2\",\"begin\":59,\"end\":118,\"result\":\"English\",\"metadata\":{\"sentence\":2,\"embeddings\":[4,5,6]}}",
             "]",
             "}"),
      file = json)
  close(json)

  sentences <- spark_read_json(sc, name = "sentences", path = "test.json", overwrite = TRUE)
  sentences_local <- sdf_collect(sentences)

  expect_equal(sentences_local$text, c("t e x t"))

  expected <- list(
      list(begin = 0,
           end = 58,
           metadata = list(embeddings = c(1, 2, 3), sentence = 1),
           result = "French",
           type = "document1"),
      list(begin = 59,
           end = 118,
           metadata = list(embeddings = c(4, 5, 6), sentence = 2),
           result = "English",
           type = "document2")
  )
  expect_equal(sentences_local$sentences, list(expected))

  file.remove("test.json")
})

test_that("sdf_collect() works with structs inside nested arrays", {
  if (spark_version(sc) < "2.3")
    skip("deserializing Spark StructType into named list is only supported in Spark 2.3+")
  if (spark_version(sc) < "2.4")
    skip("to_json on nested arrays is only supported in Spark 2.4+")

  json <- file("test.json", "w+")
  cat(paste0("{",
             "\"text\":\"t e x t\",",
             "\"sentences\":[",
             "[{\"type\":\"document1\",\"begin\":0,\"end\":58,\"result\":\"French\",\"metadata\":{\"sentence\":1,\"embeddings\":[1,2,3]}}],",
             "[{\"type\":\"document2\",\"begin\":59,\"end\":118,\"result\":\"English\",\"metadata\":{\"sentence\":2,\"embeddings\":[4,5,6]}}]",
             "]",
             "}"),
      file = json)
  close(json)

  sentences <- spark_read_json(sc, name = "sentences", path = "test.json", overwrite = TRUE)
  sentences_local <- sdf_collect(sentences)

  expect_equal(sentences_local$text, c("t e x t"))

  expected <- list(
      list(list(begin = 0,
           end = 58,
           metadata = list(embeddings = c(1, 2, 3), sentence = 1),
           result = "French",
           type = "document1")),
      list(list(begin = 59,
           end = 118,
           metadata = list(embeddings = c(4, 5, 6), sentence = 2),
           result = "English",
           type = "document2"))
  )
  expect_equal(sentences_local$sentences, list(expected))

  file.remove("test.json")
})

test_that("sdf_collect() supports callback", {
  if (spark_version(sc) < "2.0") skip("batch collection requires Spark 2.0")

  batch_count <- 0
  row_count <- 0

  sdf_len(sc, 10, repartition = 2) %>%
    sdf_collect(callback = function(df) {
      batch_count <<- batch_count + 1
      row_count <<- row_count + nrow(df)
    })

  expect_equal(
    batch_count,
    ifelse("arrow" %in% .packages(), 2, 1)
  )

  expect_equal(
    row_count,
    10
  )

  last_idx <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    sdf_collect(callback = function(df, idx) {
      last_idx <<- idx
    })

  expect_equal(
    last_idx,
    ifelse("arrow" %in% .packages(), 2, 1)
  )
})

test_that("sdf_collect() supports callback expression", {
  if (spark_version(sc) < "2.0") skip("batch collection requires Spark 2.0")

  row_count <- 0
  sdf_len(sc, 10, repartition = 2) %>%
    collect(callback = ~(row_count <<- row_count + nrow(.x)))

  expect_equal(
    10,
    row_count
  )
})
