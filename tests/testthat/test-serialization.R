context("serialization")
sc <- testthat_spark_connection()

test_requires("nycflights13")
flights_tbl <- testthat_tbl("flights")

ensure_round_trip <- function(sc, data) {
  # round-trip data through Spark
  copied <- copy_to(sc, data, overwrite = TRUE)
  collected <- as.data.frame(collect(copied))

  # compare without row.names (as we don't preserve those)
  lhs <- data
  row.names(lhs) <- NULL
  rhs <- collected
  row.names(rhs) <- NULL

  expect_equal(unname(lhs), unname(rhs))
}

test_that("objects survive Spark roundtrips", {
  skip_on_cran()

  datasets <- list(mtcars = mtcars)
  for (dataset in datasets)
    ensure_round_trip(sc, dataset)
})

test_that("primitive values survive Spark roundtrips", {
  skip_on_cran()

  n <- 10
  df <- data.frame(
    int = as.integer(1:n),
    dbl = as.double(1:n),
    lgl = rep_len(c(TRUE, FALSE), n),
    chr = letters[1:10],
    stringsAsFactors = FALSE
  )

  ensure_round_trip(sc, df)

})

test_that("NA values survive Spark roundtrips", {
  skip_on_cran()

  n <- 10
  df <- data.frame(
    int = as.integer(1:n),
    dbl = as.double(1:n),
    # lgl = rep_len(c(TRUE, FALSE), n), # TODO
    # chr = letters[1:10],              # TODO
    stringsAsFactors = FALSE
  )

  df[n / 2, ] <- NA

  ensure_round_trip(sc, df)
})

test_that("data.frames with '|' can be copied", {
  skip_on_cran()

  pipes <- data.frame(
    x = c("|||", "|||", "|||"),
    y = c(1, 2, 3),
    stringsAsFactors = FALSE
  )

  ensure_round_trip(sc, pipes)
})

test_that("data.frames with many columns survive roundtrip", {
  skip_on_cran()

  n <- 1E3
  data <- as.data.frame(replicate(n, 1L, simplify = FALSE))
  names(data) <- paste("X", 1:n, sep = "")

  ensure_round_trip(sc, data)
})

test_that("data.frames with many columns don't cause Java StackOverflows", {
  skip_on_cran()

  version <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  n <- if (version >= "2.0.0") 500 else 5000
  df <- matrix(0, ncol = n, nrow = 2) %>% as_data_frame()
  sdf <- copy_to(sc, df, overwrite = TRUE)

  # the above failed with a Java StackOverflow with older versions of sparklyr
  expect_true(TRUE, info = "no Java StackOverflow on copy of large dataset")
})

test_that("'sdf_predict()', 'predict()' return same results", {
  test_requires("dplyr")

  model <- flights_tbl %>%
    na.omit() %>%
    ml_decision_tree(sched_dep_time ~ dep_time)

  predictions <- sdf_predict(model)
  n1 <- spark_dataframe(predictions) %>% invoke("count")
  n2 <- length(predict(model))

  expect_equal(n1, n2)

  lhs <- predictions %>%
    sdf_read_column("prediction")

  rhs <- predict(model)

  expect_identical(lhs, rhs)
})

test_that("copy_to() succeeds when last column contains missing / empty values", {

  df <- data.frame(
    x = c(1, 2),
    z = c(NA, ""),
    stringsAsFactors = FALSE
  )

  df_tbl <- copy_to(sc, df, serializer = "csv_string", overwrite = TRUE)

  expect_equal(sdf_nrow(df_tbl), 2)
  expect_equal(sdf_ncol(df_tbl), 2)
})

test_that("collect() can retrieve all data types correctly", {
  # https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes
  library(dplyr)

  sdate <- "from_unixtime(unix_timestamp('01-01-2010' , 'dd-MM-yyyy'))"
  rdate <- as.Date("01-01-2010", "%d-%m-%Y") %>% as.character()
  rtime <- as.POSIXct(1, origin = "1970-01-01", tz = "UTC") %>% as.character()

  hive_type <- tibble::frame_data(
    ~stype,     ~svalue,       ~rtype,   ~rvalue,
    "tinyint",       "1",   "integer",       "1",
    "smallint",      "1",   "integer",       "1",
    "integer",       "1",   "integer",       "1",
    "bigint",        "1",   "numeric",       "1",
    "float",         "1",   "numeric",       "1",
    "double",        "1",   "numeric",       "1",
    "decimal",       "1",   "numeric",       "1",
    "timestamp",     "1",   "POSIXct",     rtime,
    "date",        sdate,      "Date",     rdate,
    "string",          1, "character",       "1",
    "varchar(10)",     1, "character",       "1",
    "char(10)",        1, "character",       "1",
    "boolean",    "true",   "logical",    "TRUE"
  )

  if (spark_version(sc) < "2.2.0") {
    hive_type <- hive_type %>% filter(stype != "integer")
  }

  spark_query <- hive_type %>%
    mutate(
      query = paste0("cast(", svalue, " as ", stype, ") as ", gsub("\\(|\\)", "", stype), "_col")
    ) %>%
    pull(query) %>%
    paste(collapse = ", ") %>%
    paste("SELECT", .)

  spark_types <- DBI::dbGetQuery(sc, spark_query) %>%
    lapply(function(e) class(e)[[1]]) %>%
    as.character()

  expect_equal(
    spark_types,
    hive_type %>% pull(rtype)
  )

  spark_results <- DBI::dbGetQuery(sc, spark_query) %>%
    lapply(as.character)
  names(spark_results) <- NULL
  spark_results <- spark_results %>% unlist()

  expect_equal(
    spark_results,
    hive_type %>% pull(rvalue)
  )
})

test_that("collect() can retrieve NULL data types as NAs", {
  library(dplyr)

  hive_type <- tibble::frame_data(
        ~stype,        ~rtype,
     "tinyint",     "integer",
    "smallint",     "integer",
     "integer",     "integer",
      "bigint",     "numeric",
       "float",     "numeric",
      "double",     "numeric",
     "decimal",     "numeric",
   "timestamp",     "POSIXct",
        "date",        "Date",
      "string",   "character",
 "varchar(10)",   "character",
    "char(10)",   "character"
  )

  if (spark_version(sc) < "2.2.0") {
    hive_type <- hive_type %>% filter(stype != "integer")
  }

  spark_query <- hive_type %>%
    mutate(
      query = paste0("cast(NULL as ", stype, ") as ", gsub("\\(|\\)", "", stype), "_col")
    ) %>%
    pull(query) %>%
    paste(collapse = ", ") %>%
    paste("SELECT", .)

  spark_types <- DBI::dbGetQuery(sc, spark_query) %>%
    lapply(function(e) class(e)[[1]]) %>%
    as.character()

  expect_equal(
    spark_types,
    hive_type %>% pull(rtype)
  )

  spark_results <- DBI::dbGetQuery(sc, spark_query)

  lapply(names(spark_results), function(e) {
    expect_true(is.na(spark_results[[e]]), paste(e, "expected to be NA"))
  })
})

test_that("invoke() can roundtrip POSIXlt fields", {
  invoke_static(
    sc,
    "sparklyr.Test",
    "roundtrip",
    list(
      as.POSIXlt(Sys.time(), "GMT"),
      as.POSIXlt(Sys.time(), "GMT")
    )
  )
})

test_that("invoke() can roundtrip collect fields", {
  invoke_static(
    sc,
    "sparklyr.Test",
    "roundtrip",
    list(
      as.POSIXlt(Sys.time(), "GMT"),
      as.POSIXlt(Sys.time(), "GMT")
    )
  )
})

test_that("collect() can retrieve as.POSIXct fields with timezones", {
  tz_entries <- list(
    as.POSIXct("2018-01-01", tz = "UTC"),
    as.POSIXct("2018-01-01", tz = "GMT"),
    as.POSIXct("2018-01-01", tz = "America/Los_Angeles"),
    as.POSIXct("2018-01-01 03:33", tz = "America/Los_Angeles"),
    as.POSIXct("2018-01-01 03:33", tz = "Europe/Brussels")
  )

  collected <- sdf_len(sc, 1) %>%
    spark_apply(function(e, c) c, context = list(tzs = tz_entries)) %>%
    collect() %>%
    as.list() %>%
    unname()

  expect_true(
    all.equal(tz_entries, collected)
  )
})
