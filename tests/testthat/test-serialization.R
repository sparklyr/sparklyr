context("serialization")
sc <- testthat_spark_connection()

test_requires("nycflights13")

flights_small <- flights %>% dplyr::sample_n(10000)
flights_tbl <- testthat_tbl("flights_small")

logical_nas <- data_frame(bools = c(T, NA, F))
logical_nas_tbl <- testthat_tbl("logical_nas")

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
  datasets <- list(mtcars = mtcars)
  for (dataset in datasets)
    ensure_round_trip(sc, dataset)
})

test_that("primitive values survive Spark roundtrips", {
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
  pipes <- data.frame(
    x = c("|||", "|||", "|||"),
    y = c(1, 2, 3),
    stringsAsFactors = FALSE
  )

  ensure_round_trip(sc, pipes)
})

test_that("data.frames with many columns survive roundtrip", {
  skip_slow("takes too long to measure coverage")

  n <- 1E3
  data <- as.data.frame(replicate(n, 1L, simplify = FALSE))
  names(data) <- paste("X", 1:n, sep = "")

  ensure_round_trip(sc, data)
})

test_that("data.frames with many columns don't cause Java StackOverflows", {
  version <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  n <- if (version >= "2.0.0") 500 else 5000
  df <- matrix(0, ncol = n, nrow = 2) %>% as_tibble()
  sdf <- copy_to(sc, df, overwrite = TRUE)

  # the above failed with a Java StackOverflow with older versions of sparklyr
  expect_true(TRUE, info = "no Java StackOverflow on copy of large dataset")
})

test_that("'ml_predict()', 'predict()' return same results", {
  test_requires("dplyr")

  model <- flights_tbl %>%
    na.omit() %>%
    ml_decision_tree(sched_dep_time ~ dep_time)

  predictions <- ml_predict(model)
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

  utime <- as.numeric(as.POSIXct("2010-01-01 01:01:10", origin = "1970-01-01", tz = "UTC"))
  sdate <- "from_unixtime(unix_timestamp('01-01-2010' , 'dd-MM-yyyy'))"
  rdate <- as.Date("01-01-2010", "%d-%m-%Y") %>% as.character()
  stime <- paste0("to_utc_timestamp(from_unixtime(", utime, "), 'UTC')")
  rtime <- "2010-01-01 01:01:10"
  atime <- as.character(as.POSIXct(utime, origin = "1970-01-01"))

  arrow_compat <- using_arrow()

  hive_type <- tibble::frame_data(
    ~stype,      ~svalue,      ~rtype,   ~rvalue,      ~atype,    ~avalue,
    "tinyint",       "1",   "integer",       "1",   "integer",        "1",
    "smallint",      "1",   "integer",       "1",   "integer",        "1",
    "integer",       "1",   "integer",       "1",   "integer",        "1",
    "bigint",        "1",   "numeric",       "1", "integer64",        "1",
    "float",         "1",   "numeric",       "1",   "numeric",        "1",
    "double",        "1",   "numeric",       "1",   "numeric",        "1",
    "decimal",       "1",   "numeric",       "1",   "numeric",        "1",
    "timestamp",   stime,   "POSIXct",     rtime,   "POSIXct",      atime,
    "date",        sdate,      "Date",     rdate,      "Date",      rdate,
    "string",          1, "character",       "1", "character",        "1",
    "varchar(10)",     1, "character",       "1", "character",        "1",
    "char(10)",        1, "character",       "1", "character",        "1",
    "boolean",    "true",   "logical",    "TRUE",   "logical",     "TRUE",
  )

  if (spark_version(sc) < "2.2.0") {
    hive_type <- hive_type %>% filter(stype != "integer")
  }

  if ("arrow" %in% installed.packages() && packageVersion("arrow") < "0.12.0") {
    hive_type <- hive_type %>% filter(stype != "smallint", stype != "float")
  }

  if ("arrow" %in% installed.packages() && packageVersion("arrow") <= "0.13.0") {
    hive_type <- hive_type %>% filter(stype != "tinyint")
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
    hive_type %>% pull(!! if(arrow_compat) "atype" else "rtype")
  )

  spark_results <- DBI::dbGetQuery(sc, spark_query)
  names(spark_results) <- NULL
  spark_results <- sapply(spark_results, as.character)

  expect_equal(
    spark_results,
    hive_type %>% pull(!! if(arrow_compat) "avalue" else "rvalue")
  )
})

test_that("collect() can retrieve NULL data types as NAs", {
  library(dplyr)

  hive_type <- tibble::frame_data(
        ~stype,        ~rtype,        ~atype,
     "tinyint",     "integer",         "raw",
    "smallint",     "integer",     "integer",
     "integer",     "integer",     "integer",
      "bigint",     "numeric",   "integer64",
       "float",     "numeric",     "numeric",
      "double",     "numeric",     "numeric",
     "decimal",     "numeric",     "numeric",
   "timestamp",     "POSIXct",     "POSIXct",
        "date",        "Date",        "Date",
      "string",   "character",   "character",
 "varchar(10)",   "character",   "character",
    "char(10)",   "character",   "character",
  )

  if (using_arrow()) {
    # Disable while tracking fix for ARROW-3794
    hive_type <- hive_type %>% filter(stype != "tinyint")

    if (packageVersion("arrow") < "0.12.0") {
      # Disable while tracking fix for ARROW-3795
      hive_type <- hive_type %>% filter(stype != "bigint")
    }
  }

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
    hive_type %>% pull(!! if(using_arrow()) "atype" else "rtype")
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
  # Disabled in arrow while #1737 is investigated
  skip_on_arrow()

  tz_entries <- list(
    as.POSIXct("2018-01-01", tz = "UTC"),
    as.POSIXct("2018-01-01", tz = "GMT"),
    as.POSIXct("2018-01-01", tz = "America/Los_Angeles"),
    as.POSIXct("2018-01-01 03:33", tz = "America/Los_Angeles"),
    as.POSIXct("2018-01-01 03:33", tz = "Europe/Brussels")
  )

  collected <- sdf_len(sc, 1) %>%
    spark_apply(function(e, c) c$tzs, context = list(tzs = tz_entries)) %>%
    collect() %>%
    as.list() %>%
    unname()

  expect_true(
    all.equal(tz_entries, collected)
  )
})

test_that("collect() can retrieve specific dates without timezones", {
  data_tbl <- sdf_copy_to(
    sc,
    data_frame(
      t = c(1419126103)
    )
  )

  expect_equal(
    data_tbl %>%
      mutate(date_alt = from_utc_timestamp(timestamp(t) ,'UTC')) %>%
      pull(date_alt),
    as.POSIXct("2014-12-21 01:41:43 UTC", tz = "UTC")
  )

  expect_equal(
    data_tbl %>%
      mutate(date_alt = to_date(from_utc_timestamp(timestamp(t) ,'UTC'))) %>%
      pull(date_alt),
    as.Date(
      data_tbl %>%
        mutate(date_alt = as.character(to_date(from_utc_timestamp(timestamp(t) ,'UTC')))) %>%
        pull(date_alt)
    )
  )
})

test_that("collect() can retrieve logical columns with NAs", {
  expect_equal(
    logical_nas,
    logical_nas_tbl %>% dplyr::collect()
  )
})
