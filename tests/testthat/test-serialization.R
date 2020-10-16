context("serialization")

sc <- testthat_spark_connection()

test_requires("nycflights13")

flights_small <- flights %>% dplyr::sample_n(10000)
flights_tbl <- testthat_tbl("flights_small")

logical_nas <- tibble(bools = c(T, NA, F))
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
  for (dataset in datasets) {
    ensure_round_trip(sc, dataset)
  }
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
  skip_databricks_connect()

  n <- 1E3
  data <- as.data.frame(replicate(n, 1L, simplify = FALSE))
  names(data) <- paste("X", 1:n, sep = "")

  ensure_round_trip(sc, data)
})

test_that("data.frames with many columns don't cause Java StackOverflows", {
  skip_databricks_connect()
  version <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  n <- if (version >= "2.0.0") 500 else 5000
  df <- matrix(0, ncol = n, nrow = 2) %>% as_tibble(.name_repair = "unique")
  sdf <- copy_to(sc, df, overwrite = TRUE)

  # the above failed with a Java StackOverflow with older versions of sparklyr
  expect_true(TRUE, info = "no Java StackOverflow on copy of large dataset")
})

test_that("'ml_predict()', 'predict()' return same results", {
  skip_databricks_connect()
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

  df_tbl <- copy_to(sc, df, overwrite = TRUE)

  expect_equal(sdf_nrow(df_tbl), 2)
  expect_equal(sdf_ncol(df_tbl), 2)
})

arrow_compat <- using_arrow()
if (arrow_compat && packageVersion("arrow") > "0.17.1") {
  # Arrow will pull int64 into R as integer if all values fit in 32-bit int
  # (which they do in this test suite)
  arrowbigint <- "integer"
} else {
  arrowbigint <- "integer64"
}

test_that("collect() can retrieve all data types correctly", {
  skip_databricks_connect()
  # https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes
  library(dplyr)

  epoch_utime <- 0
  epoch_sdate <- "from_unixtime(unix_timestamp('01-01-1970' , 'dd-MM-yyyy'))"
  epoch_rdate <- as.Date("01-01-1970", "%d-%m-%Y") %>% as.character()
  epoch_stime <- paste0("to_utc_timestamp(from_unixtime(", epoch_utime, "), 'UTC')")
  epoch_rtime <- "1970-01-01"
  epoch_atime <- as.character(as.POSIXct(epoch_utime, origin = "1970-01-01"))

  utime <- as.numeric(as.POSIXct("2010-01-01 01:01:10", origin = "1970-01-01", tz = "UTC"))
  sdate <- "from_unixtime(unix_timestamp('01-01-2010' , 'dd-MM-yyyy'))"
  rdate <- as.Date("01-01-2010", "%d-%m-%Y") %>% as.character()
  stime <- paste0("to_utc_timestamp(from_unixtime(", utime, "), 'UTC')")
  rtime <- "2010-01-01 01:01:10"
  atime <- as.character(as.POSIXct(utime, origin = "1970-01-01"))

  hive_type <- tibble::tribble(
    ~stype,            ~svalue,      ~rtype,     ~rvalue,      ~atype,     ~avalue,
    "tinyint",             "1",   "integer",         "1",   "integer",         "1",
    "smallint",            "1",   "integer",         "1",   "integer",         "1",
    "integer",             "1",   "integer",         "1",   "integer",         "1",
    "bigint",              "1",   "numeric",         "1", arrowbigint,         "1",
    "float",               "1",   "numeric",         "1",   "numeric",         "1",
    "double",              "1",   "numeric",         "1",   "numeric",         "1",
    "decimal",             "1",   "numeric",         "1",   "numeric",         "1",
    "timestamp",   epoch_stime,   "POSIXct", epoch_rtime,   "POSIXct", epoch_atime,
    "date",        epoch_sdate,      "Date", epoch_rdate,      "Date", epoch_rdate,
    "timestamp",         stime,   "POSIXct",       rtime,   "POSIXct",       atime,
    "date",              sdate,      "Date",       rdate,      "Date",       rdate,
    "string",              "1", "character",         "1", "character",         "1",
    "varchar(10)",         "1", "character",         "1", "character",         "1",
    "char(10)",            "1", "character",         "1", "character",         "1",
    "boolean",          "true",   "logical",      "TRUE",   "logical",      "TRUE",
  )

  if (spark_version(sc) < "2.2.0") {
    hive_type <- hive_type %>% filter(stype != "integer")
  }
  if (.Platform$OS.type == "windows") {
    # Deserialization of Date type from Spark SQL has been problematic on Windows
    # for some strange, platform-specific reasons.
    hive_type <- hive_type %>% filter(stype != "date")
  }

  spark_query <- hive_type %>%
    mutate(
      query = paste0("cast(", svalue, " as ", stype, ") as ", gsub("\\(|\\)", "", stype), "_col", row_number())
    ) %>%
    pull(query) %>%
    paste(collapse = ", ") %>%
    paste("SELECT", .)

  spark_types <- DBI::dbGetQuery(sc, spark_query) %>%
    lapply(function(e) class(e)[[1]]) %>%
    as.character()

  expect_equal(
    spark_types,
    hive_type %>% pull(!!if (arrow_compat) "atype" else "rtype")
  )

  spark_results <- DBI::dbGetQuery(sc, spark_query)
  names(spark_results) <- NULL
  spark_results <- sapply(spark_results, as.character)

  expect_equal(
    spark_results,
    hive_type %>% pull(!!if (arrow_compat) "avalue" else "rvalue")
  )
})

test_that("collect() can retrieve NULL data types as NAs", {
  library(dplyr)

  hive_type <- tibble::tribble(
    ~stype, ~rtype, ~atype,
    "tinyint", "integer", "integer",
    "smallint", "integer", "integer",
    "integer", "integer", "integer",
    "bigint", "numeric", arrowbigint,
    "float", "numeric", "numeric",
    "double", "numeric", "numeric",
    "decimal", "numeric", "numeric",
    "timestamp", "POSIXct", "POSIXct",
    "date", "Date", "Date",
    "string", "character", "character",
    "varchar(10)", "character", "character",
    "char(10)", "character", "character",
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
    hive_type %>% pull(!!if (arrow_compat) "atype" else "rtype")
  )

  spark_results <- DBI::dbGetQuery(sc, spark_query)

  lapply(names(spark_results), function(e) {
    expect_true(is.na(spark_results[[e]]), paste(e, "expected to be NA"))
  })
})

test_that("collect() can retrieve date types successfully", {
  skip_on_windows()

  df <- data.frame(
    date = c(
      as.Date("1961-01-20"),
      as.Date("1970-01-01"),
      as.Date("1981-01-20"),
      as.Date("2001-01-20")
    ),
    name = c(
      "John F. Kennedy",
      NA,
      "Ronald Reagan",
      "George W. Bush"
    ),
    stringsAsFactors = FALSE
  )
  expect_equal(
    as.list(df),
    as.list(df %>% sdf_copy_to(sc, ., overwrite = TRUE) %>% sdf_collect())
  )
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
  succeed()
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
  succeed()
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
    tibble(t = c(1419126103))
  )

  expect_equal(
    as.double(
      data_tbl %>%
        mutate(date_alt = from_utc_timestamp(timestamp(t), "UTC")) %>%
        pull(date_alt)
    ),
    as.double(as.POSIXct("2014-12-21 01:41:43 UTC", tz = "UTC")),
    tolerance = 0.1,
    scale = 1
  )

  expect_equal(
    as.double(
      data_tbl %>%
        mutate(date_alt = to_date(from_utc_timestamp(timestamp(t), "UTC"))) %>%
        pull(date_alt)
    ),
    as.double(
      as.Date(
        data_tbl %>%
          mutate(date_alt = as.character(to_date(from_utc_timestamp(timestamp(t), "UTC")))) %>%
          pull(date_alt)
      )
    ),
    tolerance = 0.1,
    scale = 1
  )
})

test_that("collect() can retrieve logical columns with NAs", {
  expect_equal(
    logical_nas,
    logical_nas_tbl %>% dplyr::collect()
  )
})

test_that("environments are sent to Scala Maps (#1058)", {
  expect_identical(
    invoke_static(sc, "sparklyr.Test", "readMap", as.environment(list(foo = 5))),
    list(foo = 5)
  )

  expect_identical(
    invoke_static(sc, "sparklyr.Test", "readMap", as.environment(list(foo = 2L))),
    list(foo = 2L)
  )

  expect_identical(
    invoke_static(sc, "sparklyr.Test", "readMap", as.environment(list(foo = "bar"))),
    list(foo = "bar")
  )
})

test_that("collect() can retrieve nested list efficiently", {
  skip_databricks_connect()
  skip_on_windows()

  if (spark_version(sc) < "2.0.0") skip("performance improvement not available")

  temp_json <- tempfile(fileext = ".json")

  list(list(g = 1, d = 1:1000), list(g = 2, d = 1:1000)) %>%
    jsonlite::write_json(temp_json, auto_unbox = T)

  nested <- spark_read_json(sc, temp_json, memory = FALSE)

  expect_equal(nrow(collect(nested)), 2)
})
