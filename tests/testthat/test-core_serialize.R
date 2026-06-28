# ---------------------------------------------------------------------------
# Connection-free serializer tests. The write*/read* functions operate on a
# binary connection, so we exercise them through an in-memory rawConnection
# (read_bin dispatches to read_bin.default for a rawConnection) with no Spark.
# The live round-trip tests below cover the happy path against real Spark; these
# add the edge branches the fast local path never hits.
# ---------------------------------------------------------------------------

# Run a writer `fn` into a raw connection, then read it back with readObject().
serde_read_back <- function(fn) {
  wc <- rawConnection(raw(0), "wb")
  fn(wc)
  bytes <- rawConnectionValue(wc)
  close(wc)
  rc <- rawConnection(bytes, "rb")
  on.exit(close(rc))
  readObject(rc)
}
serde_roundtrip <- function(x) {
  serde_read_back(function(con) writeObject(con, x))
}

test_that("serializer round-trips scalar types", {
  expect_equal(serde_roundtrip(5L), 5L)
  expect_equal(serde_roundtrip(3.14), 3.14)
  expect_true(serde_roundtrip(TRUE))
  expect_equal(serde_roundtrip("hello"), "hello")
  expect_equal(serde_roundtrip(as.raw(c(1, 2, 3))), as.raw(c(1, 2, 3)))
  expect_equal(serde_roundtrip(as.Date("2021-03-04")), as.Date("2021-03-04"))
  expect_equal(serde_roundtrip(factor("b")), "b")

  t <- as.POSIXct("2021-03-04 05:06:07", tz = "UTC")
  expect_equal(as.double(serde_roundtrip(t)), as.double(t), tolerance = 1e-3)
})

test_that("serializer round-trips arrays and lists", {
  expect_equal(serde_roundtrip(list(1L, 2L, 3L)), c(1L, 2L, 3L)) # same-type -> array
  expect_equal(serde_roundtrip(list("a", "b")), list("a", "b")) # array of strings
  expect_equal(serde_roundtrip(list()), list()) # empty array
  expect_equal(serde_roundtrip(list(1L, "a")), list(1L, "a")) # mixed -> list
  # an empty raw round-trips to raw()
  expect_equal(serde_roundtrip(raw(0)), raw(0))
})

test_that("serializer maps NA scalars to NULL and NULLs back to NA", {
  expect_null(serde_roundtrip(NA_integer_))
  # a same-typed list containing NA serializes as a list; NULLs read back as NA
  expect_equal(serde_roundtrip(list(1L, NA_integer_, 3L)), list(1L, NA, 3L))
})

test_that("serializer round-trips a data.frame as a column list", {
  # writeObject() iterates a data.frame's columns and serializes each scalar, so
  # use a single-row frame (the column writer assumes scalar values).
  back <- serde_roundtrip(
    data.frame(a = 1L, b = "x", stringsAsFactors = FALSE)
  )
  expect_length(back, 2)
  expect_equal(back[[1]], 1L)
  expect_equal(back[[2]], "x")
})

test_that("writeObject() handles struct / env / spark_apply_binary_result writes", {
  wc <- rawConnection(raw(0), "wb")
  on.exit(close(wc))
  # struct path + listToStruct()
  expect_silent(writeObject(wc, listToStruct(list(a = 1L))))
  # environment -> map write path
  expect_silent(writeObject(wc, as.environment(list(a = 1L, b = 2L))))
  # spark_apply_binary_result -> writeList
  expect_silent(
    writeObject(
      wc,
      structure(list(raw(1)), class = "spark_apply_binary_result")
    )
  )
})

test_that("writeObject() errors on an unsupported type", {
  wc <- rawConnection(raw(0), "wb")
  on.exit(close(wc))
  bad <- structure(1, class = "zzz_unsupported")
  # writeType = FALSE reaches the switch's default stop
  expect_error(writeObject(wc, bad, writeType = FALSE), "Unsupported type")
  # writeType = TRUE reaches writeType()'s default stop
  expect_error(writeObject(wc, bad), "Unsupported type")
})

test_that("writeJobj() errors on an invalid jobj", {
  wc <- rawConnection(raw(0), "wb")
  on.exit(close(wc))
  with_mocked_bindings(
    isValidJobj = function(x) FALSE,
    .package = "sparklyr",
    expect_error(writeJobj(wc, list(id = "bad")), "invalid jobj")
  )
})

test_that("readStruct() reads a Spark struct into a named list", {
  s <- serde_read_back(function(con) {
    writeType(con, "struct")
    writeObject(con, list("x", "y")) # field names
    writeObject(con, list(1L, "a")) # field values (mixed -> stays a list)
  })
  expect_s3_class(s, "struct")
  expect_equal(s$x, 1L)
  expect_equal(s$y, "a")
})

test_that("readMap() reads a Spark map into a named list", {
  m <- serde_read_back(function(con) {
    writeType(con, "environment")
    writeInt(con, 1L)
    writeString(con, "foo")
    writeObject(con, 5)
  })
  expect_equal(m, list(foo = 5))
})

test_that("readStringArray() reads an 'f'-typed string array", {
  arr <- serde_read_back(function(con) {
    writeBin(charToRaw("f"), con)
    writeString(con, "ab<NA>")
  })
  expect_equal(arr, list("a", "b", NA_character_))
})

test_that("readObject() parses a 'J' JSON-typed value", {
  res <- serde_read_back(function(con) {
    writeBin(charToRaw("J"), con)
    writeString(con, '{"a":1}')
  })
  expect_equal(res$a, 1L)
})

test_that("readObject() errors on an unknown deserialization type", {
  rc <- rawConnection(charToRaw("Z"), "rb")
  on.exit(close(rc))
  expect_error(readObject(rc), "Unsupported type for deserialization")
})

test_that("readRaw() handles the NA (length -1) sentinel", {
  na_raw <- serde_read_back(function(con) {
    writeBin(charToRaw("r"), con)
    writeInt(con, -1L)
  })
  expect_true(is.na(na_raw))
})

test_that("readString() warns and removes embedded nuls", {
  wc <- rawConnection(raw(0), "wb")
  writeInt(wc, 3L)
  writeBin(as.raw(c(0x61, 0x00, 0x62)), wc, endian = "big")
  bytes <- rawConnectionValue(wc)
  close(wc)
  rc <- rawConnection(bytes, "rb")
  on.exit(close(rc))
  expect_warning(s <- readString(rc), "embedded nuls")
  expect_equal(s, "ab")
})

test_that("readDate()/readTime() honor sparklyr.collect.datechars", {
  withr::local_options(sparklyr.collect.datechars = TRUE)
  expect_equal(serde_roundtrip(as.Date("2021-01-02")), "2021-01-02")
  expect_type(
    serde_roundtrip(as.POSIXct("2021-01-02 03:04:05", tz = "UTC")),
    "character"
  )
})

test_that("typed readers return empty vectors for n = 0", {
  rc <- rawConnection(raw(0), "rb")
  on.exit(close(rc))
  expect_identical(readInt(rc, 0), integer(0))
  expect_identical(readDouble(rc, 0), double(0))
  expect_identical(readBoolean(rc, 0), logical(0))
  expect_identical(readTime(rc, 0), as.POSIXct(character(0)))
  expect_identical(readDateArray(rc, 0), as.Date(NA))
})

test_that("read_bin.livy_backend reads from the wrapped connection", {
  wc <- rawConnection(raw(0), "wb")
  writeBin(7L, wc, endian = "big")
  bytes <- rawConnectionValue(wc)
  close(wc)
  rc <- rawConnection(bytes, "rb")
  on.exit(close(rc))
  livy <- structure(list(rc = rc), class = "livy_backend")
  expect_equal(read_bin(livy, integer(), 1, endian = "big"), 7L)
})

# read_bin.spark_connection isn't registered in NAMESPACE (no @export), so under
# load_all it falls through to read_bin.default; read_bin.spark_worker_connection
# IS registered and routes to the same read_bin_wait(), so test through that.
test_that("read_bin_wait() returns data immediately when available", {
  worker <- structure(
    list(state = new.env(), config = list(), backend = "<be>"),
    class = "spark_worker_connection"
  )
  with_mocked_bindings(
    readBin = function(con, what, n, ...) 42L,
    .package = "base",
    expect_equal(read_bin(worker, integer(), 1, endian = "big"), 42L)
  )
})

test_that("read_bin_wait() polls and reports progress until data arrives", {
  fake_sc <- structure(
    list(
      state = new.env(),
      config = list(sparklyr.progress.interval = 0),
      backend = "<be>"
    ),
    class = "spark_worker_connection"
  )
  calls <- 0
  progressed <- FALSE
  with_mocked_bindings(
    readBin = function(con, what, n, ...) {
      calls <<- calls + 1
      if (calls < 3) integer(0) else 42L
    },
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    with_mocked_bindings(
      connection_progress = function(sc) {
        progressed <<- TRUE
        invisible(NULL)
      },
      connection_progress_terminated = function(sc) invisible(NULL),
      .package = "sparklyr",
      {
        expect_equal(read_bin(fake_sc, integer(), 1, endian = "big"), 42L)
        # also drive the no-endian read path inside the poll loop
        calls <- 0
        expect_equal(read_bin(fake_sc, integer(), 1), 42L)
      }
    )
  )
  expect_true(progressed)
})

test_that("read_bin_wait() errors when it times out", {
  fake_sc <- structure(
    list(
      state = new.env(),
      config = list(
        sparklyr.backend.timeout = 0,
        sparklyr.progress.interval = 999
      ),
      backend = "<be>"
    ),
    class = "spark_worker_connection"
  )
  with_mocked_bindings(
    readBin = function(con, what, n, ...) integer(0),
    Sys.sleep = function(...) invisible(NULL),
    .package = "base",
    expect_error(
      read_bin(fake_sc, integer(), 1, endian = "big"),
      "timed out"
    )
  )
})

skip_connection("core_serialize")
skip_on_livy()

sc <- testthat_spark_connection()

test_requires("nycflights13")

flights_small <- flights %>% dplyr::sample_n(10000)
flights_tbl <- testthat_tbl("flights_small")

logical_nas <- tibble(bools = c(T, NA, F))
logical_nas_tbl <- testthat_tbl("logical_nas")

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
  epoch_stime <- paste0(
    "to_utc_timestamp(from_unixtime(",
    epoch_utime,
    "), 'UTC')"
  )
  epoch_rtime <- "1970-01-01"
  epoch_atime <- as.character(as.POSIXct(epoch_utime, origin = "1970-01-01"))

  utime <- as.numeric(as.POSIXct(
    "2010-01-01 01:01:10",
    origin = "1970-01-01",
    tz = "UTC"
  ))
  sdate <- "from_unixtime(unix_timestamp('01-01-2010' , 'dd-MM-yyyy'))"
  rdate <- as.Date("01-01-2010", "%d-%m-%Y") %>% as.character()
  stime <- paste0("to_utc_timestamp(from_unixtime(", utime, "), 'UTC')")
  rtime <- "2010-01-01 01:01:10"
  atime <- as.character(as.POSIXct(utime, origin = "1970-01-01"))

  hive_type <- dplyr::tribble(
    ~stype        , ~svalue     , ~rtype      , ~rvalue     , ~atype      , ~avalue     ,
    "tinyint"     , "1"         , "integer"   , "1"         , "integer"   , "1"         ,
    "smallint"    , "1"         , "integer"   , "1"         , "integer"   , "1"         ,
    "integer"     , "1"         , "integer"   , "1"         , "integer"   , "1"         ,
    "bigint"      , "1"         , "numeric"   , "1"         , arrowbigint , "1"         ,
    "float"       , "1"         , "numeric"   , "1"         , "numeric"   , "1"         ,
    "double"      , "1"         , "numeric"   , "1"         , "numeric"   , "1"         ,
    "decimal"     , "1"         , "numeric"   , "1"         , "numeric"   , "1"         ,
    "timestamp"   , epoch_stime , "POSIXct"   , epoch_rtime , "POSIXct"   , epoch_atime ,
    "date"        , epoch_sdate , "Date"      , epoch_rdate , "Date"      , epoch_rdate ,
    "timestamp"   , stime       , "POSIXct"   , rtime       , "POSIXct"   , atime       ,
    "date"        , sdate       , "Date"      , rdate       , "Date"      , rdate       ,
    "string"      , "1"         , "character" , "1"         , "character" , "1"         ,
    "varchar(10)" , "1"         , "character" , "1"         , "character" , "1"         ,
    "char(10)"    , "1"         , "character" , "1"         , "character" , "1"         ,
    "boolean"     , "true"      , "logical"   , "TRUE"      , "logical"   , "TRUE"      ,
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
      query = paste0(
        "cast(",
        svalue,
        " as ",
        stype,
        ") as ",
        gsub("\\(|\\)", "", stype),
        "_col",
        row_number()
      )
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

  skip_on_os("mac")
  expect_equal(
    spark_results,
    hive_type %>% pull(!!if (arrow_compat) "avalue" else "rvalue")
  )
})

test_that("collect() can retrieve NULL data types as NAs", {
  library(dplyr)

  hive_type <- dplyr::tribble(
    ~stype        , ~rtype      , ~atype      ,
    "tinyint"     , "integer"   , "integer"   ,
    "smallint"    , "integer"   , "integer"   ,
    "integer"     , "integer"   , "integer"   ,
    "bigint"      , "numeric"   , arrowbigint ,
    "float"       , "numeric"   , "numeric"   ,
    "double"      , "numeric"   , "numeric"   ,
    "decimal"     , "numeric"   , "numeric"   ,
    "timestamp"   , "POSIXct"   , "POSIXct"   ,
    "date"        , "Date"      , "Date"      ,
    "string"      , "character" , "character" ,
    "varchar(10)" , "character" , "character" ,
    "char(10)"    , "character" , "character" ,
  )

  if (spark_version(sc) < "2.2.0") {
    hive_type <- hive_type %>% filter(stype != "integer")
  }

  spark_query <- hive_type %>%
    mutate(
      query = paste0(
        "cast(NULL as ",
        stype,
        ") as ",
        gsub("\\(|\\)", "", stype),
        "_col"
      )
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

  df <- dplyr::tibble(
    date = as.Date(
      c(
        "1000-01-01",
        "1888-06-01",
        "1969-12-31",
        "1970-01-01",
        "1970-01-02",
        "1981-01-20",
        "2001-01-20",
        "3111-01-20"
      )
    )
  )
  expect_equivalent(
    df,
    df %>% sdf_copy_to(sc, ., overwrite = TRUE) %>% sdf_collect()
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
          mutate(
            date_alt = as.character(to_date(from_utc_timestamp(
              timestamp(t),
              "UTC"
            )))
          ) %>%
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
    invoke_static(
      sc,
      "sparklyr.Test",
      "readMap",
      as.environment(list(foo = 5))
    ),
    list(foo = 5)
  )

  expect_identical(
    invoke_static(
      sc,
      "sparklyr.Test",
      "readMap",
      as.environment(list(foo = 2L))
    ),
    list(foo = 2L)
  )

  expect_identical(
    invoke_static(
      sc,
      "sparklyr.Test",
      "readMap",
      as.environment(list(foo = "bar"))
    ),
    list(foo = "bar")
  )
})

test_that("collect() can retrieve nested list efficiently", {
  skip_databricks_connect()
  skip_on_windows()

  if (spark_version(sc) < "2.0.0") {
    skip("performance improvement not available")
  }

  temp_json <- tempfile(fileext = ".json")

  list(list(g = 1, d = 1:1000), list(g = 2, d = 1:1000)) %>%
    jsonlite::write_json(temp_json, auto_unbox = T)

  nested <- spark_read_json(sc, temp_json, memory = FALSE)

  expect_equal(nrow(collect(nested)), 2)
})

test_that("array of temporal values are preserved with Spark 3.0+", {
  test_requires_version("3.0.0")
  skip_on_arrow()

  df <- dplyr::tibble(
    char = c("one", "two"),
    int = c(3L, 4L),
    int_arr = list(seq(5), seq(10)),
    date_arr = list(
      as.Date(seq(5) * 700, origin = "1970-01-01"),
      as.Date(seq(7) * 100, origin = "1970-01-01")
    ),
    timestamp_arr = list(
      as.POSIXct(seq(5) * 700000, origin = "1970-01-01"),
      as.POSIXct(seq(7) * 1000000, origin = "1970-01-01")
    ),
    char_arr = list(letters[1:13], letters[14:26])
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(sdf %>% collect(), df)
})

test_clear_cache()
