sc <- testthat_spark_connection()

test_avro_schema <- list(
  type = "record",
  name = "topLevelRecord",
  fields = list(
    list(name = "a", type = list("double", "null")),
    list(name = "b", type = list("int", "null")),
    list(name = "c", type = list("string", "null"))
  )
) %>%
  jsonlite::toJSON(auto_unbox = TRUE) %>%
  as.character()

test_that("spark_read_csv() succeeds when column contains similar non-ascii", {
  skip_on_livy()
  skip_connection("format-csv")
  if (.Platform$OS.type == "windows") {
    skip("CSV encoding is slightly different in windows")
  }
  skip_databricks_connect()

  csvPath <- get_test_data_path(
    "spark-read-csv-column-containing-non-ascii.csv"
  )
  df <- spark_read_csv(
    sc,
    name = "teste",
    path = csvPath,
    header = TRUE,
    delimiter = ";",
    memory = FALSE
  )

  expect_true(
    all(dplyr::tbl_vars(df) == c("Municpio", "var", "var_1_0")),
    info = "success reading non-ascii similar columns from csv"
  )
})

test_that("spark_read_json() can load data using column names", {
  skip_on_livy()
  skip_connection("format-json")
  jsonPath <- get_test_data_path(
    "spark-read-json-can-load-data-using-column-names.json"
  )
  df <- spark_read_json(
    sc,
    name = "iris_json_named",
    path = jsonPath,
    columns = c("a", "b", "c")
  )

  testthat::expect_equal(colnames(df), c("a", "b", "c"))
})

test_that("spark_read_json() can load data using column types", {
  skip_on_livy()
  skip_connection("format-json")
  test_requires("dplyr")

  jsonPath <- get_test_data_path(
    "spark-read-json-can-load-data-using-column-types.json"
  )

  expect_warning_on_arrow(
    df <- spark_read_json(
      sc,
      name = "iris_json_typed",
      path = jsonPath,
      columns = list(
        "Sepal_Length" = "character",
        "Species" = "character",
        "Other" = "struct<a:integer,b:character>"
      )
    ) %>%
      collect()
  )

  expect_true(is.character(df$Sepal_Length))
  expect_true(is.character(df$Species))
  expect_true(is.list(df$Other))
})

test_that("spark_read_json() can load nested structs using column types", {
  skip_on_livy()
  skip_connection("format-json")
  jsonPath <- get_test_data_path(
    "spark-read-json-can-load-nested-structs-using-column-types.json"
  )
  columns <- list(
    a = "integer",
    b = "double",
    d = "date",
    s = list(
      a = "integer",
      b = "double",
      d = "date",
      s = "string",
      i = list(
        x = "integer"
      )
    )
  )
  sdf <- spark_read_json(
    sc,
    path = jsonPath,
    columns = columns
  )

  expect_equal(
    sdf %>% sdf_schema(expand_struct_cols = TRUE),
    list(
      a = list(name = "a", type = "IntegerType"),
      b = list(name = "b", type = "DoubleType"),
      d = list(name = "d", type = "DateType"),
      s = list(
        name = "s",
        type = list(
          a = list(name = "a", type = "IntegerType"),
          b = list(name = "b", type = "DoubleType"),
          d = list(name = "d", type = "DateType"),
          s = list(name = "s", type = "StringType"),
          i = list(
            name = "i",
            type = list(x = list(name = "x", type = "IntegerType"))
          )
        )
      )
    )
  )
})

test_that("spark_read_csv() can read long decimals", {
  skip_on_livy()
  skip_connection("format-csv")
  csvPath <- get_test_data_path(
    "spark-read-csv-can-read-long-decimals.csv"
  )
  df <- spark_read_csv(
    sc,
    name = "test_big_decimals",
    path = csvPath
  )

  expect_equal(sdf_nrow(df), 2)
})

test_that("spark_read_csv() can rename columns", {
  skip_on_livy()
  skip_connection("format-csv")
  csvPath <- get_test_data_path(
    "spark-read-csv-can-rename-columns.csv"
  )
  df <- spark_read_csv(
    sc,
    name = "test_column_rename",
    path = csvPath,
    columns = c("AA", "BB", "CC")
  ) %>%
    collect()

  expect_equal(names(df), c("AA", "BB", "CC"))
})

test_that("spark_read_text() can read a whole file", {
  skip_on_livy()
  skip_connection("format-csv")
  skip_databricks_connect()
  test_requires("dplyr")

  whole_tbl <- spark_read_text(
    sc,
    "whole",
    get_test_data_path("spark-read-text-can-read-a-whole-file.txt"),
    overwrite = T,
    whole = TRUE
  )

  expect_equal(
    whole_tbl %>% collect() %>% nrow(),
    1L
  )
})

test_that("spark_read_csv() can read with no name", {
  skip_on_livy()
  skip_connection("format-csv")
  test_requires("dplyr")

  csvPath <- get_test_data_path(
    "spark-read-csv-can-read-with-no-name.txt"
  )
  expect_equal(colnames(spark_read_csv(sc, csvPath)), "a")
  expect_equal(colnames(spark_read_csv(sc, "test", csvPath)), "a")
  expect_equal(colnames(spark_read_csv(sc, path = csvPath)), "a")
})

test_that("spark_read_csv() can read with named character name", {
  skip_on_livy()
  skip_connection("format-csv")
  test_requires("dplyr")

  csvPath <- get_test_data_path(
    "spark-read-csv-can-read-with-named-character-name.txt"
  )

  name <- c(name = "testname")
  expect_equal(colnames(spark_read_csv(sc, name, csvPath)), "a")
})

test_that("spark_read_csv() can read column types", {
  skip_on_livy()
  skip_connection("format-csv")
  csvPath <- get_test_data_path(
    "spark-read-csv-can-read-column-types.txt"
  )
  df_schema <- spark_read_csv(
    sc,
    name = "test_columns",
    path = csvPath,
    columns = c(a = "byte", b = "integer", c = "double")
  ) %>%
    sdf_schema()

  expect_equal(
    df_schema,
    list(
      a = list(name = "a", type = "ByteType"),
      b = list(name = "b", type = "IntegerType"),
      c = list(name = "c", type = "DoubleType")
    )
  )
})

test_that("spark_read_csv() can read verbatim column types", {
  skip_on_livy()
  skip_connection("format-csv")
  csvPath <- get_test_data_path(
    "spark-read-csv-can-read-verbatim-column-types.csv"
  )
  df_schema <- spark_read_csv(
    sc,
    name = "test_columns",
    path = csvPath,
    columns = c(a = "ByteType", b = "IntegerType", c = "DoubleType")
  ) %>%
    sdf_schema()

  expect_equal(
    df_schema,
    list(
      a = list(name = "a", type = "ByteType"),
      b = list(name = "b", type = "IntegerType"),
      c = list(name = "c", type = "DoubleType")
    )
  )
})

test_that("spark_read_csv() can read if embedded nuls present", {
  skip_on_livy()
  skip_connection("format-csv")
  skip_on_arrow() # ARROW-6582

  fpath <- get_test_data_path("with_embedded_nul.csv")
  df <- spark_read_csv(sc, name = "test_embedded_nul", path = fpath)
  expect_equivalent(
    suppressWarnings(df %>% collect()),
    data.frame(test = "teststring", stringsAsFactors = FALSE)
  )
  expect_warning(
    df %>% collect(),
    "Input contains embedded nuls, removing."
  )
})

test_that("spark_read() works as expected", {
  skip_on_livy()
  skip_connection("format-generalized")
  paths <- c(
    "hdfs://localhost:9000/1/a/b",
    "hdfs://localhost:9000/2",
    "hdfs://localhost:9000/c/d/e",
    "hdfs://localhost:9000/f",
    "hdfs://localhost:9000/3/4/5"
  )
  reader <- function(path) {
    data.frame(
      md5 = openssl::multihash(path, algos = "md5"),
      length = nchar(path)
    )
  }

  expected_md5s <- sapply(
    paths,
    function(x) openssl::multihash(x, algos = "md5")
  ) %>%
    unlist()
  names(expected_md5s) <- NULL

  expected_lengths <- sapply(paths, function(x) nchar(x))
  names(expected_lengths) <- NULL

  sdf <- spark_read(
    sc,
    paths,
    reader,
    packages = c("openssl"),
    columns = list(md5 = "character", length = "integer")
  )
  expect_equivalent(
    sdf_collect(sdf),
    data.frame(
      md5 = expected_md5s,
      length = expected_lengths,
      stringsAsFactors = FALSE
    )
  )
})

test_that("spark_read_avro() works as expected", {
  skip_on_livy()
  skip_connection("format-avro")
  test_requires_version("2.4.0")
  skip_databricks_connect()

  expected <- dplyr::tibble(
    a = c(1, NaN, 3, 4, NaN),
    b = c(-2L, 0L, 1L, 3L, 6L),
    c = c("ab", "cde", "zzzz", "", "fghi")
  )

  for (avro_schema in list(NULL, test_avro_schema)) {
    actual <- spark_read_avro(
      sc,
      path = get_test_data_path("test_spark_read.avro"),
      avro_schema = avro_schema
    ) %>%
      sdf_collect()

    expect_equal(colnames(expected), colnames(actual))

    for (col in colnames(expected)) {
      expect_equal(expected[[col]], actual[[col]])
    }
  }
})

test_that("spark_read_binary can process input directory without partition specs", {
  skip_on_livy()
  skip_connection("format-binary")
  test_requires_version("3.0.0")

  dir <- get_test_data_path("test_spark_read_binary")
  sdf <- spark_read_binary(sc, dir = dir, name = random_string()) %>%
    dplyr::arrange(path)
  df <- sdf %>% collect()

  expect_equal(colnames(df), c("path", "modificationTime", "length", "content"))
  expect_equivalent(
    df %>% dplyr::select(path, length, content),
    dplyr::tribble(
      ~path                                          , ~length , ~content          ,
      paste0("file:", file.path(dir, "file0.dat"))   ,       4 , charToRaw("1234") ,
      paste0("file:", file.path(dir, "file1.dat"))   ,       4 , charToRaw("5678") ,
      paste0("file:", file.path(dir, "file2.dat"))   ,       4 , charToRaw("abcd") ,
      paste0("file:", file.path(dir, "file3.ascii")) ,       4 , charToRaw("efgh") ,
    )
  )
})

test_that("spark_read_binary can process input directory with flat partition specs", {
  skip_on_livy()
  skip_connection("format-binary")
  test_requires_version("3.0.0")

  dir <- get_test_data_path("test_spark_read_binary_with_flat_partition_specs")
  sdf <- spark_read_binary(sc, dir = dir, name = random_string()) %>%
    dplyr::arrange(partition, path)
  df <- sdf %>% collect()

  expect_equal(
    colnames(df),
    c("path", "modificationTime", "length", "content", "partition")
  )
  expect_equivalent(
    df %>% dplyr::select(path, length, content, partition),
    dplyr::tribble(
      ~path                                                       , ~length , ~content          , ~partition ,
      paste0("file:", file.path(dir, "partition=0", "file0.dat")) ,       4 , charToRaw("1234") , 0L         ,
      paste0("file:", file.path(dir, "partition=1", "file1.dat")) ,       4 , charToRaw("5678") , 1L         ,
      paste0("file:", file.path(dir, "partition=2", "file2.dat")) ,       4 , charToRaw("abcd") , 2L         ,
      paste0("file:", file.path(dir, "partition=2", "file3.dat")) ,       4 , charToRaw("efgh") , 2L         ,
    )
  )
})

test_that("spark_read_binary can process input directory with nested partition specs", {
  skip_on_livy()
  skip_connection("format-binary")
  test_requires_version("3.0.0")

  dir <- get_test_data_path(
    "test_spark_read_binary_with_nested_partition_specs"
  )
  sdf <- spark_read_binary(sc, dir = dir, name = random_string()) %>%
    dplyr::arrange(a, b)
  df <- sdf %>% collect()

  expect_equal(
    colnames(df),
    c("path", "modificationTime", "length", "content", "a", "b")
  )
  expect_equivalent(
    df %>% dplyr::select(path, length, content, a, b),
    dplyr::tribble(
      ~path                                                      , ~length , ~content          , ~a , ~b ,
      paste0("file:", file.path(dir, "a=0", "b=0", "file0.dat")) ,       4 , charToRaw("1234") , 0L , 0L ,
      paste0("file:", file.path(dir, "a=0", "b=1", "file2.dat")) ,       4 , charToRaw("abcd") , 0L , 1L ,
      paste0("file:", file.path(dir, "a=1", "b=0", "file1.dat")) ,       4 , charToRaw("5678") , 1L , 0L ,
      paste0("file:", file.path(dir, "a=1", "b=1", "file3.dat")) ,       4 , charToRaw("efgh") , 1L , 1L ,
    )
  )
})

test_that("spark_read_binary can support 'pathGlobFilter' option correctly", {
  skip_on_livy()
  skip_connection("format-binary")
  test_requires_version("3.0.0")

  dir <- get_test_data_path("test_spark_read_binary")
  sdf <- spark_read_binary(
    sc,
    dir = dir,
    name = random_string(),
    path_glob_filter = "*.dat"
  ) %>%
    dplyr::arrange(path)
  df <- sdf %>% collect()

  expect_equal(
    colnames(df),
    c("path", "modificationTime", "length", "content")
  )
  expect_equivalent(
    df %>% dplyr::select(path, length, content),
    dplyr::tribble(
      ~path                                        , ~length , ~content          ,
      paste0("file:", file.path(dir, "file0.dat")) ,       4 , charToRaw("1234") ,
      paste0("file:", file.path(dir, "file1.dat")) ,       4 , charToRaw("5678") ,
      paste0("file:", file.path(dir, "file2.dat")) ,       4 , charToRaw("abcd") ,
    )
  )
})

test_that("spark_read_binary supports 'recursiveFileLookup' option correctly", {
  skip_on_livy()
  skip_connection("format-binary")
  test_requires_version("3.0.0")

  dir <- get_test_data_path("test_spark_read_binary_recursive_file_lookup")
  sdf <- spark_read_binary(
    sc,
    dir = dir,
    name = random_string(),
    recursive_file_lookup = TRUE
  ) %>%
    dplyr::arrange(path)
  df <- sdf %>% collect()

  expect_equal(colnames(df), c("path", "modificationTime", "length", "content"))
  expect_equivalent(
    df %>% dplyr::select(path, length, content),
    dplyr::tribble(
      ~path                                                      , ~length , ~content          ,
      paste0("file:", file.path(dir, "a=0", "b=0", "file0.dat")) ,       4 , charToRaw("1234") ,
      paste0("file:", file.path(dir, "a=1", "b=0", "file1.dat")) ,       4 , charToRaw("5678") ,
      paste0("file:", file.path(dir, "b=1", "file3.dat"))        ,       4 , charToRaw("efgh") ,
      paste0("file:", file.path(dir, "file2.dat"))               ,       4 , charToRaw("abcd") ,
    )
  )
})

test_that("spark_read_image works as expected", {
  skip_on_livy()
  skip_connection("format-image")
  test_requires_version("2.4.0")

  dir <- get_test_data_path("images")
  sdf <- spark_read_image(
    sc,
    name = random_string(),
    dir = dir,
    repartition = 4
  )

  expect_equal(sdf_nrow(sdf), 8)
  expect_equal(sdf_ncol(sdf), 1)
  expect_equal(sdf_num_partitions(sdf), 4)
  expect_equal(
    sdf_schema(sdf, expand_struct_cols = TRUE),
    list(
      image = list(
        name = "image",
        type = list(
          origin = list(name = "origin", type = "StringType"),
          height = list(name = "height", type = "IntegerType"),
          width = list(name = "width", type = "IntegerType"),
          nChannels = list(name = "nChannels", type = "IntegerType"),
          mode = list(name = "mode", type = "IntegerType"),
          data = list(name = "data", type = "BinaryType")
        )
      )
    )
  )
  expect_equal(
    sdf %>%
      dplyr::transmute(origin = image[["origin"]]) %>%
      dplyr::arrange(origin) %>%
      dplyr::collect(),
    dplyr::tibble(
      origin = lapply(
        c(
          "edit-sql",
          "help",
          "livy-ui",
          "livy",
          "spark-log",
          "spark-ui",
          "spark",
          "yarn-ui"
        ),
        function(x) paste0("file://", file.path(dir, x), ".png")
      ) %>%
        unlist()
    )
  )
})

test_that("spark_read_libsvm() reads a libsvm file", {
  skip_on_livy()
  skip_connection("format-libsvm")
  skip_databricks_connect()

  path <- get_test_data_path("sample_libsvm_data.txt")
  sdf <- spark_read_libsvm(sc, name = random_string(), path = path)

  expect_setequal(colnames(sdf), c("label", "features"))
  expect_gt(sdf_nrow(sdf), 0)
})

test_readwrite <- function(sc, writer, reader, name = "testtable", ...) {
  skip_on_livy()
  skip_on_arrow_devel()
  skip_databricks_connect()
  path <- file.path(dirname(sc$output_file), c("batch_1", "batch_2"))
  path_glob <- file.path(dirname(sc$output_file), "batch*")
  on.exit(unlink(path, recursive = TRUE, force = TRUE), add = TRUE)

  writer(sdf_copy_to(sc, data.frame(line = as.character(1L:3L))), path[1L])
  writer(sdf_copy_to(sc, data.frame(line = as.character(4L:6L))), path[2L])

  if (is.element("whole", names(list(...))) && isTRUE(list(...)$whole)) {
    res_1 <- reader(sc, name, path[1L], ...) %>%
      collect() %>%
      pull(contents) %>%
      strsplit("\n") %>%
      unlist() %>%
      sort()
    res_2 <- reader(sc, name, path[2L], ...) %>%
      collect() %>%
      pull(contents) %>%
      strsplit("\n") %>%
      unlist() %>%
      sort()
    res_3 <- reader(sc, name, path, ...) %>%
      collect() %>%
      pull(contents) %>%
      strsplit("\n") %>%
      unlist() %>%
      sort()
    res_4 <- reader(sc, name, path_glob, ...) %>%
      collect() %>%
      pull(contents) %>%
      strsplit("\n") %>%
      unlist() %>%
      sort()
  } else {
    res_1 <- reader(sc, name, path[1L], ...) %>%
      collect() %>%
      pull(line) %>%
      sort()
    res_2 <- reader(sc, name, path[2L], ...) %>%
      collect() %>%
      pull(line) %>%
      sort()
    res_3 <- reader(sc, name, path, ...) %>%
      collect() %>%
      pull(line) %>%
      sort()
    res_4 <- reader(sc, name, path_glob, ...) %>%
      collect() %>%
      pull(line) %>%
      sort()
  }

  list(
    all(res_1 == as.character(1:3)),
    all(res_2 == as.character(4:6)),
    all(res_3 == as.character(1:6)),
    all(res_4 == as.character(1:6))
  )
}

test_that(
  "spark_read_parquet() reads multiple parquet files",
  expect_equal(
    test_readwrite(
      sc = sc,
      writer = spark_write_parquet,
      reader = spark_read_parquet
    ),
    list(TRUE, TRUE, TRUE, TRUE)
  )
)

test_that("spark_read_orc() reads multiple orc files", {
  test_requires_version("2.0.0")
  expect_equal(
    test_readwrite(sc = sc, writer = spark_write_orc, reader = spark_read_orc),
    list(TRUE, TRUE, TRUE, TRUE)
  )
})

test_that(
  "spark_read_json() reads multiple json files",
  expect_equal(
    test_readwrite(
      sc = sc,
      writer = spark_write_json,
      reader = spark_read_json
    ),
    list(TRUE, TRUE, TRUE, TRUE)
  )
)

test_that(
  "spark_read_text() reads multiple text files",
  expect_equal(
    test_readwrite(
      sc = sc,
      writer = spark_write_text,
      reader = spark_read_text
    ),
    list(TRUE, TRUE, TRUE, TRUE)
  )
)

test_that(
  "spark_read_text() throws a useful error for multiple files with whole=TRUE",
  expect_error(
    test_readwrite(
      sc = sc,
      writer = spark_write_text,
      reader = spark_read_text,
      whole = TRUE
    ),
    "spark_read_text is only suppored with path of length 1 if whole=TRUE"
  )
)

test_that("spark_read_compat_param() requires a path", {
  # data_interface.R:17-18 — both name and path NULL.
  # `sc` is intentionally NULL here: this branch errors before `sc` is ever
  # used (only the gen_sdf_name() branches read sc$config).
  expect_error(
    spark_read_compat_param(NULL, name = NULL, path = NULL),
    "The 'path' parameter must be specified"
  )
})

test_that("spark_read_compat_param() resolves name/path combinations", {
  sc <- list(config = list()) # gen_sdf_name() only reads sc$config

  # name == path: treat the value as the path, generate a name
  out <- spark_read_compat_param(sc, name = "data.csv", path = "data.csv")
  expect_length(out, 2)
  expect_identical(out[[2]], "data.csv")

  # only path supplied: generate a name from the path
  out <- spark_read_compat_param(sc, name = NULL, path = "data.csv")
  expect_identical(out[[2]], "data.csv")

  # both supplied: passed through unchanged
  expect_identical(
    spark_read_compat_param(sc, name = "tbl", path = "data.csv"),
    c("tbl", "data.csv")
  )
})

test_that("spark_data_apply_mode() errors on an unsupported mode type", {
  # data_interface.R:527 — numeric mode hits neither the list nor character branch
  expect_error(
    spark_data_apply_mode(list(), 123),
    "Unsupported type"
  )
})

test_that("spark_data_apply_mode() returns options unchanged for NULL mode", {
  # data_interface.R:521 — the is.null(mode) short-circuit
  opts <- list(x = 1)
  expect_identical(spark_data_apply_mode(opts, NULL), opts)
})

test_that("avro_set_schema() rejects a non-character schema", {
  # data_interface.R:1285
  expect_error(
    avro_set_schema(list(), 123),
    "Expect Avro schema to be a JSON string"
  )
})

test_that("avro_set_schema() attaches a character schema and passes through NULL", {
  # data_interface.R:1284-1290
  expect_identical(avro_set_schema(list(), NULL), list())
  expect_identical(
    avro_set_schema(list(), "{\"type\":\"record\"}")$avroSchema,
    "{\"type\":\"record\"}"
  )
})

test_that("spark_csv_options() emits boolean strings for header/inferSchema", {
  # data_read.R:46-68 — TRUE branches map to "true", anything else to "false"
  opts <- spark_csv_options(
    header = TRUE,
    inferSchema = TRUE,
    delimiter = ",",
    quote = "\"",
    escape = "\\",
    charset = "UTF-8",
    nullValue = NULL,
    options = list()
  )

  expect_equal(opts$header, "true")
  expect_equal(opts$inferSchema, "true")
  expect_equal(opts$delimiter, ",")
  expect_equal(opts$quote, "\"")
  expect_equal(opts$escape, "\\")
  expect_equal(opts$charset, "UTF-8")
  # toString(NULL) yields the empty string
  expect_equal(opts$nullValue, "")
})

test_that("spark_csv_options() maps non-TRUE header/inferSchema to 'false'", {
  # data_read.R:59-60 — the else branches of the header/inferSchema flags
  opts <- spark_csv_options(
    header = FALSE,
    inferSchema = FALSE,
    delimiter = ";",
    quote = "'",
    escape = "|",
    charset = "latin1",
    nullValue = "NA",
    options = list()
  )

  expect_equal(opts$header, "false")
  expect_equal(opts$inferSchema, "false")
  expect_equal(opts$delimiter, ";")
  expect_equal(opts$nullValue, "NA")
})

test_that("spark_csv_options() preserves caller-supplied options", {
  # data_read.R:56-57 — user `options` are prepended and survive the merge
  opts <- spark_csv_options(
    header = TRUE,
    inferSchema = FALSE,
    delimiter = ",",
    quote = "\"",
    escape = "\\",
    charset = "UTF-8",
    nullValue = NULL,
    options = list(maxColumns = "5", mode = "DROPMALFORMED")
  )

  expect_equal(opts$maxColumns, "5")
  expect_equal(opts$mode, "DROPMALFORMED")
  # built-in options are still present alongside the custom ones
  expect_equal(opts$header, "true")
  expect_equal(opts$inferSchema, "false")
})

test_that("spark_read_csv() errors when infer_schema=TRUE with typed columns", {
  # data_read.R:158-162 — the mutually-exclusive guard between
  # infer_schema and a typed `columns` specification.
  skip_on_livy()
  skip_connection("format-csv")

  csvPath <- get_test_data_path(
    "spark-read-csv-can-read-column-types.txt"
  )
  expect_error(
    spark_read_csv(
      sc,
      name = "test_infer_schema_conflict",
      path = csvPath,
      columns = c(a = "byte", b = "integer", c = "double"),
      infer_schema = TRUE
    ),
    "'infer_schema' must be set to FALSE when 'columns' specifies column types"
  )
})
test_clear_cache()
