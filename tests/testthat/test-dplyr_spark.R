skip_connection("dplyr_spark")
test_requires("dplyr")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
mtcars_tbl <- testthat_tbl("mtcars")
has_predicates <- tidyselect_data_has_predicates(mtcars_tbl)
df1 <- tibble(a = 1:3, b = letters[1:3])
df1_tbl <- testthat_tbl("df1")

test_remote_name <- function(x, y) {
  if (packageVersion("dbplyr") <= "2.3.4") {
    y <- ident(y)
  }
  expect_equal(dbplyr::remote_name(x), y)
}

test_that("process_tbl_name works as expected", {
  skip_if(any(grepl("connect_", class(sc))))
  expect_equal(sparklyr:::process_tbl_name("a"), "a")
  expect_equal(sparklyr:::process_tbl_name("xyz"), "xyz")
  expect_equal(sparklyr:::process_tbl_name("x.y"), dbplyr::in_schema("x", "y"))
  expect_equal(
    sparklyr:::process_tbl_name("x.y.z"),
    dbplyr::in_catalog("x", "y", "z")
  )

  df1 <- dplyr::tibble(a = 1, g = 2) %>%
    copy_to(sc, ., "ptn_df1", overwrite = TRUE)
  df2 <- dplyr::tibble(b = 1, g = 2) %>%
    copy_to(sc, ., "ptn_df2", overwrite = TRUE)

  query <- sql(
    "SELECT ptn_df1.a, ptn_df2.b, ptn_df1.g FROM ptn_df1 LEFT JOIN ptn_df2 ON ptn_df1.g = ptn_df2.g"
  )
  expect_equivalent(
    tbl(sc, query) %>% collect(),
    dplyr::tibble(a = 1, b = 1, g = 2)
  )
})

test_that("in_schema() works as expected", {
  skip_on_arrow()
  skip_on_livy()
  if (spark_version(sc) < "3.4.0") {
    db_name <- random_string("test_db_")

    queries <- c(
      sprintf("CREATE DATABASE `%s`", db_name),
      sprintf(
        "CREATE TABLE IF NOT EXISTS `%s`.`hive_tbl` (`x` INT) USING hive",
        db_name
      )
    )
    for (query in queries) {
      DBI::dbGetQuery(sc, query)
    }

    expect_equivalent(
      dplyr::tbl(sc, dbplyr::in_schema(db_name, "hive_tbl")) %>% collect(),
      dplyr::tibble(x = integer())
    )
  }
})

test_that("sdf_remote_name returns null for computed tables", {
  test_remote_name(iris_tbl, "iris")

  virginica_sdf <- iris_tbl %>% filter(Species == "virginica")
  expect_equal(dbplyr::remote_name(virginica_sdf), NULL)
})

test_that("sdf_remote_name ignores the last group_by() operation(s)", {
  sdf <- iris_tbl
  for (i in seq(4)) {
    sdf <- sdf %>% dplyr::group_by(Species)
    test_remote_name(sdf, "iris")
  }
})

test_that("sdf_remote_name ignores the last ungroup() operation(s)", {
  sdf <- iris_tbl
  for (i in seq(4)) {
    sdf <- sdf %>% dplyr::ungroup()
    test_remote_name(sdf, "iris")
  }
})

test_that("sdf_remote_name works with arrange followed by compute", {
  tbl <- copy_to(sc, dplyr::tibble(lts = letters[26:24], nums = seq(3)))
  ordered_tbl <- tbl %>% arrange(lts) %>% compute(name = "ordered_tbl")

  test_remote_name(
    ordered_tbl,
    "ordered_tbl"
  )
  expect_equivalent(
    tbl(sc, "ordered_tbl") %>% collect(),
    dplyr::tibble(lts = letters[24:26], nums = 3:1)
  )
})

test_that("result from dplyr::compute() has remote name", {
  sdf <- iris_tbl
  sdf <- sdf %>% dplyr::mutate(y = 5) %>% dplyr::compute()
  expect_false(is.null(sdf %>% dbplyr::remote_name()))
})

test_that("tbl_spark prints", {
  print_output <- capture.output(print(iris_tbl))
  # dbplyr (>= 2.6.0) replaced the single `# Source: table<...>` header with a
  # `# A query:` line plus a separate `# Database:` line.
  expect_equal(
    print_output[1],
    "# A query:  ?? x 5"
  )
  expect_equal(
    print_output[2],
    "# Database: spark_connection"
  )
})

test_that("Connection functions work", {
  sc <- testthat_spark_connection()

  tbl_mtcars <- testthat_tbl("mtcars")

  sql_mtcars <- dbplyr::remote_query(tbl_mtcars)

  # dbplyr (>= 2.6.0) replaced the single `# Source: table<...>` header with a
  # `# A query:` line plus a separate `# Database:` line.
  copy_to_output <- capture.output(copy_to.src_spark(
    sc,
    mtcars,
    "src_mtcars",
    overwrite = TRUE
  ))
  expect_equal(copy_to_output[[1]], "# A query:  ?? x 11")
  expect_equal(copy_to_output[[2]], "# Database: spark_connection")

  if (using_livy()) {
    expect_error(
      print.src_spark(sc)
    )
  } else {
    expect_output(
      print.src_spark(sc)
    )
  }

  # expect_message(
  #  db_explain.spark_connection(sc, sql_mtcars),
  #  "== Physical Plan =="
  # )
  #
  # expect_silent(
  #   db_save_query.spark_connection(
  #     con = sc,
  #     sql = sql_mtcars,
  #     name = "temp_mtcars"
  #   )
  # )
  #
  # expect_silent(
  #   db_analyze.spark_connection(
  #     con = sc,
  #     table = "mtcars"
  #   )
  # )
  #
  # expect_equal(
  #   db_desc.src_spark(sc),
  #   spark_db_desc(sc)
  # )
  #
  # expect_equal(
  #   db_connection_describe.src_spark(sc),
  #   spark_db_desc(sc)
  # )
  #
})

test_clear_cache()
