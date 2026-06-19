skip_connection("dplyr-spark")

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
