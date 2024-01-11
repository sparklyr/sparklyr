skip_connection("install")
skip_on_livy()
skip_on_arrow_devel()

sc <- testthat_spark_connection()


test_that("Install commands work", {
  expect_true(spark_can_install())

  spark_version <- as.character(spark_version(sc))

  expect_equal(
    spark_install_version_expand(spark_version, TRUE),
    spark_version
  )

  expect_equal(
    spark_install_version_expand(spark_version, FALSE),
    spark_version
  )

  expect_equal(
    names(spark_default_version()),
    c("spark", "hadoop")
  )

  spark_home <- Sys.getenv("SPARK_HOME")
  if(spark_home == "") spark_home <- NULL
  expect_equal(spark_home(), spark_home)
})

skip_databricks_connect()
test_that("supported spark_versions can be downloaded", {
  skip("")
  test_requires("RCurl")

  versions <- spark_versions(latest = FALSE)
  versions <- versions[versions$download != "", ]
  for (row in seq_len(nrow(versions))) {
    version <- versions[row, ]
    expect_true(
      RCurl::url.exists(version$download),
      label = paste(version$spark, version$hadoop),
      info = version$download
      )
  }
})

test_that("spark_versions downloads use https", {
  skip("")

  versions <- spark_versions(latest = FALSE)
  versions <- versions[versions$download != "", ]
  for (row in seq_len(nrow(versions))) {
    version <- versions[row, ]
    expect_true(
      length(grep("^https", version$download)) == 1,
      label = paste(version$spark, version$hadoop),
      info = version$download
      )
  }
})
