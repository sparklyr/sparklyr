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



