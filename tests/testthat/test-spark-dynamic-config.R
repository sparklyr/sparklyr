context("spark runtime configuration")

sc <- testthat_spark_connection()

test_that("configuration getter works", {

  if (spark_version(sc) >= "2.0.0") {
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", 1L)
    expect_equal("1",
                 unname(unlist(spark_session_config(sc, "spark.sql.shuffle.partitions.local"))))

    # make sure we didn't just get lucky
    spark_session_config(sc, "spark.sql.shuffle.partitions.local", 2L)
    expect_equal("2",
                 unname(unlist(spark_session_config(sc, "spark.sql.shuffle.partitions.local"))))
  }
})
