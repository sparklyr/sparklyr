skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("supported spark_versions can be downloaded", {
  skip("")
  test_requires("RCurl")

  versions <- spark_versions(latest = FALSE)
  versions <- versions[versions$download != "", ]
  for (row in seq_len(nrow(versions))) {
    version <- versions[row, ]
    expect_true(RCurl::url.exists(version$download), label = paste(version$spark, version$hadoop), info = version$download)
  }
})

test_that("spark_versions downloads use https", {
  skip("")

  versions <- spark_versions(latest = FALSE)
  versions <- versions[versions$download != "", ]
  for (row in seq_len(nrow(versions))) {
    version <- versions[row, ]
    expect_true(length(grep("^https", version$download)) == 1, label = paste(version$spark, version$hadoop), info = version$download)
  }
})
