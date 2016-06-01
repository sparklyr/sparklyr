library(testthat)

context("Install")

test_that_spark_download <- function() {
  versions <- spark_versions(latest = FALSE)

  versions <- versions[versions$download != "", ]
  for (row in 1:nrow(versions)) {
    version <- versions[row, ]
    expect_true(RCurl::url.exists(version$download), label = paste(version$spark, version$hadoop), info = version$download)
  }
}

test_that("supported spark_versions can be downloaded", {
  test_that_spark_download()
})

test_that_spark_download_uses_https <- function() {
  versions <- spark_versions(latest = FALSE)

  versions <- versions[versions$download != "", ]
  for (row in 1:nrow(versions)) {
    version <- versions[row, ]
    expect_true(length(grep("^https", version$download)) == 1, label = paste(version$spark, version$hadoop), info = version$download)
  }
}

test_that("spark_versions downloads use https", {
  test_that_spark_download_uses_https()
})

