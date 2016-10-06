context("install")

test_that("supported spark_versions can be downloaded", {
  skip_on_cran()
  test_requires("RCurl")

  versions <- spark_versions(latest = FALSE)
  versions <- versions[versions$download != "", ]
  for (row in 1:nrow(versions)) {
    version <- versions[row, ]
    expect_true(RCurl::url.exists(version$download), label = paste(version$spark, version$hadoop), info = version$download)
  }
})

test_that("spark_versions downloads use https", {
  skip_on_cran()

  versions <- spark_versions(latest = FALSE)
  versions <- versions[versions$download != "", ]
  for (row in 1:nrow(versions)) {
    version <- versions[row, ]
    expect_true(length(grep("^https", version$download)) == 1, label = paste(version$spark, version$hadoop), info = version$download)
  }
})
