library(testthat)

context("Install")

test_that_spark_download <- function(supported = TRUE) {
  lapply(spark_versions(supported = supported), function(sparkVersion) {
    lapply(spark_versions_hadoop(sparkVersion, supported = supported), function(hadoopVersion) {
      link <- spark_versions_download_url(sparkVersion, hadoopVersion)

      expect_true(RCurl::url.exists(link), label = paste(sparkVersion, hadoopVersion), info = link)
    })
  })
}

test_that("supported spark_versions can be downloaded", {
  test_that_spark_download()
})

test_that("all spark_versions can be downloaded", {
  skip("Long running test to be manually scheduled")

  test_that_spark_download(FALSE)
})
