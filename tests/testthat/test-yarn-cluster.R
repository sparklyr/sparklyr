context("yarn cluster")

test_that("'spark_yarn_cluster_get_resource_manager_webapp' fails under HA configuration", {
  Sys.setenv(
    YARN_CONF_DIR = dirname(dir(getwd(), recursive = TRUE, pattern = "yarn-site.xml", full.names = TRUE))
  )

  testthat::expect_error(
    spark_yarn_cluster_get_resource_manager_webapp()
  )

  Sys.unsetenv("YARN_CONF_DIR")
})
