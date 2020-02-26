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

test_that("'spark_yarn_cluster_get_conf_property' does variable expansion for conf property value strings", {
  Sys.setenv(
    YARN_CONF_DIR = dirname(dir(getwd(), recursive = TRUE, pattern = "yarn-site.xml", full.names = TRUE))
  )

  expect_equal(
    spark_yarn_cluster_get_conf_property("yarn.resourcemanager.nonexistent"),
    character(),
    info = "nonexistent property name"
  )

  expect_equal(
    spark_yarn_cluster_get_conf_property("yarn.resourcemanager.empty"),
    "",
    info = "empty property value string"
  )

  expect_equal(
    spark_yarn_cluster_get_conf_property("yarn.resourcemanager.port"),
    "8032",
    info = "numeric property value string"
  )

  expect_equal(
    spark_yarn_cluster_get_conf_property("yarn.resourcemanager.host"),
    "invalidhost123.com",
    info = "character property value string"
  )

  expect_equal(
    spark_yarn_cluster_get_conf_property("yarn.resourcemanager.hostname"),
    "invalidhost123.com",
    info = "variable property value string"
  )

  expect_equal(
    spark_yarn_cluster_get_conf_property("yarn.resourcemanager.address"),
    "invalidhost123.com:8032",
    info = "nested variable property value string"
  )

  Sys.unsetenv("YARN_CONF_DIR")
})
