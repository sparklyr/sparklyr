skip_on_livy()
skip_on_arrow()
skip_on_windows()

test_that("Installation and uninstallation of Spark work", {
  test_spark_version <- "1.6.3"
  test_hadoop_version <- "2.6"

  install_dir <- spark_install_dir()

  test_folder <- sprintf(
    "spark-%s-bin-hadoop%s",
    test_spark_version,
    test_hadoop_version
    )

  install_msg <- sprintf(
    "Installing Spark %s for Hadoop %s or later.",
    test_spark_version,
    test_hadoop_version
    )

  expect_message(
    spark_install(
      version = test_spark_version,
      hadoop_version = test_hadoop_version,
      verbose = TRUE
    ),
    install_msg
  )

  expect_true(
    file.exists(file.path(install_dir, test_folder))
  )

  error_msg <- sprintf(
    "Spark %s for Hadoop %s or later already installed.",
    test_spark_version,
    test_hadoop_version
  )

  expect_message(
    spark_install(
      version = test_spark_version,
      hadoop_version = test_hadoop_version,
      verbose = TRUE
    ),
    error_msg
  )

  uninstall_msg <- sprintf(
    "spark-%s-bin-hadoop%s successfully uninstalled.",
    test_spark_version,
    test_hadoop_version
  )

  expect_message(
    spark_uninstall(
      version = test_spark_version,
      hadoop_version = test_hadoop_version
    ),
    uninstall_msg
  )

  expect_false(
    file.exists(file.path(install_dir, test_folder))
  )

})


test_that("Finding invalid Spark version fails", {
  expect_warning(
    spark_install_find(version = "1.1")
  )
})


