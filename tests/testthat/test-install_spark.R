skip("to expedite tests")
skip_connection("install_spark")
skip_on_livy()
skip_on_arrow_devel()

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
  if (spark_home == "") {
    spark_home <- NULL
  }
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

test_that("Installation and uninstallation of Spark work", {
  skip_on_windows()
  skip_on_ci()
  test_spark_version <- "4.0.1"
  test_hadoop_version <- "3"

  withr::with_options(
    new = list("spark.install.dir" = tempdir()),
    {
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
    }
  )
})


test_that("Finding invalid Spark version fails", {
  skip_on_windows()
  skip_on_ci()
  expect_warning(
    spark_install_find(version = "1.1")
  )
})


test_clear_cache()
