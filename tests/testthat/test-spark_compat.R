skip_connection("spark_compat")
skip_on_livy()
skip_on_arrow_devel()

test_that("spark_require_version enforces minimum and maximum versions", {
  with_mocked_bindings(
    spark_version = function(sc) numeric_version("3.5.0"),
    .package = "sparklyr",
    {
      # within range -> TRUE
      expect_true(spark_require_version("sc", "3.0.0"))
      expect_true(spark_require_version("sc", "3.0.0", required_max = "4.0.0"))

      # below the minimum -> error
      expect_error(
        spark_require_version("sc", "4.0.0", module = "feat"),
        "feat requires Spark 4.0.0 or higher"
      )

      # at/above the maximum -> error
      expect_error(
        spark_require_version(
          "sc",
          "3.0.0",
          module = "feat",
          required_max = "3.5.0"
        ),
        "feat is removed in Spark 3.5.0"
      )
    }
  )
})

test_that("spark_require_version infers the module from the calling function", {
  with_mocked_bindings(
    spark_version = function(sc) numeric_version("2.0.0"),
    .package = "sparklyr",
    {
      my_feature <- function() spark_require_version("sc", "3.0.0")
      expect_error(my_feature(), "my_feature requires Spark 3.0.0 or higher")
    }
  )
})

test_that("is_required_spark compares versions for connections and jobjs", {
  with_mocked_bindings(
    spark_version = function(x) numeric_version("3.5.0"),
    .package = "sparklyr",
    {
      sc <- structure(list(), class = "spark_connection")
      expect_true(is_required_spark(sc, "3.0.0"))
      expect_false(is_required_spark(sc, "4.0.0"))
    }
  )

  # the spark_jobj method resolves the connection and delegates
  with_mocked_bindings(
    spark_connection = function(x) {
      structure(list(), class = "spark_connection")
    },
    spark_version = function(x) numeric_version("3.5.0"),
    .package = "sparklyr",
    {
      jobj <- structure(list(), class = "spark_jobj")
      expect_true(is_required_spark(jobj, "3.0.0"))
      expect_false(is_required_spark(jobj, "4.0.0"))
    }
  )
})

test_that("spark_param_deprecated warns with the default and explicit version", {
  expect_warning(
    spark_param_deprecated("foo"),
    "'foo' parameter is deprecated in Spark 3.x"
  )
  expect_warning(
    spark_param_deprecated("bar", version = "4.0"),
    "'bar' parameter is deprecated in Spark 4.0"
  )
})

test_that("package_version2 accepts strings and numeric_version objects", {
  expect_identical(package_version2("1.2.3"), package_version("1.2.3"))
  expect_identical(
    package_version2(numeric_version("1.2.3")),
    package_version("1.2.3")
  )
})
