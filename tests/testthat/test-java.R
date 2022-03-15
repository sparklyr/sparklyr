
test_that("'validate_java_version_line' fails with 1.6", {
  expect_error(
    validate_java_version_line(
      "local",
      c(
        "java version \"1.6.0\"",
        "Java(TM) SE Runtime Environment (build 1.6.0_000-000)",
        "Java HotSpot(TM) 64-Bit Server VM (build 00.000-000, mixed mode)"
      )
    )
  )
})

test_that("'validate_java_version_line' works with 1.8", {
  validate_java_version_line(
    "local",
    c(
      "java version \"1.7.0\"",
      "Java(TM) SE Runtime Environment (build 1.7.0_000-000)",
      "Java HotSpot(TM) 64-Bit Server VM (build 00.000-000, mixed mode)"
    )
  )

  succeed()
})

test_that("'validate_java_version_line' fails with java 9 in local mode", {
  expect_error(
    validate_java_version_line(
      "local",
      c(
        "java version 9",
        "Java(TM) SE Runtime Environment (build 1.9.0_000-000)",
        "Java HotSpot(TM) 64-Bit Server VM (build 25.144-000, mixed mode)"
      )
    )
  )
})

test_that("'validate_java_version_line' works with java 9 in non-local mode", {
  validate_java_version_line(
    "yarn-client",
    c(
      "java version 9",
      "Java(TM) SE Runtime Environment (build 1.9.0_000-000)",
      "Java HotSpot(TM) 64-Bit Server VM (build 25.144-000, mixed mode)"
    )
  )

  succeed()
})

# Since adoption of JEP 223, some Java versions are released only with Major
# version and no Minor.Security information. In such cases version is shown as
# X instead of X.0.0
# Also newer versions include a date as part of the version string. Must be able
# to differentiate numbers from dates from numbers from release.
test_that("'validate_java_version_line' works on version without minor.security and date exists", {
  validate_java_version_line(
    "local",
    c(
      "java version \"11\" 2018-09-25",
      "Java(TM) SE Runtime Environment (build 00+00-0000)",
      "Java HotSpot(TM) 64-Bit Server VM (build 00+00-0000, mixed mode, sharing)"
    )
  )

  succeed()
})

test_that("'validate_java_version_line' works when date is present and version has x.y.z format", {
  validate_java_version_line(
    "local",
    c(
      "java version \"15.0.2\" 2021-01-19",
      "Java(TM) SE Runtime Environment (build 15.0.2+7-27)",
      "Java HotSpot(TM) 64-Bit Server VM (build 15.0.2+7-27, mixed mode, sharing)"
    )
  )

  succeed()
})
