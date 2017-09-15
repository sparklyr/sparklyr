context("java")

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


