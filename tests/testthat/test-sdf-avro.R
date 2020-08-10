context("sdf-avro")

sc <- testthat_spark_connection()

test_that("to_avro and from_avro work properly", {
  test_requires_version("2.4.0")
  skip_databricks_connect()

  df <- tibble::tibble(
    student = list(
      list(name = "Alice", id = 1L, grade = 3.9),
      list(name = "Bob", id = 2L, grade = 3.7),
      list(name = "Carol", id = 3L, grade = 4.0)
    )
  )
  sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
  sdf_transformed <- sdf_to_avro(sdf)
  sdf_transformed %>%
    sdf_collect() %>%
    (
      function(collected) {
        expect_equal(colnames(collected), c("student"))
        expect_equal(typeof(collected$student), "list")
        expect_equal(typeof(collected$student[[1]]), "raw")
      })

  schema <- list(
    type = "record",
    name = "topLevelRecord",
    fields = list(
      list(
        name = "student",
        type = list(
          list(
            type = "record",
            name = "student",
            namespace = "topLevelRecord",
            fields = list(
              list(name = "grade", type = list("double", "null")),
              list(name = "id", type = list("long", "null")),
              list(name = "name", type = list("string", "null"))
            )
          ),
          "null"
        )
      )
    )
  )

  collected <- sdf_from_avro(
    sdf_transformed,
    c(student = rjson::toJSON(schema))
  ) %>%
    sdf_collect()

  expect_equal(colnames(collected), "student")
  expect_equal(length(collected$student), length(df$student))
  for (i in seq_along(collected$student)) {
    actual <- collected$student[[i]]$student
    expected <- df$student[[i]]
    expect_equal(
      names(actual),
      c("grade", "id", "name")
    )
    for (attr in c("grade", "id", "name")) {
      expect_equal(actual[[attr]], expected[[attr]])
    }
  }
})
