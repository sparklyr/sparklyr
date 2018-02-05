context("ml feature - sql transformer")

sc <- testthat_spark_connection()

test_that("ft_sql/dplyr_transformer() works", {
  iris_tbl <- testthat_tbl("iris")
  transformed <- iris_tbl %>%
    mutate(pw2 = Petal_Width * 2)

  expect_identical(
    iris_tbl %>%
      ft_dplyr_transformer(transformed) %>%
      collect(),
    transformed %>%
      collect()
  )

  expect_identical(
    iris_tbl %>%
      ft_sql_transformer("select *, petal_width * 2 as pw2 from `__THIS__`") %>%
      collect(),
    transformed %>%
      collect()
  )

  sql_transformer <- ft_sql_transformer(
    sc, "select *, petal_width * 2 as pw2 from `__THIS__`")

  expect_equal(
    ml_param_map(sql_transformer),
    list(statement = "select *, petal_width * 2 as pw2 from `__THIS__`")
  )

  dplyr_transformer <- ft_dplyr_transformer(sc, transformed)

  expect_equal(
    ml_param_map(dplyr_transformer),
    list(statement = "SELECT `Sepal_Length`, `Sepal_Width`, `Petal_Length`, `Petal_Width`, `Species`, `Petal_Width` * 2.0 AS `pw2`\nFROM `__THIS__`")
  )
})

test_that("ft_dplyr_transformer() handles cases where table name isn't quoted (#1249)", {
  test_requires_version("2.0.0", "sample_frac() requires Spark 2.0+")
  iris_tbl <- testthat_tbl("iris")
  sampled <- iris_tbl %>%
    sample_frac(0.01)
  expect_true(grepl("__THIS__",
        ft_dplyr_transformer(sc, sampled) %>%
          ml_param("statement")))
})
