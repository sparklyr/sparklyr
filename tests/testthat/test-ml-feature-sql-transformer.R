context("ml feature - sql transformer")

skip_databricks_connect()

sc <- testthat_spark_connection()
iris_tbl <- testthat_tbl("iris")

test_that("ft_sql_transformer() param setting", {
  test_requires_latest_spark()
  test_args <- list(
    statement = "lalallalalal"
  )
  test_param_setting(sc, ft_sql_transformer, test_args)
})

test_that("ft_sql_transformer() works", {
  transformed <- iris_tbl %>%
    dplyr::mutate(pw2 = Petal_Width * 2)

  expect_identical(
    iris_tbl %>%
      ft_sql_transformer("select *, petal_width * 2 as pw2 from `__THIS__`") %>%
      collect(),
    transformed %>%
      collect()
  )

  sql_transformer <- ft_sql_transformer(
    sc, "select *, petal_width * 2 as pw2 from `__THIS__`"
  )

  expect_equal(
    ml_param_map(sql_transformer),
    list(statement = "select *, petal_width * 2 as pw2 from `__THIS__`")
  )
})
