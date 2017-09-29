context("ml sql transformer")

sc <- testthat_spark_connection()

test_that("ft_sql_transformer() works", {
  iris_tbl <- testthat_tbl("iris")
  transformed <- iris_tbl %>%
    mutate(pw2 = Petal_Width * 2)

  expect_identical(
    iris_tbl %>%
      ft_sql_transformer(ft_extract_sql(transformed)) %>%
      collect(),
    transformed %>%
      collect()
  )

  expect_identical(
    iris_tbl %>%
      ft_sql_transformer("select *, petal_width * 2 as pw2 from __THIS__") %>%
      collect(),
    transformed %>%
      collect()
  )
})
