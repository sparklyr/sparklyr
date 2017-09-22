context("ml programming")

sc <- testthat_spark_connection()

test_that("one can program with ft_ functions", {
  iris_tbl <- testthat_tbl("iris")
  foo1 <- function(such_wow) {
    ft_binarizer(iris_tbl, "Petal_Width", "is_big", threshold = such_wow)
  }
  foo2 <- function(many_cool) {
    foo1(many_cool)
  }

  df <- ft_binarizer(iris_tbl, "Petal_Width", "is_big", threshold = 0.3) %>%
    collect()
  df1 <- collect(foo2(0.3))
  df2 <- collect(foo1(0.3))

  expect_identical(df, df1)
  expect_identical(df, df2)
})
