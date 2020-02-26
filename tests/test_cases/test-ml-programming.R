context("ml programming")

sc <- testthat_spark_connection()

test_that("one can program with ft_ functions (.tbl_spark)", {
  iris_tbl <- testthat_tbl("iris")
  foo1 <- function(such_wow) {
    ft_binarizer(iris_tbl, "Petal_Width", "is_big", threshold = such_wow)
  }
  foo2 <- function(many_cool) {
    foo1(many_cool)
  }
  foo3 <- function(so_quo) {
    foo2(so_quo)
  }

  df <- ft_binarizer(iris_tbl, "Petal_Width", "is_big", threshold = 0.3) %>%
    collect()
  df1 <- collect(foo1(0.3))
  df2 <- collect(foo2(0.3))
  df3 <- collect(foo3(0.3))

  expect_identical(df, df1)
  expect_identical(df, df2)
  expect_identical(df, df3)
})

test_that("one can program with ft_ function (.spark_connection)", {
  foo1 <- function(such_tidy, such_wow) {
    ft_binarizer(such_tidy, "Petal_Width", "is_big", threshold = such_wow)
  }
  foo2 <- function(many_bangs, many_cool) {
    foo1(many_bangs, many_cool)
  }
  foo3 <- function(so_unquo, so_quo) {
    foo2(so_unquo, so_quo)
  }

  bin1 <- foo1(sc, 0.3)
  bin2 <- foo2(sc, 0.3)
  bin3 <- foo3(sc, 0.3)

  for (binarizer in list(bin1, bin2, bin3))
    expect_identical(
      ml_param_map(binarizer),
      list(
        output_col = "is_big",
        threshold = 0.3,
        input_col = "Petal_Width"
      )
    )

})
