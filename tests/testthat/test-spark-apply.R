context("spark apply")

test_requires("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

dates <- data.frame(dates = c(as.Date("2015/12/19"), as.Date(NA), as.Date("2015/12/19")))
dates_tbl <- testthat_tbl("dates")

colnas <- data.frame(c1 = c("A", "B"), c2 = c(NA, NA))
colnas_tbl <- testthat_tbl("colnas")

test_that("'spark_apply' can apply identity function", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e) %>% collect(),
    iris_tbl %>% collect()
  )
})

test_that("'spark_apply' works with 'group_by'", {

  grouped_lm <- spark_apply(
    iris_tbl,
    function(e) {
      lm(Petal_Width ~ Petal_Length, e)$coefficients[["(Intercept)"]]
    },
    names = "Intercept",
    group_by = "Species") %>% collect()

  lapply(
    unique(iris$Species),
    function(species_test) {
      expect_equal(
        grouped_lm[grouped_lm$Species == species_test, ]$Intercept,
        lm(Petal.Width ~ Petal.Length, iris[iris$Species == species_test, ])$coefficients[["(Intercept)"]]
      )
    }
  )
})

test_columns_param <- function(cols) {
  fn <- function(x) {
    x * x
  }
  sdf <- sdf_copy_to(sc, data.frame("x" = c(seq(1.0, 10.0, 1.0))), overwrite = TRUE)
  res <- spark_apply(sdf, fn, columns = cols) %>% sdf_collect()
  if (!identical(names(cols), NULL)) {
    expect_equal(names(res), names(cols))
    col_name <- names(cols)[[1]]
  } else {
    expect_equal(names(res), cols)
    col_name <- cols[[1]]
  }
  expect_equal(nrow(res), 10)
  for (x in seq(1, 10)) {
    expect_equal(res[x,][[col_name]], x * x)
  }
}

test_that("'spark_apply' works with columns param of type vector", {
  test_columns_param(cols = c(result = "double"))
})

test_that("'spark_apply' works with columns param of type string", {
  test_columns_param(cols = c("result"))
})

test_that("'spark_apply' works with columns param of type list", {
  test_columns_param(cols = list(result = "double"))
})
