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
