context("spark apply")
test_requires("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

test_that("'spark_apply' can apply identity function", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e) %>% collect(),
    iris_tbl %>% collect()
  )
})

test_that("'spark_apply' can filter columns", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e[1:1]) %>% collect(),
    iris_tbl %>% select(Sepal_Length) %>% collect()
  )
})

test_that("'spark_apply' can add columns", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) cbind(e, 1), names = c(colnames(iris_tbl), "new")) %>% collect(),
    iris_tbl %>% mutate(new = 1) %>% collect()
  )
})

test_that("'spark_apply' can concatenate", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) apply(e, 1, paste, collapse = " "), names = "s") %>% collect(),
    iris_tbl %>% transmute(s = paste(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width, Species)) %>% collect()
  )
})

test_that("'spark_apply' can filter", {
  expect_equal(
    iris_tbl %>% spark_apply(function(e) e[e$Species == "setosa",]) %>% collect(),
    iris_tbl %>% filter(Species == "setosa") %>% collect()
  )
})

test_that("'spark_apply' works with 'sdf_repartition'", {
  expect_equal(
    iris_tbl %>% sdf_repartition(2L) %>% spark_apply(function(e) e) %>% collect(),
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

test_that("'spark_apply' works with 'group_by' over multiple columns", {

  iris_tbl_ints <- iris_tbl %>%
    mutate(Petal_Width_Int = as.integer(Petal_Width))

  grouped_lm <- spark_apply(
    iris_tbl_ints,
    function(e, species, petal_width) {
      lm(Petal_Width ~ Petal_Length, e)$coefficients[["(Intercept)"]]
    },
    names = "Intercept",
    group_by = c("Species", "Petal_Width_Int")) %>% collect()

  iris_int <- iris %>% mutate(
    Petal_Width_Int = as.integer(Petal.Width),
    GroupBy = paste(Species, Petal_Width_Int, sep = "|")
  )

  lapply(
    unique(iris_int$GroupBy),
    function(group_by_entry) {
      parts <- strsplit(group_by_entry, "\\|")
      species_test <- parts[[1]][[1]]
      petal_width_test <- as.integer(parts[[1]][[2]])

      expect_equal(
        grouped_lm[grouped_lm$Species == species_test & grouped_lm$Petal_Width_Int == petal_width_test, ]$Intercept,
        lm(Petal.Width ~ Petal.Length, iris_int[iris_int$Species == species_test & iris_int$Petal_Width_Int == petal_width_test, ])$coefficients[["(Intercept)"]]
      )
    }
  )
})

test_that("'spark_apply' works over empty partitions", {
  expect_equal(
    sdf_len(sc, 10, repartition = 12) %>%
      spark_apply(function(e) e) %>%
      collect() %>%
      as.data.frame(),
    data.frame(id = seq_len(10))
  )
})
