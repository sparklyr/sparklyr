context("window functions")

sc <- testthat_spark_connection()

test_that("sd works in grouped mutates", {
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")

  sd_s <- iris_tbl %>%
    group_by(Species) %>%
    mutate(sd_width = sd(Sepal_Width)) %>%
    select(Species, sd_width) %>%
    distinct() %>%
    collect() %>%
    pull(sd_width)

  sd_r <- iris %>%
    group_by(Species) %>%
    mutate(sd_width = sd(Sepal.Width)) %>%
    select(Species, sd_width) %>%
    distinct() %>%
    pull(sd_width)

  expect_equal(sd_s, sd_r)
})

test_that("unsupported window function errors", {
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(distincts = n_distinct()) %>%
      collect(),
    "not supported by this database"
  )
})
