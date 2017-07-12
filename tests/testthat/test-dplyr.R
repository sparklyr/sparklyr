context("dplyr")
sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")

df1 <- data_frame(a = 1:3, b = letters[1:3])
df2 <- data_frame(b = letters[1:3], c = letters[24:26])

df1_tbl <- testthat_tbl("df1")
df2_tbl <- testthat_tbl("df2")

test_that("the implementation of 'mutate' functions as expected", {
  test_requires("dplyr")

  expect_equal(
    iris %>% mutate(x = Species) %>% tbl_vars() %>% length(),
    iris_tbl %>% mutate(x = Species) %>% collect() %>% tbl_vars() %>% length()
  )
})

test_that("the implementation of 'filter' functions as expected", {
  test_requires("dplyr")

  expect_equal(
    iris %>%
      filter(`Sepal.Length` == 5.1) %>%
      filter(`Sepal.Width` == 3.5) %>%
      filter(`Petal.Length` == 1.4) %>%
      filter(`Petal.Width` == 0.2) %>%
      select(`Species`),
    iris %>%
      filter(`Sepal.Length` == 5.1) %>%
      filter(`Sepal.Width` == 3.5) %>%
      filter(`Petal.Length` == 1.4) %>%
      filter(`Petal.Width` == 0.2) %>%
      select(`Species`)
  )
})

test_that("'head' uses 'limit' clause", {
  test_requires("dplyr")
  test_requires("dbplyr")

  expect_true(
    grepl(
      "LIMIT",
      sql_render(head(iris_tbl))
    )
  )
})

test_that("'left_join' does not use 'using' clause", {
  test_requires("dplyr")
  test_requires("dbplyr")

  expect_equal(
    spark_version(sc) >= "2.0.0" && packageVersion("dplyr") < "0.5.0.90",
    grepl(
      "USING",
      sql_render(left_join(df1_tbl, df2_tbl))
    )
  )
})

test_that("the implementation of 'left_join' functions as expected", {
  test_requires("dplyr")

  expect_true(
    all.equal(
      left_join(df1, df2),
      left_join(df1_tbl, df2_tbl) %>% collect()
    )
  )
})

test_that("the implementation of 'sample_n' functions as expected", {
  test_requires("dplyr")

  # As of Spark 2.1.0, sampling functions are not exact.
  expect_lt(
    iris_tbl %>% sample_n(10) %>% collect() %>% nrow(),
    nrow(iris)
  )
})

test_that("the implementation of 'sample_frac' functions returns a sample", {
  test_requires("dplyr")

  # As of Spark 2.1.0, sampling functions are not exact.
  expect_lt(
    iris_tbl %>% sample_frac(0.2) %>% collect() %>% nrow(),
    nrow(iris)
  )
})

test_that("'sdf_broadcast' forces broadcast hash join", {
  query_plan <- df1_tbl %>%
    sdf_broadcast() %>%
    left_join(df2_tbl, by = "b") %>%
    spark_dataframe() %>%
    invoke("queryExecution") %>%
    invoke("optimizedPlan") %>%
    invoke("toString")
  expect_match(query_plan, "BroadcastHint")
})
