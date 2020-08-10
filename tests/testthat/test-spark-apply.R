context("spark-apply")

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
    group_by = "Species"
  ) %>% collect()

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
    expect_equal(res[x, ][[col_name]], x * x)
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

test_that("'spark_apply' works with fetch_result_as_sdf = FALSE", {
  actual <- sdf_len(sc, 4) %>%
    spark_apply(
      function(df, ctx) {
        lapply(df$id, function(id) {
          list(a = seq(id), b = ctx)
        })
      },
      context = list(1, 2, 3),
      fetch_result_as_sdf = FALSE
    )
  expected <- list(
    list(a = seq(1), b = list(1, 2, 3)),
    list(a = seq(2), b = list(1, 2, 3)),
    list(a = seq(3), b = list(1, 2, 3)),
    list(a = seq(4), b = list(1, 2, 3))
  )
  expect_equal(expected, actual)
})

test_that("'spark_apply' supports partition index as parameter", {
  expect_equivalent(
    sdf_len(sc, 10, repartition = 5) %>%
      spark_apply(
        function(df, ctx, partition_index) {
          library(dplyr)
          library(magrittr)

          df <- df %>% mutate(ctx = ctx[[1]]$ctx, partition_index = partition_index)
          df
        },
        context = list(list(ctx = "ctx")),
        partition_index_param = "partition_index",
        columns = c("id", "ctx", "partition_index")
      ) %>%
      sdf_collect(),
    data.frame(
      id = seq(1, 10),
      ctx = replicate(10, "ctx"),
      partition_index = c(sapply(seq(0, 4), function(x) c(x, x))),
      stringsAsFactors = FALSE
    )
  )
})

test_that("'spark_apply' supports nested lists as return type", {
  skip_databricks_connect()
  test_requires_version("2.4.0")

  df <- data.frame(
    json = c(
      "[{\"name\":\"Alice\",\"id\":1}, {\"name\":\"Bob\",\"id\":2}]",
      "[{\"name\":\"Carlos\",\"id\":3}, {\"name\":\"David\",\"id\":4}]",
      "[{\"name\":\"Eddie\",\"id\":5}, {\"name\":\"Frank\",\"id\":6}]"
    )
  )
  actual <- sdf_copy_to(sc, df, overwrite = TRUE) %>%
    spark_apply(
      function(df) {
        tibble::tibble(
          person = lapply(
            df$json,
            function(x) rjson::fromJSON(x)
          )
        )
      }
    ) %>%
    sdf_collect()
  expected <- list(
    list(
      list(id = 1, name = "Alice"),
      list(id = 2, name = "Bob")
    ),
    list(
      list(id = 3, name = "Carlos"),
      list(id = 4, name = "David")
    ),
    list(
      list(id = 5, name = "Eddie"),
      list(id = 6, name = "Frank")
    )
  )
  expect_equal(nrow(actual), 3)
  expect_equal(ncol(actual), 1)
  expect_equal(colnames(actual), "person")
  expect_equal(actual$person, expected)
})
