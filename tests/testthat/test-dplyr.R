context("dplyr")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
test_requires("dplyr")

df1 <- tibble(a = 1:3, b = letters[1:3])
df2 <- tibble(b = letters[1:3], c = letters[24:26])

df1_tbl <- testthat_tbl("df1")
df2_tbl <- testthat_tbl("df2")

test_that("the implementation of 'mutate' functions as expected", {
  test_requires("dplyr")

  expect_equivalent(
    iris %>% mutate(x = Species) %>% tbl_vars() %>% length(),
    iris_tbl %>% mutate(x = Species) %>% collect() %>% tbl_vars() %>% length()
  )
})

test_that("the implementation of 'filter' functions as expected", {
  test_requires("dplyr")

  expect_equivalent(
    iris_tbl %>%
      filter(Sepal_Length == 5.1) %>%
      filter(Sepal_Width == 3.5) %>%
      filter(Petal_Length == 1.4) %>%
      filter(Petal_Width == 0.2) %>%
      select(Species) %>%
      collect(),
    iris %>%
      transmute(
        Sepal_Length = `Sepal.Length`,
        Sepal_Width = `Sepal.Width`,
        Petal_Length = `Petal.Length`,
        Petal_Width = `Petal.Width`,
        Species = Species
      ) %>%
      filter(Sepal_Length == 5.1) %>%
      filter(Sepal_Width == 3.5) %>%
      filter(Petal_Length == 1.4) %>%
      filter(Petal_Width == 0.2) %>%
      transmute(Species = as.character(Species))
  )
})

test_that("grepl works as expected", {
  test_requires("dplyr")

  regexes <- c(
    "a|c", ".", "b", "x|z", "", "y", "e", "^", "$", "^$", "[0-9]", "[a-z]", "[b-z]"
  )
  verify_equivalent <- function(actual, expected) {
    # handle an edge case for arrow-enabled Spark connection
    for (col in colnames(df2)) {
      expect_equivalent(
        as.character(actual[[col]]),
        as.character(expected[[col]])
      )
    }
  }
  for (regex in regexes) {
    verify_equivalent(
      df2 %>% dplyr::filter(grepl(regex, b)),
      df2_tbl %>% dplyr::filter(grepl(regex, b)) %>% collect()
    )
    verify_equivalent(
      df2 %>% dplyr::filter(grepl(regex, c)),
      df2_tbl %>% dplyr::filter(grepl(regex, c)) %>% collect()
    )
  }
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

  expect_equivalent(
    left_join(df1, df2),
    left_join(df1_tbl, df2_tbl) %>% collect()
  )
})

test_that("'sample_n' works as expected", {
  test_requires_version("2.0.0")
  test_requires("dplyr")

  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      sample_sdf <- iris_tbl %>%
        sample_n(10, weight = !!weight, replace = replace)
      expect_equal(colnames(sample_sdf), colnames(iris_tbl))
      expect_equal(sample_sdf %>% collect() %>% nrow(), 10)

      sample_sdf <- iris_tbl %>%
        select(Petal_Length) %>%
        sample_n(10, weight = !!weight, replace = replace)
      expect_equal(colnames(sample_sdf), "Petal_Length")
      expect_equal(sample_sdf %>% collect() %>% nrow(), 10)
    }
  }
})

test_that("'sample_frac' works as expected", {
  test_requires_version("2.0.0")
  test_requires("dplyr")

  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      sample_sdf <- iris_tbl %>%
        sample_frac(0.2, weight = !!weight, replace = replace)
      expect_equal(colnames(sample_sdf), colnames(iris_tbl))
      expect_equal(sample_sdf %>% collect() %>% nrow(), round(0.2 * nrow(iris)))

      sample_sdf <- iris_tbl %>%
        select(Petal_Length) %>%
        sample_frac(0.2, weight = !!weight, replace = replace)
      expect_equal(colnames(sample_sdf), "Petal_Length")
      expect_equal(sample_sdf %>% collect() %>% nrow(), round(0.2 * nrow(iris)))
    }
  }
})

test_that("weighted sampling works as expected with integer weight columns", {
  test_requires_version("2.0.0")
  test_requires("dplyr")

  sdf <- copy_to(sc, tibble::tibble(id = seq(100), weight = seq(100)))

  for (replace in list(FALSE, TRUE)) {
    sample_sdf <- sdf %>%
      sample_n(20, weight = weight, replace = replace)
    expect_equal(colnames(sample_sdf), colnames(sdf))
    expect_equal(sample_sdf %>% collect() %>% nrow(), 20)

    sample_sdf <- sdf %>%
      sample_frac(0.2, weight = weight, replace = replace)
    expect_equal(colnames(sample_sdf), colnames(sdf))
    expect_equal(sample_sdf %>% collect() %>% nrow(), 20)
  }
})

test_that("set.seed makes sampling outcomes deterministic", {
  test_requires_version("2.0.0")
  test_requires("dplyr")

  sdf <- copy_to(sc, tibble::tibble(id = seq(1000), weight = rep(seq(5), 200)))

  for (weight in list(NULL, rlang::sym("weight"))) {
    for (replace in list(FALSE, TRUE)) {
      outcomes <- lapply(
        seq(2),
        function(i) {
          set.seed(142857L)
          sdf %>% sample_n(200, weight = weight, replace = replace) %>% collect()
        }
      )

      expect_equivalent(outcomes[[1]], outcomes[[2]])

      outcomes <- lapply(
        seq(2),
        function(i) {
          set.seed(142857L)
          sdf %>% sample_frac(0.2, weight = weight, replace = replace) %>% collect()
        }
      )

      expect_equivalent(outcomes[[1]], outcomes[[2]])
    }
  }
})

test_that("'sdf_broadcast' forces broadcast hash join", {
  if (is_testing_databricks_connect()) {
    # DB Connect's optimized plans don't display much useful information when calling toString,
    # so we use the analyzed plan instead
    plan_type <- "analyzed"
  } else {
    plan_type <- "optimizedPlan"
  }

  query_plan <- df1_tbl %>%
    sdf_broadcast() %>%
    left_join(df2_tbl, by = "b") %>%
    spark_dataframe() %>%
    invoke("queryExecution") %>%
    invoke(plan_type) %>%
    invoke("toString")
  expect_match(query_plan, "B|broadcast")
})

test_that("can compute() over tables", {
  test_requires("dplyr")

  iris_tbl %>% compute()

  succeed()
})
