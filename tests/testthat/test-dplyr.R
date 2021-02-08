context("dplyr")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
test_requires("dplyr")

df1 <- tibble(a = 1:3, b = letters[1:3])
df2 <- tibble(b = letters[1:3], c = letters[24:26])

df1_tbl <- testthat_tbl("df1")
df2_tbl <- testthat_tbl("df2")

test_that("'select' works with where(...) predicate", {
  test_requires("dplyr")

  expect_equal(
    iris %>% select(where(is.numeric)) %>% tbl_vars() %>% gsub("\\.", "_", .),
    iris_tbl %>% select(where(is.numeric)) %>% collect() %>% tbl_vars()
  )
})

test_that("'summarize' works with where(...) predicate", {
  test_requires("dplyr")

  expect_equivalent(
    iris %>% summarize(across(where(is.numeric), mean)),
    iris_tbl %>% summarize(across(where(is.numeric), mean)) %>% collect()
  )

  expect_equivalent(
    iris %>% summarize(across(starts_with("Petal"), mean)),
    iris_tbl %>% summarize(across(starts_with("Petal"), mean)) %>% collect()
  )

  expect_equivalent(
    iris %>% summarize(across(where(is.factor), n_distinct)),
    iris_tbl %>% summarize(across(where(is.character), n_distinct)) %>% collect()
  )
})

test_that("the implementation of 'mutate' functions as expected", {
  test_requires("dplyr")

  expect_equal(
    iris %>% mutate(x = Species) %>% tbl_vars() %>% gsub("\\.", "_", .),
    iris_tbl %>% mutate(x = Species) %>% collect() %>% tbl_vars()
  )
})

test_that("'mutate' works with `where(...)` predicate", {
  test_requires("dplyr")
  # Arrow currently throws java.lang.UnsupportedOperationException for this type
  # of query
  skip_on_arrow()

  df <- tibble::tibble(
    x = seq(3),
    y = letters[1:3],
    t = as.POSIXct(seq(3), origin = "1970-01-01"),
    z = seq(3) + 5L
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>% mutate(across(where(is.numeric), exp)) %>% collect(),
    df %>% mutate(across(where(is.numeric), exp))
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

test_that("if_else works as expected", {
  sdf <- copy_to(sc, tibble::tibble(x = c(0.9, NA_real_, 1.1)))

  expect_equal(
    sdf %>% dplyr::mutate(x = ifelse(x > 1, "good", "bad")) %>% dplyr::pull(x),
    c("bad", NA, "good")
  )
  expect_equal(
    sdf %>% dplyr::mutate(x = ifelse(x > 1, "good", "bad", "unknown")) %>%
      dplyr::pull(x),
    c("bad", "unknown", "good")
  )
})

test_that("if_all and if_any work as expected", {
  df <- tibble::tibble(
    row_num = seq(4),
    b_a = c(FALSE, FALSE, TRUE, TRUE),
    b_b = c(FALSE, TRUE, FALSE, TRUE),
    ba = FALSE,
    bb= TRUE
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>%
      filter(if_any(starts_with("b_"))) %>%
      collect(),
    df %>%
      filter(if_any(starts_with("b_")))
  )

  expect_equivalent(
    sdf %>%
      filter(if_all(starts_with("b_"))) %>%
      collect(),
    df %>%
      filter(if_all(starts_with("b_")))
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

test_that("'sdf_broadcast' forces broadcast hash join", {
  query_plan <- df1_tbl %>%
    sdf_broadcast() %>%
    left_join(df2_tbl, by = "b") %>%
    spark_dataframe() %>%
    invoke("queryExecution") %>%
    invoke("analyzed") %>%
    invoke("toString")
  expect_match(query_plan, "B|broadcast")
})

test_that("can compute() over tables", {
  test_requires("dplyr")

  iris_tbl %>% compute()

  succeed()
})

test_that("mutate creates NA_real_ column correctly", {
  sdf <- sdf_len(sc, 5L) %>% dplyr::mutate(z = NA_real_, sq = id * id)

  expect_equivalent(
    sdf %>% collect(),
    tibble::tibble(id = seq(5), z = NA_real_, sq = id * id)
  )
})

test_that("transmute creates NA_real_ column correctly", {
  sdf <- sdf_len(sc, 5L) %>% dplyr::transmute(z = NA_real_, sq = id * id)

  expect_equivalent(
    sdf %>% collect(),
    tibble::tibble(z = NA_real_, sq = seq(5) * seq(5))
  )
})

test_that("process_tbl_name works as expected", {
  expect_equal(sparklyr:::process_tbl_name("a"), "a")
  expect_equal(sparklyr:::process_tbl_name("xyz"), "xyz")
  expect_equal(sparklyr:::process_tbl_name("x.y"), dbplyr::in_schema("x", "y"))
})
