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

test_that("rowSums works as expected", {
  df <- do.call(
    tibble::tibble,
    lapply(
      seq(6L),
      function(x) {
        column <- list(runif(100))
        names(column) <- paste0("col", x)
        column
      }
    ) %>%
      unlist(recursive = FALSE)
  ) %>%
    dplyr::mutate(na = NA_real_)
  sdf <- copy_to(sc, df, overwrite = TRUE)
  expect_row_sums_eq <- function(x, na.rm) {
    expected <- df %>% dplyr::mutate(row_sum = rowSums(.[x], na.rm = na.rm))

    expect_equivalent(
      expected,
      sdf %>%
        dplyr::mutate(row_sum = rowSums(.[x], na.rm = na.rm)) %>%
        collect()
    )
    expect_equivalent(
      expected,
      sdf %>%
        dplyr::mutate(row_sum = rowSums(sdf[x], na.rm = na.rm)) %>%
        collect()
    )
  }
  test_cases <- list(
    4L, 2:4, 4:2, -3L, -2:-4, c(2L, 4L),
    "col5", c("col1", "col3", "col6"),
    "na", c("col1", "na"), c("col1", "na", "col3", "col6"),
    NULL
  )

  for (x in test_cases) {
    for (na.rm in c(FALSE, TRUE)) {
      expect_row_sums_eq(x, na.rm = na.rm)
    }
  }
})

test_that("weighted.mean works as expected", {
  df <- tibble::tibble(
    x = c(NA_real_, 3.1, 2.2, NA_real_, 3.3, 4),
    w = c(NA_real_, 1, 0.5, 1, 0.75, NA_real_)
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf %>% dplyr::summarize(wm = weighted.mean(x, w)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w * x, na.rm = TRUE) /
             sum(w * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )

  df <- tibble::tibble(
    x = rep(c(NA_real_, 3.1, 2.2, NA_real_, 3.3, 4), 3L),
    w = rep(c(NA_real_, 1, 0.5, 1, 0.75, NA_real_), 3L),
    grp = c(rep(1L, 6L), rep(2L, 6L), rep(3L, 6L))
  )
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equal(
    sdf %>% dplyr::summarize(wm = weighted.mean(x, w)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w * x, na.rm = TRUE) /
             sum(w * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )
  expect_equal(
    sdf %>% dplyr::summarize(wm = weighted.mean(x ^ 3, w ^ 2)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w ^ 2 * x ^ 3, na.rm = TRUE) /
             sum(w ^ 2 * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
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

  expect_equivalent(
    left_join(df1, df2) %>% dplyr::arrange(b),
    left_join(df1_tbl, df2_tbl) %>% dplyr::arrange(b) %>% collect()
  )
})

test_that("'sample_n' works as expected", {
  test_requires_version("2.0.0")
  skip_livy()
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
  skip_livy()
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
  skip_livy()
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
  skip_livy()
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
