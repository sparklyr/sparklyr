context("dplyr")

sc <- testthat_spark_connection()

iris_tbl <- testthat_tbl("iris")
mtcars_tbl <- testthat_tbl("mtcars")
test_requires("dplyr")

df1 <- tibble(a = 1:3, b = letters[1:3])
df2 <- tibble(b = letters[1:3], c = letters[24:26])

df1_tbl <- testthat_tbl("df1")
df2_tbl <- testthat_tbl("df2")

dplyr_across_test_cases_df <- tibble(
  x = seq(3),
  y = letters[1:3],
  t = as.POSIXct(seq(3), origin = "1970-01-01", tz = "UTC"),
  z = seq(3) + 5L
)
dplyr_across_test_cases_tbl <- testthat_tbl("dplyr_across_test_cases_df")

scalars_df <- tibble::tibble(
  row_num = seq(4),
  b_a = c(FALSE, FALSE, TRUE, TRUE),
  b_b = c(FALSE, TRUE, FALSE, TRUE),
  ba = FALSE,
  bb = TRUE,
  n_a = c(2, 3, 6, 7),
  n_b = c(3, 6, 2, 7),
  c_a = c("aa", "ab", "ca", "dd"),
  c_b = c("ab", "bc", "ac", "ad")
)
scalars_sdf <- copy_to(sc, scalars_df, overwrite = TRUE)

arrays_df <- tibble::tibble(
  row_num = seq(4),
  a_a = list(1:4, 2:5, 3:6, 4:7),
  a_b = list(4:7, 3:6, 2:5, 1:4)
)
arrays_sdf <- copy_to(sc, arrays_df, overwrite = TRUE)

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

test_that("'mutate' works as expected", {
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

  expect_equivalent(
    dplyr_across_test_cases_tbl %>% mutate(across(where(is.numeric), exp)) %>% collect(),
    dplyr_across_test_cases_df %>% mutate(across(where(is.numeric), exp))
  )
})

test_that("'across()' works with formula syntax", {
  test_requires("dplyr")
  test_requires_version("2.4.0")
  # Arrow currently throws java.lang.UnsupportedOperationException for this type
  # of query
  skip_on_arrow()

  expect_equivalent(
    dplyr_across_test_cases_tbl %>% mutate(across(where(is.numeric), ~ sin(.x^3 + .x))) %>% collect(),
    dplyr_across_test_cases_df %>% mutate(across(where(is.numeric), ~ sin(.x^3 + .x)))
  )
})

test_that("'mutate' and 'transmute' work with NSE", {
  test_requires("dplyr")
  col <- "mpg"

  expect_equivalent(
    mtcars_tbl %>% mutate(!!col := !!rlang::sym(col) * 2) %>% collect(),
    mtcars %>% mutate(!!col := !!rlang::sym(col) * 2)
  )
  expect_equivalent(
    mtcars_tbl %>% transmute(!!col := !!rlang::sym(col) * 2) %>% collect(),
    mtcars %>% transmute(!!col := !!rlang::sym(col) * 2)
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
  expect_equivalent(
    scalars_sdf %>%
      filter(if_any(starts_with("b_"))) %>%
      collect(),
    scalars_df %>%
      filter(if_any(starts_with("b_")))
  )

  expect_equivalent(
    scalars_sdf %>%
      filter(if_all(starts_with("b_"))) %>%
      collect(),
    scalars_df %>%
      filter(if_all(starts_with("b_")))
  )
})

test_that("if_all and if_any work as expected with boolean predicates", {
  test_requires_version("2.4.0")
  if (packageVersion("dbplyr") < "2") {
    skip("This feature is only supported by dbplyr 2.0 or above")
  }
  skip_on_arrow()

  expect_equivalent(
    scalars_sdf %>%
      filter(if_all(starts_with("n_"), ~ .x > 5)) %>%
      collect(),
    scalars_df %>% filter(if_all(starts_with("n_"), ~ .x > 5))
  )

  expect_equivalent(
    scalars_sdf %>%
      filter(if_any(starts_with("n_"), ~ .x > 5)) %>%
      collect(),
    scalars_df %>% filter(if_any(starts_with("n_"), ~ .x > 5))
  )

  expect_equivalent(
    scalars_sdf %>%
      filter(if_all(starts_with("n_"), c(~ .x > 5, ~ .x < 3))) %>%
      collect(),
    scalars_df %>% filter(if_all(starts_with("n_"), c(~ .x > 5, ~ .x < 3)))
  )

  expect_equivalent(
    scalars_sdf %>%
      filter(if_any(starts_with("n_"), c(~ .x > 6, ~ .x < 3))) %>%
      collect(),
    scalars_df %>% filter(if_any(starts_with("n_"), c(~ .x > 6, ~ .x < 3)))
  )

  expect_equivalent(
    scalars_sdf %>%
      dplyr::filter(if_all(starts_with("c_"), grepl, "caabac")) %>%
      pull(row_num),
    c(1L, 3L)
  )

  expect_equivalent(
    scalars_sdf %>%
      dplyr::filter(if_any(starts_with("c_"), grepl, "aac")) %>%
      pull(row_num),
    c(1L, 3L)
  )

  expect_equivalent(
    scalars_sdf %>%
      dplyr::filter(if_any(starts_with("c_"), grepl, "bcad")) %>%
      pull(row_num),
    c(2L, 3L, 4L)
  )


  expect_equivalent(
    arrays_sdf %>%
      filter(if_all(starts_with("a_"), ~ array_contains(.x, 5L))) %>%
      pull(row_num),
    c(2L, 3L)
  )

  expect_equivalent(
    arrays_sdf %>%
      filter(if_any(starts_with("a_"), ~ array_contains(.x, 7L))) %>%
      pull(row_num),
    c(1L, 4L)
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

test_that("compute() works as expected", {
  test_requires("dplyr")

  sdf <- sdf_len(sc, 10L)
  sdf_even <- sdf %>% dplyr::filter(id %% 2 == 0)
  sdf_odd <- sdf %>% dplyr::filter(id %% 2 == 1)

  expect_null(sdf_even %>% sparklyr:::sdf_remote_name())
  expect_null(sdf_odd %>% sparklyr:::sdf_remote_name())

  # caching Spark dataframes with random names
  sdf_even_cached <- sdf_even %>% dplyr::compute()
  sdf_odd_cached <- sdf_odd %>% dplyr::compute()

  expect_equivalent(
    sdf_even_cached %>% collect(),
    tibble::tibble(id = c(2L, 4L, 6L, 8L, 10L))
  )
  expect_equivalent(
    sdf_odd_cached %>% collect(),
    tibble::tibble(id = c(1L, 3L, 5L, 7L, 9L))
  )

  # caching Spark dataframes with pre-determined names
  sdf_congruent_to_1_mod_3 <- sdf %>% dplyr::filter(id %% 3 == 1)
  sdf_congruent_to_2_mod_3 <- sdf %>% dplyr::filter(id %% 3 == 2)

  expect_null(sdf_congruent_to_1_mod_3 %>% sparklyr:::sdf_remote_name())
  expect_null(sdf_congruent_to_2_mod_3 %>% sparklyr:::sdf_remote_name())

  sdf_congruent_to_1_mod_3_cached <- sdf_congruent_to_1_mod_3 %>%
    dplyr::compute(name = "congruent_to_1_mod_3")
  sdf_congruent_to_2_mod_3_cached <- sdf_congruent_to_2_mod_3 %>%
    dplyr::compute(name = "congruent_to_2_mod_3")

  expect_equal(
    sdf_congruent_to_1_mod_3_cached %>% sparklyr:::sdf_remote_name(),
    dbplyr::ident("congruent_to_1_mod_3")
  )
  expect_equivalent(
    sdf_congruent_to_2_mod_3_cached %>% sparklyr:::sdf_remote_name(),
    dbplyr::ident("congruent_to_2_mod_3")
  )

  temp_view <- sdf_congruent_to_2_mod_3 %>% dplyr::compute("temp_view")
  expect_equivalent(
    temp_view %>% sparklyr:::sdf_remote_name(), dbplyr::ident("temp_view")
  )

  expect_equivalent(
    sdf_congruent_to_1_mod_3_cached %>% collect(),
    tibble::tibble(id = c(1L, 4L, 7L, 10L))
  )
  expect_equivalent(
    sdf_congruent_to_2_mod_3_cached %>% collect(),
    tibble::tibble(id = c(2L, 5L, 8L))
  )
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

test_that("overwriting a temp view", {
  temp_view_name <- random_string()

  sdf <- sdf_len(sc, 5L) %>%
    dplyr::mutate(foo = "foo") %>%
    dplyr::compute(name = temp_view_name)
  sdf <- sdf_len(sc, 5L) %>%
    dplyr::compute(name = temp_view_name)

  expect_equivalent(sdf %>% collect(), tibble::tibble(id = seq(5)))
  expect_equivalent(
    dplyr::tbl(sc, temp_view_name) %>% collect(), tibble::tibble(id = seq(5))
  )
})

test_that("dplyr::distinct() impl is configurable", {
  options(sparklyr.dplyr_distinct.impl = "tbl_lazy")
  on.exit(options(sparklyr.dplyr_distinct.impl = NULL))

  tbl_name <- random_string()
  sdf <- copy_to(sc, data.frame(a = c(1, 1)), name = tbl_name)

  query <- sdf %>%
    dplyr::distinct() %>%
    dbplyr::remote_query() %>%
    strsplit("\\s+")

  expect_equal(
    toupper(query[[1]]),
    c("SELECT", "DISTINCT", "*", "FROM", sprintf("`%s`", toupper(tbl_name)))
  )
  expect_equivalent(
    sdf %>% dplyr::distinct() %>% collect(),
    data.frame(a = 1)
  )
})

test_that("process_tbl_name works as expected", {
  expect_equal(sparklyr:::process_tbl_name("a"), "a")
  expect_equal(sparklyr:::process_tbl_name("xyz"), "xyz")
  expect_equal(sparklyr:::process_tbl_name("x.y"), dbplyr::in_schema("x", "y"))

  df1 <- tibble::tibble(a = 1, g = 2) %>%
    copy_to(sc, ., "df1", overwrite = TRUE)
  df2 <- tibble::tibble(b = 1, g = 2) %>%
    copy_to(sc, ., "df2", overwrite = TRUE)

  query <- sql("SELECT df1.a, df2.b, df1.g FROM df1 LEFT JOIN df2 ON df1.g = df2.g")
  expect_equivalent(
    tbl(sc, query) %>% collect(),
    tibble::tibble(a = 1, b = 1, g = 2)
  )
})

test_that("in_schema() works as expected", {
  skip_on_arrow()
  db_name <- random_string("test_db_")

  queries <- c(
    sprintf("CREATE DATABASE `%s`", db_name),
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s`.`hive_tbl` (`x` INT) USING hive",
      db_name
    )
  )
  for (query in queries) {
    DBI::dbGetQuery(sc, query)
  }

  expect_equivalent(
    dplyr::tbl(sc, dbplyr::in_schema(db_name, "hive_tbl")) %>% collect(),
    tibble::tibble(x = integer())
  )
})

test_that("sdf_remote_name returns null for computed tables", {
  expect_equal(sparklyr:::sdf_remote_name(iris_tbl), ident("iris"))

  virginica_sdf <- iris_tbl %>% filter(Species == "virginica")
  expect_equal(sparklyr:::sdf_remote_name(virginica_sdf), NULL)
})

test_that("sdf_remote_name ignores the last group_by() operation(s)", {
  sdf <- iris_tbl
  for (i in seq(4)) {
    sdf <- sdf %>% dplyr::group_by(Species)
    expect_equal(sdf %>% sparklyr:::sdf_remote_name(), ident("iris"))
  }
})

test_that("sdf_remote_name ignores the last ungroup() operation(s)", {
  sdf <- iris_tbl
  for (i in seq(4)) {
    sdf <- sdf %>% dplyr::ungroup()
    expect_equal(sdf %>% sparklyr:::sdf_remote_name(), ident("iris"))
  }
})

test_that("result from dplyr::compute() has remote name", {
  sdf <- iris_tbl
  sdf <- sdf %>% dplyr::mutate(y = 5) %>% dplyr::compute()
  expect_false(is.null(sdf %>% sparklyr:::sdf_remote_name()))
})

test_that("dplyr::summarize() emits an error for summarizer using one-sided formula", {
  expect_error(
    iris_tbl %>% summarize(across(starts_with("Petal"), ~ mean(.x) ^ 2)),
    "One-sided formula is unsupported for 'summarize' on Spark dataframes"
  )
})

test_that("tbl_ptype.tbl_spark works as expected", {
  expect_equal(df1_tbl %>% dplyr::select_if(is.integer) %>% colnames(), "a")
  expect_equal(df1_tbl %>% dplyr::select_if(is.numeric) %>% colnames(), "a")
  expect_equal(df1_tbl %>% dplyr::select_if(is.character) %>% colnames(), "b")
  expect_equal(df1_tbl %>% dplyr::select_if(is.list) %>% colnames(), character())
})
