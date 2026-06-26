skip_connection("dplyr_verbs")
test_requires("dplyr")

sc <- testthat_spark_connection()


iris_tbl <- testthat_tbl("iris")
mtcars_tbl <- testthat_tbl("mtcars")

has_predicates <- tidyselect_data_has_predicates(mtcars_tbl)


df1 <- tibble(a = 1:3, b = letters[1:3])
df2 <- tibble(b = letters[1:3], c = letters[24:26])

df1_tbl <- testthat_tbl("df1")
df2_tbl <- testthat_tbl("df2")

sdf_5 <- copy_to(sc, data.frame(id = 1:5))
sdf_10 <- copy_to(sc, data.frame(id = 1:10))

dplyr_across_test_cases_df <- tibble(
  x = seq(3),
  y = as.character(seq(3)),
  t = as.POSIXct(seq(3), origin = "1970-01-01"),
  z = seq(3) + 5L
)
dplyr_across_test_cases_tbl <- testthat_tbl("dplyr_across_test_cases_df")

test_remote_name <- function(x, y) {
  if (packageVersion("dbplyr") <= "2.3.4") {
    y <- ident(y)
  }
  expect_equal(dbplyr::remote_name(x), y)
}

scalars_df <- dplyr::tibble(
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

arrays_df <- dplyr::tibble(
  row_num = seq(4),
  a_a = list(1:4, 2:5, 3:6, 4:7),
  a_b = list(4:7, 3:6, 2:5, 1:4)
)
arrays_sdf <- copy_to(sc, arrays_df, overwrite = TRUE)


df1a <- data.frame(a = 1:3, b = letters[1:3], stringsAsFactors = FALSE)
df2a <- data.frame(
  c = letters[1:3],
  d = letters[24:26],
  stringsAsFactors = FALSE
)
df3a <- data.frame(e = 1:2, f = letters[1:2], stringsAsFactors = FALSE)
df4a <- data.frame(b = letters[4:6], a = 4:6, stringsAsFactors = FALSE)

# sdf helper functions -------------------------------------------------------

register_test_spark_connection <- function() {
  sc <- testthat_spark_connection()
  registerDoSpark(sc)
}


u <- 1234
v <- 5678
x <- 123
y <- 456
z <- 789
fn_1 <- function(x) {
  list(fn_1 = list(x = x, u = u))
}
fn_2 <- function(x) {
  y <- 123456
  inner_fn <- function(x) {
    list(inner_fn = list(x = x, y = y))
  }
  list(
    fn_2 = list(
      fn_1(list(fn_1(list(x = x, v = y)), z = z)),
      y = y,
      z = z,
      inner_fn(z)
    )
  )
}
fn_3 <- function(x) {
  u <- 789
  inner_fn <- function(z) {
    list(inner_fn = list(u = u, v = v, x = x, y = y, z = z))
  }
  inner_fn
}
fn_4 <- fn_3(1357)

"%test%" <- function(obj, quoted_expr) {
  res <- list()
  for (impl in c("do", "dopar")) {
    res[[impl]] <- eval(parse(
      text = paste(
        "obj %",
        impl,
        "% { ",
        deparse(quoted_expr),
        " }",
        sep = ""
      )
    ))
  }
  expect_equal(res$do, res$dopar)
}


test_that("'select' works with where(...) predicate", {
  skip_if(!has_predicates)

  expect_equal(
    iris %>% select(where(is.numeric)) %>% tbl_vars() %>% gsub("\\.", "_", .),
    iris_tbl %>% select(where(is.numeric)) %>% collect() %>% tbl_vars()
  )
})

test_that("'n_distinct' summarizer works as expected", {
  skip_connection("supports-na")
  summarize_n_distinct <- function(input) {
    input %>%
      summarize(
        n_distinct_default = n_distinct(x^2),
        n_distinct_na_rm_true = n_distinct(x^2, na.rm = TRUE),
        n_distinct_na_rm_false = n_distinct(x^2, na.rm = FALSE)
      )
  }

  df <- dplyr::tibble(x = c(-3L:2L, NA, NaN, NA))
  sdf <- copy_to(sc, df, name = random_string())

  expect_equal(
    df %>% summarize_n_distinct(),
    sdf %>% summarize_n_distinct() %>% collect(),
    ignore_attr = TRUE
  )
})

test_that("'summarize' works with where(...) predicate", {
  skip_if(!has_predicates)

  expect_equivalent(
    iris %>% summarize(across(where(is.numeric), mean)),
    iris_tbl %>%
      summarize(across(where(is.numeric), ~ mean(.x, na.rm = TRUE))) %>%
      collect()
  )

  expect_equivalent(
    iris %>% summarize(across(starts_with("Petal"), mean)),
    iris_tbl %>%
      summarize(across(starts_with("Petal"), ~ mean(.x, na.rm = TRUE))) %>%
      collect()
  )

  expect_equivalent(
    iris %>% summarize(across(where(is.factor), n_distinct)),
    iris_tbl %>%
      summarize(across(where(is.character), n_distinct)) %>%
      collect()
  )
})

test_that("'mutate' works as expected", {
  expect_equal(
    iris %>% mutate(x = Species) %>% tbl_vars() %>% gsub("\\.", "_", .),
    iris_tbl %>% mutate(x = Species) %>% collect() %>% tbl_vars()
  )
})

test_that("'mutate' and 'transmute' work with NSE", {
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
  sdf <- copy_to(sc, dplyr::tibble(x = c(0.9, NA_real_, 1.1)))

  expect_equal(
    sdf %>% dplyr::mutate(x = ifelse(x > 1, "good", "bad")) %>% dplyr::pull(x),
    c("bad", NA, "good")
  )
  expect_equal(
    sdf %>%
      dplyr::mutate(x = ifelse(x > 1, "good", "bad", "unknown")) %>%
      dplyr::pull(x),
    c("bad", "unknown", "good")
  )
})

test_that("if_all and if_any work as expected", {
  test_requires_package_version("dbplyr", 2)
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
  test_requires_package_version("dbplyr", 2)
  test_requires_version("2.4.0")
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

  # Passing extra arguments to the function via `...` (e.g.
  # `if_all(..., grepl, "caabac")`) is defunct in dbplyr (>= 2.6.0); the
  # arguments must be supplied through a lambda instead.
  expect_equivalent(
    scalars_sdf %>%
      dplyr::filter(if_all(starts_with("c_"), ~ grepl(.x, "caabac"))) %>%
      pull(row_num),
    c(1L, 3L)
  )

  expect_equivalent(
    scalars_sdf %>%
      dplyr::filter(if_any(starts_with("c_"), ~ grepl(.x, "aac"))) %>%
      pull(row_num),
    c(1L, 3L)
  )

  expect_equivalent(
    scalars_sdf %>%
      dplyr::filter(if_any(starts_with("c_"), ~ grepl(.x, "bcad"))) %>%
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

test_that("'head' uses 'limit' clause", {
  test_requires("dbplyr")

  expect_true(
    grepl(
      "LIMIT",
      sql_render(head(iris_tbl))
    )
  )
})

test_that("'sdf_broadcast' forces broadcast hash join", {
  skip_connection("sdf-broadcast")
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
  sdf <- sdf_10
  sdf_even <- sdf %>% dplyr::filter(id %% 2 == 0)
  sdf_odd <- sdf %>% dplyr::filter(id %% 2 == 1)

  expect_null(dbplyr::remote_name(sdf_even))
  expect_null(dbplyr::remote_name(sdf_odd))

  # caching Spark dataframes with random names
  sdf_even_cached <- sdf_even %>% dplyr::compute()
  sdf_odd_cached <- sdf_odd %>% dplyr::compute()

  expect_equivalent(
    sdf_even_cached %>% collect(),
    dplyr::tibble(id = c(2L, 4L, 6L, 8L, 10L))
  )
  expect_equivalent(
    sdf_odd_cached %>% collect(),
    dplyr::tibble(id = c(1L, 3L, 5L, 7L, 9L))
  )

  # caching Spark dataframes with pre-determined names
  sdf_congruent_to_1_mod_3 <- sdf %>% dplyr::filter(id %% 3 == 1)
  sdf_congruent_to_2_mod_3 <- sdf %>% dplyr::filter(id %% 3 == 2)

  expect_null(sdf_congruent_to_1_mod_3 %>% dbplyr::remote_name())
  expect_null(sdf_congruent_to_2_mod_3 %>% dbplyr::remote_name())

  sdf_congruent_to_1_mod_3_cached <- sdf_congruent_to_1_mod_3 %>%
    dplyr::compute(name = "congruent_to_1_mod_3")
  sdf_congruent_to_2_mod_3_cached <- sdf_congruent_to_2_mod_3 %>%
    dplyr::compute(name = "congruent_to_2_mod_3")

  test_remote_name(
    sdf_congruent_to_1_mod_3_cached,
    "congruent_to_1_mod_3"
  )
  test_remote_name(
    sdf_congruent_to_2_mod_3_cached,
    "congruent_to_2_mod_3"
  )

  temp_view <- sdf_congruent_to_2_mod_3 %>% dplyr::compute("temp_view")

  test_remote_name(
    temp_view,
    "temp_view"
  )

  expect_equivalent(
    sdf_congruent_to_1_mod_3_cached %>% collect(),
    dplyr::tibble(id = c(1L, 4L, 7L, 10L))
  )
  expect_equivalent(
    sdf_congruent_to_2_mod_3_cached %>% collect(),
    dplyr::tibble(id = c(2L, 5L, 8L))
  )
})

test_that("mutate creates NA_real_ column correctly", {
  sdf <- sdf_5 %>% dplyr::mutate(z = NA_real_, sq = id * id)

  expect_equivalent(
    sdf %>% collect(),
    dplyr::tibble(id = seq(5), z = NA_real_, sq = id * id)
  )
})

test_that("transmute creates NA_real_ column correctly", {
  sdf <- sdf_5 %>% dplyr::transmute(z = NA_real_, sq = id * id)

  expect_equivalent(
    sdf %>% collect(),
    dplyr::tibble(z = NA_real_, sq = seq(5) * seq(5))
  )
})

test_that("overwriting a temp view", {
  # Skipping while researching why override works on non-connect methods
  skip()
  temp_view_name <- random_string()

  sdf <- sdf_5 %>%
    dplyr::mutate(foo = "foo") %>%
    dplyr::compute(name = temp_view_name)
  sdf <- sdf_5 %>%
    dplyr::compute(name = temp_view_name)

  expect_equivalent(sdf %>% collect(), dplyr::tibble(id = seq(5)))
  expect_equivalent(
    dplyr::tbl(sc, temp_view_name) %>% collect(),
    dplyr::tibble(id = seq(5))
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

  query[[1]][[3]] <- gsub(sprintf("`%s`.*", tbl_name), "*", query[[1]][[3]])

  expect_equal(
    toupper(query[[1]]),
    c("SELECT", "DISTINCT", "*", "FROM", sprintf("`%s`", toupper(tbl_name)))
  )
  expect_equivalent(
    sdf %>% dplyr::distinct() %>% collect(),
    data.frame(a = 1)
  )
})

test_that("tbl_ptype.tbl_spark works as expected", {
  skip_if(!has_predicates)
  expect_equal(df1_tbl %>% dplyr::select_if(is.integer) %>% colnames(), "a")
  expect_equal(df1_tbl %>% dplyr::select_if(is.numeric) %>% colnames(), "a")
  expect_equal(df1_tbl %>% dplyr::select_if(is.character) %>% colnames(), "b")
  expect_equal(
    df1_tbl %>% dplyr::select_if(is.list) %>% colnames(),
    character()
  )
})

test_that("summarise(.groups=)", {
  sdf <- copy_to(sc, data.frame(x = 1, y = 2)) %>%
    group_by(x, y)

  expect_equal(sdf %>% summarise() %>% group_vars(), "x")
  expect_equal(sdf %>% summarise(.groups = "drop_last") %>% group_vars(), "x")
  expect_equal(
    sdf %>% summarise(.groups = "drop") %>% group_vars(),
    character()
  )
  expect_equal(
    sdf %>% summarise(.groups = "keep") %>% group_vars(),
    c("x", "y")
  )

  df <- dplyr::tibble(val1 = c(1, 2, 1, 2), val2 = c(10, 20, 30, 40))
  sdf <- copy_to(sc, df, name = random_string())
  for (groups in c("drop_last", "drop", "keep")) {
    expect_equivalent(
      sdf %>%
        group_by(val1) %>%
        summarize(result = sum(val2, na.rm = TRUE), .groups = groups) %>%
        arrange(val1) %>%
        collect(),
      df %>%
        group_by(val1) %>%
        summarize(result = sum(val2, na.rm = TRUE), .groups = groups) %>%
        arrange(val1)
    )
  }
})

test_that("cumprod works as expected", {
  skip_on_livy()
  test_requires_version("2.0.0")
  test_requires("dplyr")

  for (stats in list(
    data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, 0, 0, 10)),
    data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, NA, 0, 10)),
    data.frame(id = 1:10, x = c(1:3, -4, 5, -6, 7, 0, NA, 10))
  )) {
    stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

    expected <- stats %>%
      arrange(id) %>%
      mutate(
        cumprod = cumprod(x)
      )

    actual <- stats_tbl %>%
      arrange(id) %>%
      mutate(
        cumprod = cumprod(x)
      ) %>%
      collect() %>%
      as.data.frame()

    expect_equal(actual, expected)
  }
})

test_that("distinct equivalent to local unique when keeping all columns", {
  skip_on_livy()
  df <- dplyr::tibble(
    x = c(2, 1, 2, 1, 1, 1, 1, 1, 2),
    y = c(2, 2, 1, 1, 1, 2, 1, 2, 2),
    z = c(2, 2, 1, 1, 2, 1, 1, 2, 2)
  )
  sdf <- copy_to(sc, df, name = random_string("tmp"))

  expect_equivalent(
    sdf %>% dplyr::distinct() %>% arrange(x, y, z) %>% collect(),
    df %>% dplyr::distinct() %>% arrange(x, y, z)
  )
})

test_that("distinct for single column works as expected", {
  skip_on_livy()
  df <- dplyr::tibble(
    x = c(1, 1, 1, 1),
    y = c(1, 1, 2, 2),
    z = c(1, 2, 1, 2)
  )
  sdf <- copy_to(sc, df, name = random_string("tmp"))
  expect_equivalent(
    sdf %>% dplyr::distinct(x, .keep_all = FALSE) %>% arrange(x) %>% collect(),
    unique(df[order(df$x), "x"])
  )
  expect_equivalent(
    sdf %>% dplyr::distinct(y, .keep_all = FALSE) %>% arrange(y) %>% collect(),
    unique(df[order(df$y), "y"])
  )
})

test_that("distinct keeps only specified cols", {
  skip_on_livy()
  expect_equivalent(
    copy_to(sc, dplyr::tibble(x = c(1, 1, 1), y = c(1, 1, 1))) %>%
      dplyr::distinct(x) %>%
      collect(),
    dplyr::tibble(x = 1)
  )
})

test_that("unless .keep_all = TRUE", {
  skip_on_livy()
  df <- dplyr::tibble(x = c(1, 1, 1), y = 3:1)

  sdf <- copy_to(sc, df, name = random_string("tmp"))

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% collect(),
    df %>% dplyr::distinct(x)
  )

  expect_equivalent(
    sdf %>% dplyr::distinct(x, .keep_all = TRUE) %>% collect(),
    df %>% dplyr::distinct(x, .keep_all = TRUE),
  )
})

test_that("distinct doesn't duplicate columns", {
  skip_on_livy()
  df <- dplyr::tibble(a = 1:3, b = 4:6)
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>% dplyr::distinct(a, a) %>% arrange(a) %>% collect(),
    df %>% dplyr::distinct(a, a) %>% arrange(a)
  )
  expect_equivalent(
    sdf %>%
      dplyr::group_by(a) %>%
      dplyr::distinct(a) %>%
      arrange(a) %>%
      collect(),
    df %>% dplyr::group_by(a) %>% dplyr::distinct(a) %>% arrange(a)
  )
})

test_that("grouped distinct always includes group cols", {
  skip_on_livy()
  sdf <- copy_to(sc, dplyr::tibble(g = c(1, 2), x = c(1, 2)))
  out <- sdf %>%
    group_by(g) %>%
    distinct(x)

  expect_equivalent(
    out %>% arrange(g) %>% collect(),
    dplyr::tibble(g = c(1, 2), x = c(1, 2))
  )
  expect_equal(dplyr::group_vars(out), "g")
})

test_that("empty grouped distinct equivalent to empty ungrouped", {
  skip_on_livy()
  sdf <- copy_to(sc, dplyr::tibble(g = c(1, 2), x = c(1, 2)))

  df1 <- sdf %>%
    distinct() %>%
    group_by(g) %>%
    collect()
  df2 <- sdf %>%
    group_by(g) %>%
    distinct() %>%
    collect()

  expect_equal(df1, df2)
})

test_that("distinct on a new, mutated variable is equivalent to mutate followed by distinct", {
  skip_on_livy()
  df <- dplyr::tibble(g = c(1, 2), x = c(1, 2))
  sdf <- copy_to(sc, df, overwrite = TRUE)

  expect_equivalent(
    sdf %>% dplyr::distinct(aa = g * 2) %>% arrange(aa) %>% collect(),
    df %>% dplyr::distinct(aa = g * 2) %>% arrange(aa)
  )
})

test_that("distinct on a new, copied variable is equivalent to mutate followed by distinct", {
  skip_on_livy()
  sdf <- copy_to(sc, dplyr::tibble(g = c(1, 2), x = c(1, 2)))

  expect_equivalent(
    sdf %>% dplyr::distinct(aa = g) %>% arrange(aa) %>% collect(),
    dplyr::tibble(aa = c(1, 2))
  )
})

test_that("distinct preserves grouping", {
  skip_on_livy()
  df1 <- dplyr::tibble(x = c(1, 1, 2, 2), y = x)
  sdf1 <- copy_to(sc, df1, name = "distinct_df1")

  df <- df1 %>% dplyr::group_by(x)
  sdf <- sdf1 %>% dplyr::group_by(x)

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% arrange(x) %>% collect(),
    df %>% dplyr::distinct(x) %>% arrange(x)
  )

  expect_equivalent(
    sdf %>% dplyr::distinct(x) %>% dplyr::group_vars(),
    df %>% dplyr::group_vars()
  )

  out <- sdf %>% dplyr::distinct(x = x + 2)

  expect_equivalent(
    sdf %>% dplyr::distinct(x = x + 2) %>% arrange(x) %>% collect(),
    df %>% dplyr::distinct(x = x + 2) %>% arrange(x)
  )

  expect_equivalent(
    sdf %>% dplyr::distinct(x = x + 2) %>% dplyr::group_vars(),
    df %>% dplyr::distinct(x = x + 2) %>% dplyr::group_vars()
  )
})

test_that("distinct followed by another lazy op works as expected", {
  skip_on_livy()
  sdf <- copy_to(
    sc,
    dplyr::tibble(
      x = 1,
      y = c(1, 1, 2, 2, 1),
      z = c(1, 2, 1, 2, 1)
    )
  )

  expect_equivalent(
    sdf %>%
      dplyr::distinct() %>%
      dplyr::mutate(r = 1) %>%
      dplyr::arrange(y, z) %>%
      collect(),
    dplyr::tibble(
      x = 1,
      y = c(1, 1, 2, 2),
      z = c(1, 2, 1, 2),
      r = 1
    )
  )
})

test_that("lead and lag take numeric values for 'n' (#925)", {
  skip_on_livy()
  test_requires("dplyr")
  example_df <- tibble(
    ID = c(10L, 1L, 4L, 2L, 8L, 5L, 7L, 9L, 3L, 6L),
    Cat = rep(letters[1:5], 2),
    Numb = c(3, 10, NA, 5, 1, 6, NA, 4, NA, 8)
  )
  example_df_tbl <- copy_to(sc, example_df, overwrite = TRUE)

  expect_equal(
    example_df_tbl %>%
      mutate(Numb1 = lag(Numb, 1, order_by = ID)) %>%
      mutate(Numb2 = lead(Cat, 1, order_by = ID)) %>%
      collect() %>%
      as_tibble(),
    example_df %>%
      mutate(Numb1 = lag(Numb, 1, order_by = ID)) %>%
      mutate(Numb2 = lead(Cat, 1, order_by = ID)) %>%
      arrange(ID)
  )
})

test_that("rowSums works as expected", {
  skip_on_livy()
  df <- do.call(
    dplyr::tibble,
    lapply(
      seq(6L),
      function(x) {
        column <- list(runif(10))
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
    4L,
    2:4,
    4:2,
    -3L,
    -2:-4,
    c(2L, 4L),
    "col5",
    c("col1", "col3", "col6"),
    "na",
    c("col1", "na"),
    c("col1", "na", "col3", "col6"),
    NULL
  )

  for (x in test_cases) {
    for (na.rm in c(FALSE, TRUE)) {
      expect_row_sums_eq(x, na.rm = na.rm)
    }
  }
})

test_that("slice_max works as expected", {
  skip_on_livy()
  # skip("skip while dbplyr/#330 is investigated")

  test_requires_version("2.0.0", "bug in spark-csv")
  test_requires("dplyr")

  test_data <- c()
  for (i in seq_along(LETTERS)) {
    test_data <- c(test_data, rep.int(LETTERS[i], times = i * 10))
  }

  test_data <- tibble("X" = test_data, stringsAsFactors = F)
  test_tbl <- copy_to(sc, test_data)

  tn1 <- test_tbl %>%
    count(X) %>%
    slice_max(n = 10, order_by = n) %>%
    collect()
  tn2 <- test_data %>%
    count(X) %>%
    slice_max(n = 10, order_by = n)

  tn2 <- tn2 %>%
    mutate(n = as.integer(n)) %>%
    arrange(X)
  tn1 <- tn1 %>%
    mutate(n = as.integer(n)) %>%
    arrange(X)

  expect_equal(tn1, tn2)
})

test_that("left_join works as expected", {
  test_requires_version("2.0.0", "dots in column names")
  test_requires("dplyr")

  s1 <- data.frame(x = 1:3, y = 4:6)
  s2 <- data.frame(x = 1:3, y = 7:9)

  d1 <- sdf_copy_to(sc, s1, overwrite = TRUE)
  d2 <- sdf_copy_to(sc, s2, overwrite = TRUE)

  j1 <- left_join(d1, d2, by = "x") %>%
    dplyr::arrange(x) %>%
    collect()
  j2 <- left_join(s1, s2, by = "x")

  expect_equivalent(j1, j2) %>% dplyr::arrange(x)
})

test_that("left_join works with default suffixes", {
  test_requires("dplyr")

  s1 <- data.frame(
    group = sample(c("A", "B"), size = 10, replace = T),
    value1 = rnorm(10),
    conflict = "df1"
  )
  s2 <- data.frame(
    group = c("A", "B"),
    conflict = "df2"
  )

  d1 <- copy_to(sc, s1, "test1", overwrite = TRUE)
  d2 <- copy_to(sc, s2, "test2", overwrite = TRUE)

  j1 <- left_join(d1, d2, by = "group")
  j2 <- collect(j1)

  expect_equal(colnames(j1), c("group", "value1", "conflict_x", "conflict_y"))

  expect_named(j2, c("group", "value1", "conflict_x", "conflict_y"))
})

test_that("joins works with user-supplied `.` suffixes", {
  test_requires("dplyr")

  s1 <- data.frame(
    group = sample(c("A", "B"), size = 10, replace = T),
    value1 = rnorm(10),
    conflict = "df1"
  )
  s2 <- data.frame(
    group = c("A", "B"),
    conflict = "df2"
  )

  d1 <- copy_to(sc, s1, "test1", overwrite = TRUE)
  d2 <- copy_to(sc, s2, "test2", overwrite = TRUE)

  j1 <- left_join(d1, d2, by = "group")
  j2 <- collect(j1)

  j3 <- left_join(d1, d2, by = "group", suffix = c(".table1", ".table2"))
  j4 <- left_join(d1, d2, by = "group", suffix = c("_table1", "_table2"))

  expect_equal(colnames(j3), colnames(j4))

  expect_message(
    left_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )

  expect_message(
    right_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )

  expect_message(
    full_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )

  expect_message(
    inner_join(d1, d2, by = "group", suffix = c(".x", ".y")),
    "Replacing '.' with '_' in suffixes. New suffixes: _x, _y"
  )
})

test_that("cor, cov, sd and var works as expected", {
  test_requires("dplyr")

  stats <- data.frame(x = 1:10, y = 10:1)
  stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

  s1 <- stats %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x),
      var = var(x)
    )

  s2 <- stats_tbl %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x, na.rm = TRUE),
      var = var(x, na.rm = TRUE)
    ) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
})

test_that("cor, cov, sd and var works as expected over groups", {
  test_requires("dplyr")

  stats <- data.frame(id = rep(c(1, 2), 5), x = 1:10, y = 10:1)
  stats_tbl <- copy_to(sc, stats, overwrite = TRUE)

  s1 <- stats %>%
    group_by(id) %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x),
      var = var(x)
    ) %>%
    arrange(id, x, y) %>%
    as.data.frame()

  s2 <- stats_tbl %>%
    group_by(id) %>%
    mutate(
      cor = cor(x, y),
      cov = cov(x, y),
      sd = sd(x, na.rm = TRUE),
      var = var(x, na.rm = TRUE)
    ) %>%
    arrange(id, x, y) %>%
    collect() %>%
    as.data.frame()

  expect_equal(s1, s2)
})

test_that("count() works in grouped mutate", {
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")

  c1 <- iris_tbl %>%
    group_by(Species) %>%
    mutate(n = count()) %>%
    select(Species, n) %>%
    distinct() %>%
    collect() %>%
    arrange(Species)
  c2 <- iris_tbl %>%
    group_by(Species) %>%
    count() %>%
    collect() %>%
    arrange(Species)

  expect_equal(c1, c2)
})

test_that("weighted.mean works as expected", {
  df <- dplyr::tibble(
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

  df <- dplyr::tibble(
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
    sdf %>% dplyr::summarize(wm = weighted.mean(x^3, w^2)) %>% dplyr::pull(wm),
    df %>%
      dplyr::summarize(
        wm = sum(w^2 * x^3, na.rm = TRUE) /
          sum(w^2 * as.numeric(!is.na(x)), na.rm = TRUE)
      ) %>%
      dplyr::pull(wm)
  )
})

test_that("set.seed makes sampling outcomes deterministic", {
  skip_on_livy()
  test_requires_version("2.0.0")
  test_requires("dplyr")
  skip_connection("sample-with-seed")
  sdf <- copy_to(sc, dplyr::tibble(id = seq(1000), weight = rep(seq(5), 200)))

  for (weight in list(NULL, rlang::sym("weight"))) {
    for (replace in list(FALSE, TRUE)) {
      outcomes <- lapply(
        seq(2),
        function(i) {
          set.seed(142857L)
          sdf %>%
            sample_n(200, weight = weight, replace = replace) %>%
            collect()
        }
      )

      expect_equivalent(outcomes[[1]], outcomes[[2]])

      outcomes <- lapply(
        seq(2),
        function(i) {
          set.seed(142857L)
          sdf %>%
            sample_frac(0.2, weight = weight, replace = replace) %>%
            collect()
        }
      )

      expect_equivalent(outcomes[[1]], outcomes[[2]])
    }
  }
})

test_that("dplyr query is executed before sampling (n)", {
  skip_on_livy()
  test_requires_version("2.0.0")
  test_requires("dplyr")
  expect_equal(
    testthat_tbl("mtcars") %>%
      select(hp, mpg) %>%
      sample_n(5) %>%
      collect() %>%
      dim(),
    c(5, 2)
  )
})

test_that("dplyr query is executed before sampling (frac)", {
  skip_on_livy()
  test_requires_version("2.0.0")
  test_requires("dplyr")
  skip_connection("sample-frac-exact")
  expect_equal(
    testthat_tbl("mtcars") %>%
      select(hp, mpg) %>%
      sample_frac(0.1) %>%
      collect() %>%
      dim(),
    c(3, 2)
  )
})

test_that("'sample_frac' works as expected", {
  skip_on_livy()
  test_requires_version("2.0.0")
  iris_tbl <- testthat_tbl("iris")
  s_exact <- spark_integ_test_skip(sc, "sample-frac-exact")
  s_replace <- spark_integ_test_skip(sc, "sample-frac-replace")
  s_weights <- spark_integ_test_skip(sc, "sample-frac-weights")

  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      skip_test <- FALSE
      if (replace && s_replace) {
        skip_test <- TRUE
      }
      if (!is.null(weight) && s_weights) {
        skip_test <- TRUE
      }
      if (!skip_test) {
        sample_sdf <- iris_tbl %>%
          sample_frac(0.2, weight = !!weight, replace = replace)

        expect_equal(colnames(sample_sdf), colnames(iris_tbl))

        sample_sdf <- iris_tbl %>%
          select(Petal_Length) %>%
          sample_frac(0.2, weight = !!weight, replace = replace)

        expect_equal(colnames(sample_sdf), "Petal_Length")

        if (!s_exact && !is.null(weight)) {
          expect_equal(
            sample_sdf %>% collect() %>% nrow(),
            round(0.2 * nrow(iris))
          )
          expect_equal(
            sample_sdf %>% collect() %>% nrow(),
            round(0.2 * nrow(iris))
          )
        }
      }
    }
  }
})

test_that("'sample_n' works as expected", {
  skip_on_livy()
  test_requires_version("2.0.0")
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")
  s_replace <- spark_integ_test_skip(sc, "sample-n-replace")
  s_weights <- spark_integ_test_skip(sc, "sample-n-weights")
  for (weight in list(NULL, rlang::sym("Petal_Length"))) {
    for (replace in list(FALSE, TRUE)) {
      skip_test <- FALSE
      if (replace && s_replace) {
        skip_test <- TRUE
      }
      if (!is.null(weight) && s_weights) {
        skip_test <- TRUE
      }
      if (!skip_test) {
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
  }
})

test_that("weighted sampling works as expected with integer weight columns", {
  skip_on_livy()
  test_requires_version("2.0.0")
  test_requires("dplyr")
  iris_tbl <- testthat_tbl("iris")
  skip_connection("sample-n-weights")
  sdf <- copy_to(sc, dplyr::tibble(id = seq(100), weight = seq(100)))
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

test_that("the (serial) implementation of 'do' functions as expected", {
  skip_on_livy()
  skip_databricks_connect()
  test_requires("ggplot2")
  diamonds_tbl <- testthat_tbl("diamonds")
  test_requires("dplyr")

  R <- diamonds %>%
    filter(color == "E" | color == "I", clarity == "SI1") %>%
    group_by(color, clarity) %>%
    do(model = lm(price ~ x + y + z, data = .))

  S <- diamonds_tbl %>%
    filter(color == "E" | color == "I", clarity == "SI1") %>%
    group_by(color, clarity) %>%
    do(model = ml_linear_regression(., price ~ x + y + z))

  R <- arrange(R, as.character(color), as.character(clarity))
  S <- arrange(S, as.character(color), as.character(clarity))

  expect_identical(nrow(R), nrow(S))
  for (i in seq_len(nrow(R))) {
    lhs <- R$model[[i]]
    rhs <- S$model[[i]]
    expect_equal(lhs$coefficients, rhs$coefficients)
  }
})

test_that("'sdf_with_sequential_id' works independent of number of partitions", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df1a_1part_tbl <- testthat_tbl("df1a_1part", data = df1a, repartition = 1L)
  df1a_10part_tbl <- testthat_tbl("df1a_10part", data = df1a, repartition = 10L)

  expect_equivalent(
    df1a_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() %>% as.numeric())
  )

  expect_equivalent(
    df1a_1part_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() %>% as.numeric())
  )

  expect_equivalent(
    df1a_10part_tbl %>% sdf_with_sequential_id() %>% collect(),
    df1a %>% mutate(id = row_number() %>% as.numeric())
  )
})

test_that("'sdf_with_sequential_id' -- 'from' argument works as expected", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")

  expect_equivalent(
    df1a_tbl %>% sdf_with_sequential_id(from = 0L) %>% collect(),
    df1a %>% mutate(id = row_number() - 1.0)
  )
  expect_equivalent(
    df1a_tbl %>% sdf_with_sequential_id(from = -1L) %>% collect(),
    df1a %>% mutate(id = row_number() - 2.0)
  )
})

test_that("'sdf_last_index' works independent of number of partitions", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df1a_1part_tbl <- testthat_tbl("df1a_1part", data = df1a, repartition = 1L)
  df1a_10part_tbl <- testthat_tbl("df1a_10part", data = df1a, repartition = 10L)

  expect_equal(df1a_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_1part_tbl %>% sdf_last_index("a"), 3)
  expect_equal(df1a_10part_tbl %>% sdf_last_index("a"), 3)
})

# cols -------------------------------------------------------

test_that("'cbind' works as expected", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df1a_10part_tbl <- testthat_tbl("df1a_10part", data = df1a, repartition = 10L)
  df2a_tbl <- testthat_tbl("df2a")
  df3a_tbl <- testthat_tbl("df3a")

  expect_equivalent(
    cbind(df1a_tbl, df2a_tbl) %>% collect(),
    cbind(df1a, df2a)
  )
  expect_equivalent(
    cbind(df1a_10part_tbl, df2a_tbl) %>% collect(),
    cbind(df1a, df2a)
  )
  expect_error(
    cbind(df1a_tbl, df3a_tbl),
    "All inputs must have the same number of rows."
  )
  expect_error(
    cbind(df1a_tbl, NULL),
    "Unable to retrieve a Spark DataFrame from object of class NULL"
  )
})

test_that("'sdf_bind_cols' agrees with 'cbind'", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")
  df3a_tbl <- testthat_tbl("df3a")

  expect_equivalent(
    sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect(),
    cbind(df1a_tbl, df2a_tbl) %>% collect()
  )
  expect_error(
    sdf_bind_cols(df1a_tbl, df3a_tbl),
    "All inputs must have the same number of rows."
  )
})

test_that("'sdf_bind_cols' handles lists", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")

  expect_equivalent(
    sdf_bind_cols(list(df1a_tbl, df2a_tbl)) %>% collect(),
    sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect()
  )
})

test_that("'sdf_bind_cols' ignores NULL", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")

  expect_equivalent(
    sdf_bind_cols(df1a_tbl, df2a_tbl, NULL) %>% collect(),
    sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect()
  )
})

test_that("'sdf_bind_cols' supports programming", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df2a_tbl <- testthat_tbl("df2a")
  df3a_tbl <- testthat_tbl("df3a")

  fn <- function(...) sdf_bind_cols(...)
  expect_equivalent(
    sdf_bind_cols(df1a_tbl, df2a_tbl) %>% collect(),
    fn(df1a_tbl, df2a_tbl) %>% collect()
  )
  expect_error(
    fn(df1a_tbl, df3a_tbl),
    "All inputs must have the same number of rows."
  )
})

test_that("'sdf_bind_cols' works with overlapping columns'", {
  skip_on_livy()
  if (spark_version(sc) < "2.0.0") {
    skip("sdf_bind_cols() workaround not available in 1.6")
  }

  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  spark_df <- sdf_bind_cols(df1a_tbl, df4a_tbl) %>% collect()
  local_df <- cbind(df1a, df2a)

  expect_equal(nrow(spark_df), nrow(local_df))
  expect_equal(ncol(spark_df), ncol(local_df))
})

# rows -------------------------------------------------------

test_that("'rbind.tbl_spark' agrees with local result ", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equivalent(
    rbind(df1a_tbl, df4a_tbl) %>% collect(),
    rbind(df1a, df4a)
  )
})

test_that("'sdf_bind_rows' agrees with local result", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equivalent(
    sdf_bind_rows(df1a_tbl, df4a_tbl) %>% collect(),
    bind_rows(df1a, df4a)
  )
  expect_equivalent(
    sdf_bind_rows(df1a_tbl, select(df4a_tbl, -a)) %>% collect(),
    bind_rows(df1a, select(df4a, -a))
  )
  expect_equivalent(
    sdf_bind_rows(df1a_tbl, select(df4a_tbl, -b)) %>% collect(),
    bind_rows(df1a, select(df4a, -b))
  )
})

test_that("'sdf_bind_rows' -- 'id' argument works as expected", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equivalent(
    sdf_bind_rows(df1a_tbl, df4a_tbl, id = "source") %>% collect(),
    bind_rows(df1a, df4a, .id = "source")
  )
  expect_equivalent(
    sdf_bind_rows(x = df1a_tbl, y = df4a_tbl, id = "source") %>% collect(),
    bind_rows(x = df1a, y = df4a, .id = "source")
  )
  expect_equivalent(
    sdf_bind_rows(x = df1a_tbl, df4a_tbl, id = "source") %>% collect(),
    bind_rows(x = df1a, df4a, .id = "source")
  )
})

test_that("'sdf_bind_rows' ignores NULL", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")
  df4a_tbl <- testthat_tbl("df4a")

  expect_equivalent(
    sdf_bind_rows(list(df1a_tbl, NULL, df4a_tbl)) %>% collect(),
    sdf_bind_rows(list(df1a_tbl, df4a_tbl)) %>% collect()
  )
})

test_that("'sdf_bind_rows' err for non-tbl_spark", {
  skip_on_livy()
  df1a_tbl <- testthat_tbl("df1a")

  expect_error(
    sdf_bind_rows(df1a_tbl, df1a),
    "all inputs must be tbl_spark"
  )
})

test_that("'sdf_bind_rows' handles column type upcasting (#804)", {
  skip_on_livy()
  test_requires("dplyr")

  df5a <- dplyr::tibble(
    year = as.double(2005:2006),
    count = c(6, NaN),
    name = c("a", "b")
  )
  df6a <- dplyr::tibble(
    year = 2007:2008,
    name = c("c", "d"),
    count = c(0, 0)
  )
  df5a_tbl <- testthat_tbl("df5a")
  df6a_tbl <- testthat_tbl("df6a")

  expect_equivalent(
    bind_rows(df5a, df6a),
    sdf_bind_rows(df5a_tbl, df6a_tbl) %>%
      collect()
  )

  expect_equivalent(
    bind_rows(df6a, df5a),
    sdf_bind_rows(df6a_tbl, df5a_tbl) %>%
      collect()
  )
})

test_that("doSpark preserves exception error message", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    expect_error(
      foreach(x = 1:10) %dopar%
        {
          if (x == 10) stop("runtime error")
        },
      regexp = "\"runtime error\""
    )
  )
})

test_that("doSpark loads required packages", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    rs <- foreach(x = 1:10) %dopar%
      {
        "testthat" %in% (.packages())
      }
  )
  expect_equal(rs %>% unlist(), rep(TRUE, 10))
})

test_that("num workers greater than 1", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_gt(foreach::getDoParWorkers(), 1)
})

test_that("doSpark works for simple loop", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(x = 1:10) %test% quote(x * x)
  )
})

test_that("doSpark works for simple loop with combine function", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(x = 1:10, .combine = sum) %test% quote(x * x)
  )
})

test_that("doSpark works for simple loop of matrices", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(x = 1:10) %test% quote(as.matrix(x))
  )
})

.test_objs <- list(
  list(1, "a"),
  list("b", 4),
  list(a = 1, b = list(c = 4, d = 5), 6, list(e = list(7)))
)

test_that("doSpark works for loop with arbitrary R objects", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(x = .test_objs) %test% quote(x)
  )
})

test_that("doSpark works for loop with arbitrary R objects with combine function", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(x = .test_objs, .combine = c) %test% quote(x)
  )
})

test_that("doSpark works for loop with arbitrary R objects with multicombine", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(
      x = 1:20,
      .combine = list,
      .multicombine = TRUE,
      .maxcombine = 5
    ) %test%
      quote(x)
  )
})

test_that("doSpark works for loop referencing external functions and variables", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  n <- 5

  expect_warning_on_arrow(
    expect_equal(
      unlist(
        foreach(x = 1:5) %dopar%
          {
            n * x
          }
      ),
      n * seq(5)
    )
  )

  expect_warning_on_arrow(
    foreach(x = 1:20, .combine = list) %test%
      quote(fn_2(list(x, y, z, fn_3(x)(y), fn_4(x))))
  )
})

test_that("doSpark works with 'qs' serializer", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  test_requires("qs")

  options(sparklyr.spark_apply.serializer = "qs")
  on.exit(options(sparklyr.spark_apply.serializer = NULL))
  expect_warning_on_arrow(
    foreach(x = .test_objs) %test% quote(x)
  )
})

test_that("doSpark works with custom serializer", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  test_requires("qs")

  options(sparklyr.spark_apply.serializer = function(x) {
    qs::qserialize(x, preset = "fast")
  })
  options(sparklyr.spark_apply.deserializer = function(x) qs::qdeserialize(x))
  on.exit({
    options(sparklyr.do_spark.serializer = NULL)
    options(sparklyr.do_spark.deserializer = NULL)
  })
  expect_warning_on_arrow(
    foreach(x = .test_objs) %test% quote(x)
  )
})

test_that("doSpark works with 'qs' serializer", {
  skip_on_livy()
  skip_on_arrow_devel()
  test_requires("foreach")
  test_requires("iterators")
  test_requires_package_version("dbplyr", 2)
  register_test_spark_connection()
  expect_warning_on_arrow(
    foreach(x = .test_objs) %test% quote(x)
  )
})

test_clear_cache()
