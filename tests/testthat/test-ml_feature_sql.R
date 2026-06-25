skip_connection("ml_feature_sql")
skip_on_livy()
skip_databricks_connect()

test_that("ft_dplyr_transformer() works", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  transformed <- iris_tbl %>%
    dplyr::mutate(pw2 = Petal_Width * 2)

  expect_identical(
    iris_tbl %>%
      ft_dplyr_transformer(transformed) %>%
      collect(),
    transformed %>%
      collect()
  )

  dplyr_transformer <- ft_dplyr_transformer(sc, transformed)

  expect_equal(
    ml_param_map(dplyr_transformer),
    # dbplyr (>= 2.6.0) reverted single-table SELECT to an unqualified `*`,
    # matching the pre-2.4.0 behaviour; only 2.4.0-2.5.x qualify it as
    # `` `__THIS__`.* ``.
    if (packageVersion("dbplyr") > "2.3.4" && packageVersion("dbplyr") < "2.6.0") {
      list(
        statement = "SELECT `__THIS__`.*, `Petal_Width` * 2.0 AS `pw2`\nFROM `__THIS__`"
      )
    } else {
      list(
        statement = "SELECT *, `Petal_Width` * 2.0 AS `pw2`\nFROM `__THIS__`"
      )
    }
  )
})

test_that("ft_dplyr_transformer() supports all sampling use cases", {
  test_requires_version("2.0.0", "sample_frac() requires Spark 2.0+")
  sc <- testthat_spark_connection()
  if (spark_version(sc) > "4.0.0") {
    skip("Skipped until #3504 is resolved")
  }
  sdf <- copy_to(
    sc,
    dplyr::tibble(
      id = seq(1000),
      grp = c(rep(0L, 250), rep(1L, 250), rep(2L, 250), rep(3L, 250)),
      weight = rep(seq(5), 200)
    )
  )

  reset_prng_state <- function() {
    set.seed(142857L)
  }

  sample_n_impl <- function(..., repeatable) {
    if (repeatable) {
      reset_prng_state()
    }

    dplyr::sample_n(...)
  }

  sample_frac_impl <- function(..., repeatable) {
    if (repeatable) {
      reset_prng_state()
    }

    dplyr::sample_frac(...)
  }

  for (repeatable in c(FALSE, TRUE)) {
    for (transformed in list(
      sdf %>%
        sample_n_impl(
          100,
          replace = FALSE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_n_impl(
          100,
          replace = TRUE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_frac_impl(
          0.1,
          replace = FALSE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_frac_impl(
          0.1,
          replace = TRUE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_n_impl(
          100,
          weight = weight,
          replace = FALSE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_n_impl(
          100,
          weight = weight,
          replace = TRUE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_frac_impl(
          0.1,
          weight = weight,
          replace = FALSE,
          repeatable = repeatable
        ),
      sdf %>%
        sample_frac_impl(
          0.1,
          weight = weight,
          replace = TRUE,
          repeatable = repeatable
        )
    )) {
      if (repeatable) {
        reset_prng_state()
      }
      sampled <- sdf %>%
        ft_dplyr_transformer(transformed) %>%
        collect()

      expect_equal(sampled %>% nrow(), 100)
      if (repeatable) {
        expect_equivalent(transformed %>% collect(), sampled)
      }
    }
  }

  if (spark_version(sc) >= "3.0.0") {
    for (replace in list(FALSE, TRUE)) {
      reset_prng_state()

      transformed <- sdf %>%
        dplyr::group_by(grp) %>%
        dplyr::sample_n(5, replace = replace)
      expect_equivalent(
        sdf %>% ft_dplyr_transformer(transformed) %>% collect(),
        transformed %>% collect()
      )

      transformed <- sdf %>%
        dplyr::group_by(grp) %>%
        dplyr::sample_frac(0.1, replace = replace)
      expect_equivalent(
        sdf %>% ft_dplyr_transformer(transformed) %>% collect(),
        transformed %>% collect()
      )
    }
  }
})

test_that("ft_dplyr_transformer() handles cases where table name isn't quoted (#1249)", {
  test_requires_version("2.0.0", "sample_frac() requires Spark 2.0+")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sampled <- iris_tbl %>% dplyr::select(Species)
  expect_true(
    grepl(
      "__THIS__",
      ft_dplyr_transformer(sc, sampled) %>% ml_param("statement")
    )
  )
})

test_that("ft_r_formula() default params", {
  skip_on_arrow_devel()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_r_formula)
})

test_that("ft_r_formula() param setting", {
  skip_on_arrow_devel()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    formula = "foo ~ bar"
  )
  test_param_setting(sc, ft_r_formula, test_args)
})

test_that("r formula works as expected", {
  skip_on_arrow_devel()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  pipeline <- ml_pipeline(sc) %>%
    ft_string_indexer("Species", "species_idx") %>%
    ft_one_hot_encoder("species_idx", "species_dummy") %>%
    ft_vector_assembler(list("Petal_Width", "species_dummy"), "features")

  expect_warning_on_arrow(
    df1 <- pipeline %>%
      ml_fit_and_transform(iris_tbl) %>%
      select(features, label = Sepal_Length) %>%
      collect()
  )

  expect_warning_on_arrow(
    df2 <- iris_tbl %>%
      ft_r_formula("Sepal_Length ~ Petal_Width + Species") %>%
      select(features, label) %>%
      collect()
  )

  expect_warning_on_arrow(
    df3 <- iris_tbl %>%
      ft_r_formula("Sepal_Length ~ Petal_Width + Species") %>%
      select(features, label) %>%
      collect()
  )

  expect_equal(pull(df1, features), pull(df2, features))
  expect_equal(pull(df1, features), pull(df3, features))
  expect_equal(pull(df1, label), pull(df2, label))
  expect_equal(pull(df1, label), pull(df3, label))

  args <- list(
    x = sc,
    formula = "Sepal_Length ~ Petal_Width + Species",
    features_col = "x",
    label_col = "y"
  )

  if (spark_version(sc) >= "2.1.0") {
    args <- c(args, force_index_label = TRUE)
  }

  rf <- do.call(ft_r_formula, args)

  expect_equal(
    ml_params(rf, names(args)[-1]),
    args[-1]
  )
})

test_that("ft_r_formula takes formula", {
  skip_on_arrow_devel()
  iris_tbl <- testthat_tbl("iris")

  expect_warning_on_arrow(
    v1 <- iris_tbl %>%
      ft_r_formula("Species ~ Sepal_Length + Petal_Length") %>%
      pull(features)
  )

  expect_warning_on_arrow(
    v2 <- iris_tbl %>%
      ft_r_formula(Species ~ Sepal_Length + Petal_Length) %>%
      pull(features)
  )

  expect_equal(v1, v2)
})

test_that("ft_sql_transformer() param setting", {
  skip_on_arrow_devel()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    statement = "lalallalalal"
  )
  test_param_setting(sc, ft_sql_transformer, test_args)
})

test_that("ft_sql_transformer() works", {
  skip_on_arrow_devel()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  transformed <- iris_tbl %>%
    dplyr::mutate(pw2 = Petal_Width * 2)

  expect_identical(
    iris_tbl %>%
      ft_sql_transformer("select *, petal_width * 2 as pw2 from `__THIS__`") %>%
      collect(),
    transformed %>%
      collect()
  )

  sql_transformer <- ft_sql_transformer(
    sc,
    "select *, petal_width * 2 as pw2 from `__THIS__`"
  )

  expect_equal(
    ml_param_map(sql_transformer),
    list(statement = "select *, petal_width * 2 as pw2 from `__THIS__`")
  )
})

test_clear_cache()
