context("ml feature - dplyr transformer")

skip_databricks_connect()

sc <- testthat_spark_connection()
iris_tbl <- testthat_tbl("iris")

test_that("ft_dplyr_transformer() works", {
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
    list(statement = "SELECT `Sepal_Length`, `Sepal_Width`, `Petal_Length`, `Petal_Width`, `Species`, `Petal_Width` * 2.0 AS `pw2`\nFROM `__THIS__`")
  )
})

test_that("ft_dplyr_transformer() supports all sampling use cases", {
  test_requires_version("2.0.0", "sample_frac() requires Spark 2.0+")

  sdf <- copy_to(
    sc,
    tibble::tibble(
      id = seq(1000),
      grp = c(rep(0L, 250), rep(1L, 250), rep(2L, 250), rep(3L, 250)),
      weight = rep(seq(5), 200)
    )
  )

  reset_prng_state <- function() { set.seed(142857L) }

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
    for (transformed in
         list(
           sdf %>% sample_n_impl(
             100, replace = FALSE, repeatable = repeatable
           ),
           sdf %>% sample_n_impl(
             100, replace = TRUE, repeatable = repeatable
           ),
           sdf %>% sample_frac_impl(
             0.1, replace = FALSE, repeatable = repeatable
           ),
           sdf %>% sample_frac_impl(
             0.1, replace = TRUE, repeatable = repeatable
           ),
           sdf %>% sample_n_impl(
             100, weight = weight, replace = FALSE, repeatable = repeatable
           ),
           sdf %>% sample_n_impl(
             100, weight = weight, replace = TRUE, repeatable = repeatable
           ),
           sdf %>% sample_frac_impl(
             0.1, weight = weight, replace = FALSE, repeatable = repeatable
           ),
           sdf %>% sample_frac_impl(
             0.1, weight = weight, replace = TRUE, repeatable = repeatable)
          )
    ) {
      if (repeatable) {
        reset_prng_state()
      }
      sampled <- sdf %>% ft_dplyr_transformer(transformed) %>% collect()

      expect_equal(sampled %>% nrow(), 100)
      if (repeatable) {
        expect_equivalent(transformed %>% collect(), sampled)
      }
    }
  }

  if (spark_version(sc) >= "3.0.0") {
    for (replace in list(FALSE, TRUE)) {
      reset_prng_state()

      transformed <- sdf %>% dplyr::group_by(grp) %>% dplyr::sample_n(5, replace = replace)
      expect_equivalent(
        sdf %>% ft_dplyr_transformer(transformed) %>% collect(),
        transformed %>% collect()
      )

      transformed <- sdf %>% dplyr::group_by(grp) %>% dplyr::sample_frac(0.1, replace = replace)
      expect_equivalent(
        sdf %>% ft_dplyr_transformer(transformed) %>% collect(),
        transformed %>% collect()
      )
    }
  }
})

test_that("ft_dplyr_transformer() handles cases where table name isn't quoted (#1249)", {
  test_requires_version("2.0.0", "sample_frac() requires Spark 2.0+")
  sampled <- iris_tbl %>% dplyr::select(Species)
  expect_true(
    grepl(
      "__THIS__",
      ft_dplyr_transformer(sc, sampled) %>% ml_param("statement")
    )
  )
})
