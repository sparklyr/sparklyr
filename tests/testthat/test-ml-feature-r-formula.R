skip_connection("ml-feature-r-formula")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()
test_that("ft_r_formula() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ft_r_formula)
})

test_that("ft_r_formula() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    formula = "foo ~ bar"
  )
  test_param_setting(sc, ft_r_formula, test_args)
})

test_that("r formula works as expected", {
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

test_clear_cache()

