skip_connection("ml_linear_svc")
skip_on_livy()
skip_on_arrow_devel()

skip_databricks_connect()

test_that("ml_linear_svc() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_linear_svc)
})

test_that("ml_linear_svc() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    fit_intercept = FALSE,
    reg_param = 1e-4,
    max_iter = 50,
    standardization = FALSE,
    tol = 1e-05,
    threshold = 0.6,
    aggregation_depth = 3,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol",
    raw_prediction_col = "rpcol"
  )
  test_param_setting(sc, ml_linear_svc, test_args)
})


test_that("ml_linear_svc() runs", {
  test_requires_version("2.2.0")
  sc <- testthat_spark_connection()
  iris_tbl2 <- testthat_tbl("iris") %>%
    mutate(
      is_versicolor = ifelse(
        Species == "versicolor",
        "versicolor",
        "other"
      )
    ) %>%
    select(-Species)

  expect_error(
    ml_linear_svc(iris_tbl2, is_versicolor ~ .) %>%
      ml_predict(iris_tbl2) %>%
      pull(predicted_label),
    NA
  )
})

test_that("ml_linear_svc() warns and ignores weight_col on Spark 3.0+", {
  test_requires_version("3.0.0")
  test_requires("dplyr")
  sc <- testthat_spark_connection()
  iris_tbl2 <- testthat_tbl("iris") %>%
    mutate(is_versicolor = ifelse(Species == "versicolor", 1, 0)) %>%
    select(-Species)

  expect_warning(
    ml_linear_svc(iris_tbl2, is_versicolor ~ ., weight_col = "Sepal_Length"),
    "weight_col"
  )
})

test_that("ml_linear_svc() fits a transformer when no formula is given", {
  test_requires_version("2.2.0")
  test_requires("dplyr")
  sc <- testthat_spark_connection()
  prepped <- testthat_tbl("iris") %>%
    mutate(label = ifelse(Species == "versicolor", 1, 0)) %>%
    ft_vector_assembler(
      c("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"),
      "features"
    )

  # no formula -> the estimator is fit directly via ml_fit()
  model <- ml_linear_svc(prepped)
  expect_true(inherits(model, "ml_linear_svc_model"))
})

test_that("print.ml_model_linear_svc shows the formula and coefficients", {
  test_requires_version("2.2.0")
  test_requires("dplyr")
  sc <- testthat_spark_connection()
  iris_tbl2 <- testthat_tbl("iris") %>%
    mutate(
      is_versicolor = ifelse(Species == "versicolor", "versicolor", "other")
    ) %>%
    select(-Species)

  model <- ml_linear_svc(iris_tbl2, is_versicolor ~ .)
  expect_output(print(model), "Coefficients")
  expect_output(print(model), "Formula")
})

test_clear_cache()
