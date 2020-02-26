context("ml - evaluate")

test_that("ml_evaluate() works for logistic regression", {
  test_requires_version("2.0.0", "multiclass logreg requires spark 2+")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  model <- ml_logistic_regression(iris_tbl, Species ~ .)
  s <- ml_evaluate(model, iris_tbl)

  expect_identical(class(s)[[1]], "ml_logistic_regression_summary")

  expect_error(
    {
      s$label_col()
      s$predictions()
      s$probability_col()
    },
    NA
  )

  if (spark_version(sc) >= "2.0.0") {
    expect_error(
      {
        s$features_col()
      },
      NA
    )
  }

  if (spark_version(sc) >= "2.3.0") {
    expect_error(
      {
        s$prediction_col()
        s$accuracy()
        s$f_measure_by_label()
        s$false_positive_rate_by_label()
        s$labels()
        s$precision_by_label()
        s$recall_by_label()
        s$true_positive_rate_by_label()
        s$weighted_f_measure(0.1)
        s$weighted_f_measure()
        s$weighted_false_positive_rate()
        s$weighted_precision()
        s$weighted_recall()
        s$weighted_true_positive_rate()
      },
      NA
    )
  }
})

test_that("ml_evaluate() works for logistic regression (binary)", {
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris") %>%
    mutate(is_setosa = ifelse(Species == "setosa", 1, 0))
  model <- ml_logistic_regression(iris_tbl, is_setosa ~ Petal_Width)
  s <- ml_evaluate(model, iris_tbl)

  if (spark_version(sc) >= "2.3.0") {
    expect_identical(class(s)[[1]], "ml_binary_logistic_regression_summary")
  } else {
    expect_identical(class(s)[[1]], "ml_logistic_regression_summary")
  }

  expect_error(
    {
      s$features_col()
      s$label_col()
      s$predictions()
      s$probability_col()
    },
    NA
  )

  if (spark_version(sc) >= "2.3.0") {
    expect_error(
      {
        s$prediction_col()
        s$accuracy()
        s$f_measure_by_label()
        s$false_positive_rate_by_label()
        s$labels()
        s$precision_by_label()
        s$recall_by_label()
        s$true_positive_rate_by_label()
        s$weighted_f_measure(0.1)
        s$weighted_f_measure()
        s$weighted_false_positive_rate()
        s$weighted_precision()
        s$weighted_recall()
        s$weighted_true_positive_rate()
        s$area_under_roc()
        s$f_measure_by_threshold()
        s$pr()
        s$precision_by_threshold()
        s$recall_by_threshold()
        s$roc()
      },
      NA
    )
  }
})

test_that("ml_evaluate() works for linear regression", {
  mtcars_tbl <- testthat_tbl("mtcars")
  model <- ml_linear_regression(mtcars_tbl, mpg ~ .)
  s <- ml_evaluate(model, mtcars_tbl)
  expect_identical(class(s)[[1]], "ml_linear_regression_summary")

  expect_error(
    {
      s$coefficient_standard_errors()
      s$deviance_residuals()
      s$explained_variance
      s$features_col
      s$label_col
      s$mean_absolute_error
      s$mean_squared_error
      s$num_instances()
      s$p_values()
      s$prediction_col
      s$predictions
      s$r2
      s$residuals()
      s$root_mean_squared_error
      s$t_values()
      s$degrees_of_freedom
      s$r2adj
    },
    NA
  )
})

test_that("ml_evaluate() works for generalized linear regression", {
  test_requires_version("2.0.0", "glm requires spark 2+")
  mtcars_tbl <- testthat_tbl("mtcars")
  model <- ml_generalized_linear_regression(mtcars_tbl, mpg ~ .)
  s <- ml_evaluate(model, mtcars_tbl)
  expect_identical(class(s)[[1]], "ml_generalized_linear_regression_summary")

  expect_error(
    {
      s$aic()
      s$degrees_of_freedom()
      s$deviance()
      s$dispersion()
      s$null_deviance()
      s$num_instances()
      s$prediction_col
      s$predictions
      s$rank
      s$residual_degree_of_freedom()
      s$residual_degree_of_freedom_null()
      s$residuals()
    },
    NA
  )
})
