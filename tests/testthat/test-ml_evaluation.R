# Connection-free: the clustering silhouette path errors on Spark < 2.3, which
# CI can't exercise (Spark 3.5/4.1), so cover the guard by mocking the version.
test_that("ml_evaluate(clustering) requires Spark 2.3+ for silhouette", {
  fake_model <- structure(list(model = "<m>"), class = "ml_model_clustering")
  with_mocked_bindings(
    spark_connection = function(x, ...) {
      structure(list(), class = "spark_connection")
    },
    spark_version = function(x) numeric_version("2.2.0"),
    .package = "sparklyr",
    expect_error(
      ml_evaluate(fake_model, "<dataset>"),
      "Silhouette is only available"
    )
  )
})

skip_connection("ml_evaluation")
skip_on_livy()
skip_on_arrow_devel()
skip_databricks_connect()

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

test_that("ml_evaluate() works for kmeans", {
  test_requires_version("2.3.0", "ml_model_clustering requires spark 2.3+")

  iris_tbl <- testthat_tbl("iris")
  kmeans_silhouette <- ml_kmeans(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(kmeans_silhouette), "Silhouette")
  expect_equal(kmeans_silhouette$Silhouette, 0.85, tolerance = 0.001, scale = 1)
})

test_that("ml_evaluate() works for bisecting kmeans", {
  test_requires_version("2.3.0", "bisecting kmeans requires spark 2.3+")

  iris_tbl <- testthat_tbl("iris")
  bi_kmeans_silhouette <- ml_bisecting_kmeans(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(bi_kmeans_silhouette), "Silhouette")
  expect_equal(
    bi_kmeans_silhouette$Silhouette,
    0.517,
    tolerance = 0.001,
    scale = 1
  )
})

test_that("ml_evaluate() works for gaussian mixtures model", {
  test_requires_version("2.3.0", "gaussian mixtures model requires spark 2.3+")

  iris_tbl <- testthat_tbl("iris")
  gmm_silhouette <- ml_gaussian_mixture(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(gmm_silhouette), "Silhouette")
  expect_equal(gmm_silhouette$Silhouette, 0.477, tolerance = 0.001, scale = 1)
})

test_that("ml_evaluate() works for naive bayes model", {
  iris_tbl <- testthat_tbl("iris")
  nb_acc <- ml_naive_bayes(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(nb_acc), "Accuracy")
  expect_equal(nb_acc$Accuracy, 0.953, tolerance = 0.001, scale = 1)
})

test_that("ml_evaluate() works for random forest model", {
  iris_tbl <- testthat_tbl("iris")
  rf_acc <- ml_random_forest(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(rf_acc), "Accuracy")
  expect_equal(rf_acc$Accuracy, 1)
})

test_that("ml_evaluate() works for decision tree model", {
  iris_tbl <- testthat_tbl("iris")
  dt_acc <- ml_decision_tree(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(dt_acc), "Accuracy")
  expect_equal(dt_acc$Accuracy, 1)
})

test_that("ml_evaluate() works for mlp model", {
  iris_tbl <- testthat_tbl("iris")
  mlp_acc <- ml_multilayer_perceptron_classifier(
    iris_tbl,
    Species ~ .,
    layers = c(4, 3, 3)
  ) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(mlp_acc), "Accuracy")
  expect_true(mlp_acc$Accuracy >= 0.98)
})

test_that("ml_evaluate() works for gbt model", {
  iris_tbl <- testthat_tbl("iris")

  # gbt only supports binary classification
  iris_tbl <- iris_tbl %>% dplyr::filter(Species != "setosa")

  gbt_acc <- ml_gradient_boosted_trees(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(gbt_acc), "Accuracy")
  expect_equal(gbt_acc$Accuracy, 1)
})

test_that("ml_evaluate() works for svc model", {
  test_requires_version("2.2.0")

  iris_tbl <- testthat_tbl("iris")

  # gbt only supports binary classification
  iris_tbl <- iris_tbl %>% dplyr::filter(Species != "setosa")

  svc_acc <- ml_linear_svc(iris_tbl, Species ~ .) %>%
    ml_evaluate(iris_tbl)

  expect_equal(names(svc_acc), "Accuracy")
  expect_gt(svc_acc$Accuracy, 0.94)
})

test_that("basic binary classification evaluation works", {
  sc <- testthat_spark_connection()
  df <- data.frame(label = c(1, 1, 0, 0), features1 = c(1, 1, 0, 0))
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)
  model <- df_tbl %>%
    ft_vector_assembler("features1", "features") %>%
    ml_logistic_regression()
  auc <- ml_binary_classification_evaluator(
    model %>%
      ml_predict(ft_vector_assembler(df_tbl, "features1", "features")),
    label_col = "label",
    raw_prediction_col = "rawPrediction"
  )
  expect_equal(auc, 1)
})

test_that("basic regression evaluation works", {
  sc <- testthat_spark_connection()
  df <- data.frame(
    label = c(1.2, 4.5, 6.7),
    prediction = c(3, 5, 7)
  )
  df_tbl <- dplyr::copy_to(sc, df, overwrite = TRUE)
  mse_r <- df %>%
    dplyr::summarize(mse = sum((label - prediction)^2) / 3) %>%
    dplyr::pull(mse)

  mse_s <- ml_regression_evaluator(
    df_tbl,
    label_col = "label",
    prediction_col = "prediction",
    metric_name = "mse"
  )

  expect_equal(mse_r, mse_s)
})

test_that("ml evaluator print methods work", {
  sc <- testthat_spark_connection()

  expect_known_output(
    ml_binary_classification_evaluator(sc, uid = "foo"),
    output_file("print/binary-classification-evaluator.txt"),
    print = TRUE
  )

  expect_known_output(
    ml_multiclass_classification_evaluator(sc, uid = "foo"),
    output_file(
      ifelse(
        spark_version(sc) < "3.0.0",
        "print/multiclass-classification-evaluator.txt",
        "print/multiclass-classification-evaluator-spark-3.0.0.txt"
      )
    ),
    print = TRUE
  )

  expect_known_output(
    ml_regression_evaluator(sc, uid = "foo"),
    output_file("print/regression-evaluator.txt"),
    print = TRUE
  )
})

test_that("ml_clustering_evaluator() works", {
  test_requires_version("2.3.0", "ml clustering evaluator requires spark 2.3+")
  sc <- testthat_spark_connection()
  sample_data_path <- get_test_data_path("sample_kmeans_data.txt")
  sample_data <- spark_read_libsvm(
    sc,
    "sample_data",
    sample_data_path,
    overwrite = TRUE
  )
  kmeans <- ml_kmeans(sample_data, k = 2, seed = 1)
  predictions <- ml_transform(kmeans, sample_data)
  expect_equal(
    ml_clustering_evaluator(predictions),
    0.999753,
    tolerance = 1e-5,
    scale = 1
  )
})

test_that("ml_clustering_evalator() can be used in a CV pipeline", {
  test_requires_version("2.3.0", "ml clustering evaluator requires spark 2.3+")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  pipeline_cv <- ml_pipeline(sc) %>%
    ft_vector_assembler(
      input_cols = c(
        "Sepal_Width",
        "Sepal_Length",
        "Petal_Width",
        "Petal_Length"
      ),
      output_col = "features"
    ) %>%
    ml_kmeans(uid = "kmeans")

  # Specify hyperparameter grid
  grid <- list(
    kmeans = list(
      k = c(2, 3, 4)
    )
  )

  # Create the cross validator object
  cv <- ml_cross_validator(
    sc,
    estimator = pipeline_cv,
    estimator_param_maps = grid,
    evaluator = ml_clustering_evaluator(sc),
    num_folds = 3,
    parallelism = 1
  )

  # Train the models
  expect_error(
    ml_fit(cv, iris_tbl),
    NA
  )
})

test_clear_cache()
