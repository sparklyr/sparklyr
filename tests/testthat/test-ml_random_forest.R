skip_connection("ml_random_forest")
skip_on_livy()
skip_on_arrow_devel()

test_that("ml_random_forest_classifier() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_random_forest_classifier)
})

test_that("ml_random_forest_classifier() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    num_trees = 10,
    subsampling_rate = 0.4,
    max_depth = 9,
    min_instances_per_node = 2,
    feature_subset_strategy = "sqrt",
    impurity = "entropy",
    min_info_gain = 0.01,
    max_bins = 14,
    seed = 3145,
    thresholds = c(0.4, 0.6),
    checkpoint_interval = 15,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "featuresawefawef",
    label_col = "labelfqf",
    prediction_col = "predictionqweqwd",
    probability_col = "probabilityasdf",
    raw_prediction_col = "rawPredictionwefawf"
  )
  test_param_setting(sc, ml_random_forest_classifier, test_args)
})

test_that("ml_random_forest_regressor() default params", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_random_forest_regressor)
})

test_that("ml_random_forest_regressor() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    num_trees = 30,
    subsampling_rate = 0.3,
    max_depth = 10,
    min_instances_per_node = 2,
    feature_subset_strategy = "sqrt",
    impurity = "variance",
    min_info_gain = 0.01,
    max_bins = 12,
    seed = 55,
    checkpoint_interval = 14,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "featureswaef",
    label_col = "asdfawf",
    prediction_col = "efwf"
  )
  test_param_setting(sc, ml_random_forest_regressor, test_args)
})

test_that("Tuning works RF", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()

  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_random_forest_regressor()

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
      random_forest_regressor = list(
        max_depth = c(5, 10)
      )
    ),
    evaluator = ml_regression_evaluator(sc),
    num_folds = 2,
    seed = 1111
  )

  cv_model <- ml_fit(cv, testthat_tbl("iris"))
  expect_is(cv_model, "ml_cross_validator_model")

  cv_metrics <- ml_validation_metrics(cv_model)
  expect_equal(dim(cv_metrics), c(2, 2))
})


test_that("rf runs successfully when all args specified", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_random_forest(
        Species ~ Sepal_Width + Sepal_Length + Petal_Width,
        type = "classification",
        feature_subset_strategy = "onethird",
        impurity = "entropy",
        max_bins = 16,
        max_depth = 3,
        min_info_gain = 1e-5,
        min_instances_per_node = 2L,
        num_trees = 25L,
        thresholds = c(1 / 2, 1 / 3, 1 / 4),
        seed = 42L
      ),
    NA
  )
})

test_that("thresholds parameter behaves as expected", {
  skip_databricks_connect()
  skip_slow("takes too long to measure coverage")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  most_predicted_label <- function(x) {
    x %>%
      count(prediction) %>%
      arrange(desc(n)) %>%
      collect() %>%
      pull(prediction) %>%
      first()
  }

  rf_predictions <- iris_tbl %>%
    ml_random_forest(
      Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(0, 1, 1)
    ) %>%
    ml_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 0)

  rf_predictions <- iris_tbl %>%
    ml_random_forest(
      Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(1, 0, 1)
    ) %>%
    ml_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 1)

  rf_predictions <- iris_tbl %>%
    ml_random_forest(
      Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(1, 1, 0)
    ) %>%
    ml_predict(iris_tbl)
  expect_equal(most_predicted_label(rf_predictions), 2)
})

test_that("error for thresholds with wrong length", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  if (spark_version(sc) < "2.1.0") {
    skip("threshold length checking implemented in 2.1.0")
  }
  expect_error(
    iris_tbl %>%
      ml_random_forest(
        Species ~ Sepal_Width,
        type = "classification",
        thresholds = c(0, 1)
      )
  )
})

test_that("error for bad impurity specification", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    iris_tbl %>%
      ml_random_forest(
        Species ~ Sepal_Width,
        type = "classification",
        impurity = "variance"
      ),
    "`impurity` must be \"gini\" or \"entropy\" for classification\\."
  )

  expect_error(
    iris_tbl %>%
      ml_random_forest(
        Sepal_Length ~ Sepal_Width,
        type = "regression",
        impurity = "gini"
      ),
    "`impurity` must be \"variance\" for regression\\."
  )
})

test_that("random seed setting works", {
  skip_databricks_connect()
  skip_slow("takes too long to measure coverage")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  model_string <- function(x) {
    spark_jobj(x$model) %>%
      invoke("toDebugString") %>%
      strsplit("\n") %>%
      unlist() %>%
      tail(-1)
  }

  m1 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification", seed = 42L)

  m2 <- iris_tbl %>%
    ml_random_forest(Species ~ Sepal_Width, type = "classification", seed = 42L)

  expect_equal(model_string(m1), model_string(m2))
})

test_that("one-tree forest agrees with ml_decision_tree()", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  rf <- iris_tbl %>%
    ml_random_forest(
      Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression",
      subsampling_rate = 1,
      feature_subset_strategy = "all",
      num_trees = 1
    )
  dt <- iris_tbl %>%
    ml_decision_tree(
      Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression"
    )

  expect_equal(
    rf %>%
      ml_predict(iris_tbl) %>%
      collect(),
    dt %>%
      ml_predict(iris_tbl) %>%
      collect(),
    tolerance = 0.5,
    scale = 1
  )
})

test_that("checkpointing works for rf", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_random_forest(
        Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
        type = "regression",
        cache_node_ids = TRUE,
        checkpoint_interval = 5
      ),
    NA
  )
})

test_that("ml_random_forest() provides informative error for bad response_col", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_random_forest(iris_tbl, Sepal.Length ~ Sepal.Width),
    "`Sepal.Length` is not a column in the input dataset\\."
  )
})

test_that("residuals() call on ml_model_random_forest_regression errors", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_random_forest(iris_tbl, Sepal_Length ~ Sepal_Width) %>% residuals(),
    "'residuals\\(\\)' not supported for ml_model_random_forest_regression"
  )
})

test_that("ml_random_forest() supports response-features syntax", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_random_forest(
      iris_tbl,
      response = "Sepal_Length",
      features = c("Sepal_Width", "Petal_Length")
    ),
    NA
  )
})

test_that("ml_tree_feature_importance() works properly", {
  skip_databricks_connect()
  test_requires_version(
    "2.0.0",
    "feature importances not available prior to spark 2.0"
  )
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  rf <- iris_tbl %>%
    ml_random_forest(
      Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression",
      subsampling_rate = 1,
      feature_subset_strategy = "all",
      num_trees = 1
    )
  dt <- iris_tbl %>%
    ml_decision_tree(
      Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression"
    )

  rf_importance <- ml_tree_feature_importance(rf)
  dt_importance <- ml_tree_feature_importance(dt)
  expect_equal(colnames(rf_importance), c("feature", "importance"))
  expect_equal(nrow(rf_importance), 3)
  expect_equal(rf_importance, dt_importance, tolerance = 0.025, scale = 1)
})

test_that("ml_tree_feature_importance() works for decision tree classification", {
  skip_databricks_connect()
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  dt <- iris_tbl %>%
    ml_decision_tree(
      Species ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width,
      type = "classification"
    )
  expect_identical(
    ml_tree_feature_importance(dt) %>% names(),
    c("feature", "importance")
  )
})

test_that("ml_feature_importances work properly (#1436)", {
  skip_databricks_connect()
  test_requires_version("2.0.0")
  sc <- testthat_spark_connection()
  mtcars_tbl <- testthat_tbl("mtcars")
  feature_cols <-
    c("mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "gear", "carb")

  vector_assembler <-
    ft_vector_assembler(
      sc,
      input_cols = feature_cols,
      output_col = "features"
    )

  estimator <-
    ml_random_forest_classifier(
      sc,
      label_col = "am"
    )

  pipeline <- ml_pipeline(vector_assembler, estimator)

  pipeline_model <- pipeline %>%
    ml_fit(mtcars_tbl)

  importances1 <- data.frame(
    feature = feature_cols,
    importance = ml_feature_importances(ml_stage(pipeline_model, 2)),
    stringsAsFactors = FALSE
  ) %>%
    arrange(desc(importance))

  model <- ml_random_forest_classifier(
    mtcars_tbl,
    formula = am ~ mpg + cyl + disp + hp + drat + wt + qsec + vs + gear + carb
  )

  importances2 <- ml_tree_feature_importance(model)

  expect_equal(importances1, importances2)
})

test_clear_cache()
