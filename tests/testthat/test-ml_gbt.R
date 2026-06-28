# Connection-free: the Spark < 2.2 GBTClassifier stage build (which omits the
# probability/raw-prediction columns) can't run on CI (Spark 3.5/4.1). Mock the
# version check + the JVM layer and confirm the reduced stage is built.
test_that("ml_gbt_classifier() builds the Spark < 2.2 stage without probability cols", {
  captured <- NULL
  with_mocked_bindings(
    spark_version = function(x) numeric_version("2.1.0"),
    spark_pipeline_stage = function(sc, class, uid, ...) {
      captured <<- list(class = class, params = list(...))
      "<stage>"
    },
    jobj_set_param_helper = function(...) NULL,
    invoke = function(jobj, method, ...) "<jobj>",
    new_ml_gbt_classifier = function(jobj) "<model>",
    .package = "sparklyr",
    {
      res <- ml_gbt_classifier(
        structure(list(), class = "spark_connection"),
        uid = "gbt_1"
      )
      expect_equal(res, "<model>")
    }
  )
  expect_equal(
    captured$class,
    "org.apache.spark.ml.classification.GBTClassifier"
  )
  # the < 2.2 branch omits probability_col / raw_prediction_col
  expect_true("prediction_col" %in% names(captured$params))
  expect_false("probability_col" %in% names(captured$params))
})

skip_connection("ml_gbt")
skip_on_livy()
skip_on_arrow_devel()

test_that("ml_gbt_classifier() default params", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_gbt_classifier)
})

test_that("ml_gbt_classifier() param setting", {
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    max_iter = 10,
    max_depth = 6,
    step_size = 0.14,
    subsampling_rate = 0.5,
    feature_subset_strategy = "sqrt",
    min_instances_per_node = 2,
    max_bins = 15,
    min_info_gain = 0.01,
    loss_type = "logistic",
    seed = 123123,
    thresholds = c(0.3, 0.7),
    checkpoint_interval = 11,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "fcol",
    label_col = "lcol",
    prediction_col = "pcol",
    probability_col = "prcol",
    raw_prediction_col = "rpcol"
  )
  test_param_setting(sc, ml_gbt_classifier, test_args)
})

test_that("ml_gbt_regressor() default params", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_default_args(sc, ml_gbt_regressor)
})

test_that("ml_gbt_regressor() param setting", {
  skip_databricks_connect()
  test_requires_version("3.0.0")
  sc <- testthat_spark_connection()
  test_args <- list(
    max_iter = 10,
    max_depth = 6,
    step_size = 0.14,
    subsampling_rate = 0.5,
    feature_subset_strategy = "sqrt",
    min_instances_per_node = 2,
    max_bins = 15,
    min_info_gain = 0.01,
    loss_type = "absolute",
    seed = 123123,
    checkpoint_interval = 11,
    cache_node_ids = TRUE,
    max_memory_in_mb = 128,
    features_col = "fcol",
    label_col = "lcol"
  )
  test_param_setting(sc, ml_gbt_regressor, test_args)
})

test_that("ML Pipeline works for GBTs", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")

  m_pipeline <- ml_pipeline(sc) %>%
    ml_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_gbt_regressor()

  expect_is(m_pipeline, "ml_pipeline")

  m_fitted <- ml_fit(m_pipeline, iris_tbl)

  expect_is(m_fitted, "ml_pipeline_model")
})


test_that("Tuning works GBT", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(Sepal_Length ~ Sepal_Width + Petal_Length) %>%
    ml_gbt_regressor()

  cv <- ml_cross_validator(
    sc,
    estimator = pipeline,
    estimator_param_maps = list(
      gbt_regressor = list(
        max_depth = c(5, 10),
        max_bins = c(32, 40)
      )
    ),
    evaluator = ml_regression_evaluator(sc),
    num_folds = 2,
    seed = 1111
  )

  cv_model <- ml_fit(cv, testthat_tbl("iris"))
  expect_is(cv_model, "ml_cross_validator_model")

  cv_metrics <- ml_validation_metrics(cv_model)
  expect_equal(dim(cv_metrics), c(4, 3))
})

test_that("gbt runs successfully when all args specified", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sans_setosa <- iris_tbl %>% filter(Species != "setosa")
  model <- iris_tbl %>%
    filter(Species != "setosa") %>%
    ml_gradient_boosted_trees(
      Species ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "classification",
      max_bins = 16,
      max_depth = 3,
      min_info_gain = 1e-5,
      min_instances_per_node = 2,
      seed = 42
    )
  expect_equal(class(model)[1], "ml_model_gbt_classification")
})

test_that("thresholds parameter behaves as expected", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sans_setosa <- iris_tbl %>% filter(Species != "setosa")
  test_requires_version(
    "2.2.0",
    "thresholds not supported for GBT for Spark <2.2.0"
  )
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  most_predicted_label <- function(x) {
    expect_warning_on_arrow(
      x %>%
        collect() %>%
        count(prediction) %>%
        arrange(desc(n)) %>%
        pull(prediction) %>%
        first()
    )
  }

  gbt_predictions <- sans_setosa %>%
    ml_gradient_boosted_trees(
      Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(0, 1)
    ) %>%
    ml_predict(sans_setosa)

  expect_equal(most_predicted_label(gbt_predictions), 0)

  gbt_predictions <- sans_setosa %>%
    ml_gradient_boosted_trees(
      Species ~ Sepal_Width,
      type = "classification",
      thresholds = c(1, 0)
    ) %>%
    ml_predict(sans_setosa)

  expect_equal(most_predicted_label(gbt_predictions), 1)
})

test_that("informative error when using Spark version that doesn't support thresholds", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sans_setosa <- iris_tbl %>% filter(Species != "setosa")
  sc <- testthat_spark_connection()
  if (spark_version(sc) >= "2.2.0") {
    skip("not applicable, threshold is supported")
  }

  expect_error(
    sans_setosa %>%
      ml_gradient_boosted_trees(
        Species ~ Sepal_Width,
        type = "classification",
        thresholds = c(0, 1)
      ),
    "`thresholds` is only supported for GBT in Spark 2.2.0\\+"
  )
})

test_that("one-tree ensemble agrees with ml_decision_tree()", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sans_setosa <- iris_tbl %>% filter(Species != "setosa")
  sc <- testthat_spark_connection()
  gbt <- iris_tbl %>%
    ml_gradient_boosted_trees(
      Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression",
      subsampling_rate = 1,
      max_iter = 1
    )
  dt <- iris_tbl %>%
    ml_decision_tree(
      Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
      type = "regression"
    )

  expect_equal(
    gbt %>%
      ml_predict(iris_tbl) %>%
      collect(),
    dt %>%
      ml_predict(iris_tbl) %>%
      collect()
  )
})

test_that("checkpointing works for gbt", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sans_setosa <- iris_tbl %>% filter(Species != "setosa")
  spark_set_checkpoint_dir(sc, tempdir())
  expect_error(
    iris_tbl %>%
      ml_gradient_boosted_trees(
        Petal_Length ~ Sepal_Width + Sepal_Length + Petal_Width,
        type = "regression",
        cache_node_ids = TRUE,
        checkpoint_interval = 5
      ),
    NA
  )
})

test_that("ml_gradient_boosted_trees() supports response-features syntax", {
  skip_databricks_connect()
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  sans_setosa <- iris_tbl %>% filter(Species != "setosa")
  sc <- testthat_spark_connection()
  iris_tbl <- testthat_tbl("iris")
  expect_error(
    ml_gradient_boosted_trees(
      iris_tbl,
      response = "Sepal_Length",
      features = c("Sepal_Width", "Petal_Length")
    ),
    NA
  )
})

test_clear_cache()
