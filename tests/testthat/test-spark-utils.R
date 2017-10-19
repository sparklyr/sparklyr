context("spark utils")

sc <- testthat_spark_connection()

test_that("checkpoint directory getting/setting works", {
  spark_set_checkpoint_dir(sc, "foobar")
  expect_match(spark_get_checkpoint_dir(sc), "foobar")
})

test_that("jobj_class() works", {
  lr <- ml_logistic_regression(sc)
  expect_equal(
    jobj_class(spark_jobj(lr), simple_name = FALSE),
    c("org.apache.spark.ml.classification.LogisticRegression",
      "org.apache.spark.ml.classification.ProbabilisticClassifier",
      "org.apache.spark.ml.classification.Classifier",
      "org.apache.spark.ml.Predictor",
      "org.apache.spark.ml.Estimator",
      "org.apache.spark.ml.PipelineStage",
      "java.lang.Object")
  )

  expect_equal(
    jobj_class(spark_jobj(lr)),
    c("LogisticRegression",
      "ProbabilisticClassifier",
      "Classifier",
      "Predictor",
      "Estimator",
      "PipelineStage",
      "Object")
  )
})
