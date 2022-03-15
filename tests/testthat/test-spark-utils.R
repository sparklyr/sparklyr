
sc <- testthat_spark_connection()

test_that("checkpoint directory getting/setting works", {
  if (Sys.getenv("TEST_DATABRICKS_CONNECT") == "true") {
    checkpoint_data_path <- paste("dbfs:/tmp/data/", "foobar", sep = "")
  } else {
    checkpoint_data_path <- "foobar"
  }
  spark_set_checkpoint_dir(sc, checkpoint_data_path)
  expect_match(spark_get_checkpoint_dir(sc), checkpoint_data_path)
})

test_that("jobj_class() works", {
  lr <- ml_logistic_regression(sc)
  expect_equal(
    jobj_class(spark_jobj(lr), simple_name = FALSE),
    c(
      "org.apache.spark.ml.classification.LogisticRegression",
      "org.apache.spark.ml.classification.ProbabilisticClassifier",
      "org.apache.spark.ml.classification.Classifier",
      "org.apache.spark.ml.Predictor",
      "org.apache.spark.ml.Estimator",
      "org.apache.spark.ml.PipelineStage",
      "java.lang.Object"
    )
  )

  expect_equal(
    jobj_class(spark_jobj(lr)),
    c(
      "LogisticRegression",
      "ProbabilisticClassifier",
      "Classifier",
      "Predictor",
      "Estimator",
      "PipelineStage",
      "Object"
    )
  )
})

test_that("debug_string() works", {
  iris_tbl <- copy_to(sc, iris, overwrite = TRUE)
  debug <- sdf_debug_string(iris_tbl, print = FALSE)
  expect_true(grepl("^\\([0-9]+\\)", debug[1]))
})
