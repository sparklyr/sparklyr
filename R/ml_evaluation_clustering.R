#' Spark ML - Clustering Evaluator
#'
#' Evaluator for clustering results. The metric computes the Silhouette measure using the squared
#'   Euclidean distance. The Silhouette is a measure for the validation of the consistency
#'    within clusters. It ranges between 1 and -1, where a value close to 1 means that the
#'     points in a cluster are close to the other points in the same cluster and far from the
#'     points of the other clusters.
#'
#' @param x A \code{spark_connection} object or a \code{tbl_spark} containing label and prediction columns. The latter should be the output of \code{\link{sdf_predict}}.
#' @param features_col Name of features column.
#' @param metric_name The performance metric. Currently supports "silhouette".
#' @param prediction_col Name of the prediction column.
#' @template roxlate-ml-uid
#' @template roxlate-ml-dots
#' @return The calculated performance metric
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' formula <- Species ~ .
#'
#' # Train the models
#' kmeans_model <- ml_kmeans(iris_training, formula = formula)
#' b_kmeans_model <- ml_bisecting_kmeans(iris_training, formula = formula)
#' gmm_model <- ml_gaussian_mixture(iris_training, formula = formula)
#'
#' # Predict
#' pred_kmeans <- ml_predict(kmeans_model, iris_test)
#' pred_b_kmeans <- ml_predict(b_kmeans_model, iris_test)
#' pred_gmm <- ml_predict(gmm_model, iris_test)
#'
#' # Evaluate
#' ml_clustering_evaluator(pred_kmeans)
#' ml_clustering_evaluator(pred_b_kmeans)
#' ml_clustering_evaluator(pred_gmm)
#' }
#' @export
ml_clustering_evaluator <- function(x, features_col = "features", prediction_col = "prediction",
                                    metric_name = "silhouette", uid = random_string("clustering_evaluator_"),
                                    ...) {
  UseMethod("ml_clustering_evaluator")
}

#' @export
ml_clustering_evaluator.spark_connection <- function(x, features_col = "features", prediction_col = "prediction",
                                                     metric_name = "silhouette", uid = random_string("clustering_evaluator_"),
                                                     ...) {
  .args <- list(
    features_col = features_col,
    prediction_col = prediction_col,
    metric_name = metric_name
  ) %>%
    validator_ml_clustering_evaluator()

  evaluator <- spark_pipeline_stage(x, "org.apache.spark.ml.evaluation.ClusteringEvaluator", uid) %>%
    invoke("setFeaturesCol", .args[["features_col"]]) %>%
    invoke("setPredictionCol", .args[["prediction_col"]]) %>%
    invoke("setMetricName", .args[["metric_name"]]) %>%
    new_ml_evaluator()

  evaluator
}

#' @export
ml_clustering_evaluator.tbl_spark <- function(x, features_col = "features", prediction_col = "prediction",
                                              metric_name = "silhouette", uid = random_string("clustering_evaluator_"),
                                              ...) {
  evaluator <- ml_clustering_evaluator.spark_connection(
    x = spark_connection(x),
    features_col = features_col,
    prediction_col = prediction_col,
    metric_name = metric_name,
    uid = uid
  )

  evaluator %>%
    ml_evaluate(x)
}

# Validator
validator_ml_clustering_evaluator <- function(.args) {
  .args[["features_col"]] <- cast_string(.args[["features_col"]])
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args[["metric_name"]] <- cast_choice(.args[["metric_name"]], "silhouette")
  .args
}
