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
#' pred_kmeans <- sdf_predict(iris_test, kmeans_model)
#' pred_b_kmeans <- sdf_predict(iris_test, b_kmeans_model)
#' pred_gmm <- sdf_predict(iris_test, gmm_model)
#'
#' # Evaluate
#' ml_clustering_evaluator(pred_kmeans)
#' ml_clustering_evaluator(pred_b_kmeans)
#' ml_clustering_evaluator(pred_gmm)
#' }
#' @export
ml_clustering_evaluator <- function(
  x, features_col = "features", prediction_col = "prediction",
  metric_name = "silhouette",
  uid = random_string("clustering_evaluator_"),
  ...
) {
  UseMethod("ml_clustering_evaluator")
}

#' @export
ml_clustering_evaluator.spark_connection <- function(
  x, features_col = "features", prediction_col = "prediction",
  metric_name = "silhouette",
  uid = random_string("clustering_evaluator_"),
  ...) {

  ml_ratify_args()

  evaluator <- ml_new_identifiable(x, "org.apache.spark.ml.evaluation.ClusteringEvaluator",
                                   uid) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setMetricName", metric_name) %>%
    new_ml_evaluator()

  evaluator
}

#' @export
ml_clustering_evaluator.tbl_spark <- function(
  x, features_col = "features", prediction_col = "prediction",
  metric_name = "silhouette",
  uid = random_string("clustering_evaluator_"),
  ...){

  sc <- spark_connection(x)

  evaluator <- ml_clustering_evaluator(
    sc, features_col, prediction_col, metric_name)

  evaluator %>%
    ml_evaluate(x)
}

# Validator
ml_validator_clustering_evaluator <- function(args, nms) {
  args %>%
    ml_validate_args({
      features_col <- ensure_scalar_character(features_col)
      prediction_col <- ensure_scalar_character(prediction_col)
      metric_name <- rlang::arg_match(metric_name, c("silhouette"))
    }) %>%
    ml_extract_args(nms)
}
