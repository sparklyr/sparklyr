#' Spark ML -- Bisecting K-Means Clustering
#'
#' A bisecting k-means algorithm based on the paper "A comparison of document clustering techniques" by Steinbach, Karypis, and Kumar, with modification to fit Spark. The algorithm starts from a single cluster that contains all points. Iteratively it finds divisible clusters on the bottom level and bisects each of them using k-means, until there are k leaf clusters in total or no leaf clusters are divisible. The bisecting steps of clusters on the same level are grouped together to increase parallelism. If bisecting all divisible clusters on the bottom level would result more than k leaf clusters, larger clusters get higher priority.
#'
#' @template roxlate-ml-clustering-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-clustering-params
#' @template roxlate-ml-prediction-col
#' @param min_divisible_cluster_size The minimum number of points (if greater than or equal to 1.0) or the minimum proportion of points (if less than 1.0) of a divisible cluster (default: 1.0).
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' iris_tbl %>%
#'   select(-Species) %>%
#'   ml_bisecting_kmeans(k = 4 , Species ~ .)
#' }
#'
#' @export
ml_bisecting_kmeans <- function(
  x,
  formula = NULL,
  k = 4L,
  max_iter = 20L,
  seed = NULL,
  min_divisible_cluster_size = 1,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("bisecting_bisecting_kmeans_"), ...
) {
  UseMethod("ml_bisecting_kmeans")
}

#' @export
ml_bisecting_kmeans.spark_connection <- function(
  x,
  formula = NULL,
  k = 4L,
  max_iter = 20L,
  seed = NULL,
  min_divisible_cluster_size = 1,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("bisecting_kmeans_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_clustering(x, "org.apache.spark.ml.clustering.BisectingKMeans", uid,
                            features_col, k, max_iter, seed) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setMinDivisibleClusterSize", min_divisible_cluster_size)

  new_ml_bisecting_kmeans(jobj)
}

#' @export
ml_bisecting_kmeans.ml_pipeline <- function(
  x,
  formula = NULL,
  k = 4L,
  max_iter = 20L,
  seed = NULL,
  min_divisible_cluster_size = 1,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("bisecting_kmeans_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_bisecting_kmeans.tbl_spark <- function(
  x,
  formula = NULL,
  k = 4L,
  max_iter = 20L,
  seed = NULL,
  min_divisible_cluster_size = 1,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("bisecting_kmeans_"),
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor = predictor, formula = formula, features_col = features_col,
                         type = "clustering", constructor = new_ml_model_bisecting_kmeans)
  }
}

# Validator
ml_validator_bisecting_kmeans <- function(args, nms) {
  args %>%
    ml_validate_args({
      prediction_col <- ensure_scalar_character(prediction_col)
      min_divisible_cluster_size <- ensure_scalar_double(min_divisible_cluster_size)
    }) %>%
    ml_extract_args(nms)
}

# Constructors

new_ml_bisecting_kmeans <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_bisecting_kmeans")
}

new_ml_bisecting_kmeans_model <- function(jobj) {

  has_summary <- tryCatch(invoke(jobj, "hasSummary"),
                          error = function(e) FALSE)
  summary <- if (has_summary)
    new_ml_summary_bisecting_kmeans_model(invoke(jobj, "summary"))

  new_ml_clustering_model(
    jobj,
    cluster_centers = try_null(invoke(jobj, "clusterCenters")) %>%
      lapply(invoke, "toArray"),
    compute_cost = function(dataset) {
      invoke(jobj, "computeCost", spark_dataframe(dataset))
    },
    summary = summary,
    subclass = "ml_bisecting_kmeans_model")
}

new_ml_summary_bisecting_kmeans_model <- function(jobj) {
  new_ml_summary_clustering(
    jobj,
    subclass = "ml_summary_bisecting_kmeans")
}

new_ml_model_bisecting_kmeans <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  call) {

  summary <- model$summary

  centers <- model$cluster_centers %>%
    do.call(rbind, .) %>%
    as.data.frame() %>%
    rlang::set_names(feature_names)

  cost <- try_null(
    pipeline_model %>%
      ml_stage(1) %>%
      ml_transform(dataset) %>%
      model$compute_cost()
  )
  new_ml_model_clustering(
    pipeline, pipeline_model,
    model, dataset, formula,
    centers = centers,
    cost = cost,
    summary = summary,
    subclass = "ml_model_bisecting_kmeans",
    .features = feature_names
  )
}

# Generic implementations


#' @export
print.ml_model_bisecting_kmeans <- function(x, ...) {

  preamble <- sprintf(
    "K-means clustering with %s %s",
    nrow(x$centers),
    if (nrow(x$centers) == 1) "cluster" else "clusters"
  )

  cat(preamble, sep = "\n")
  print_newline()
  ml_model_print_centers(x)

  print_newline()
  cat("Within Set Sum of Squared Errors = ",
      if (is.null(x$cost)) "not computed." else x$cost
  )

}
