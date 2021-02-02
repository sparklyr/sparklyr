#' @include ml_clustering.R
#' @include ml_model_helpers.R
#' @include utils.R
NULL

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
#'   ml_bisecting_kmeans(k = 4, Species ~ .)
#' }
#'
#' @export
ml_bisecting_kmeans <- function(x, formula = NULL, k = 4, max_iter = 20,
                                seed = NULL, min_divisible_cluster_size = 1,
                                features_col = "features", prediction_col = "prediction",
                                uid = random_string("bisecting_bisecting_kmeans_"),
                                ...) {
  check_dots_used()
  UseMethod("ml_bisecting_kmeans")
}

#' @export
ml_bisecting_kmeans.spark_connection <- function(x, formula = NULL, k = 4, max_iter = 20,
                                                 seed = NULL, min_divisible_cluster_size = 1,
                                                 features_col = "features", prediction_col = "prediction",
                                                 uid = random_string("bisecting_bisecting_kmeans_"),
                                                 ...) {
  .args <- list(
    k = k,
    max_iter = max_iter,
    seed = seed,
    min_divisible_cluster_size = min_divisible_cluster_size,
    features_col = features_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_bisecting_kmeans()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.clustering.BisectingKMeans", uid,
    features_col = .args[["features_col"]],
    k = .args[["k"]], max_iter = .args[["max_iter"]], seed = .args[["seed"]]
  ) %>%
    invoke("setPredictionCol", .args[["prediction_col"]]) %>%
    invoke("setMinDivisibleClusterSize", .args[["min_divisible_cluster_size"]])

  new_ml_bisecting_kmeans(jobj)
}

#' @export
ml_bisecting_kmeans.ml_pipeline <- function(x, formula = NULL, k = 4, max_iter = 20,
                                            seed = NULL, min_divisible_cluster_size = 1,
                                            features_col = "features", prediction_col = "prediction",
                                            uid = random_string("bisecting_bisecting_kmeans_"),
                                            ...) {
  stage <- ml_bisecting_kmeans.spark_connection(
    x = spark_connection(x),
    formula = formula,
    k = k,
    max_iter = max_iter,
    seed = seed,
    min_divisible_cluster_size = min_divisible_cluster_size,
    features_col = features_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_bisecting_kmeans.tbl_spark <- function(x, formula = NULL, k = 4, max_iter = 20,
                                          seed = NULL, min_divisible_cluster_size = 1,
                                          features_col = "features", prediction_col = "prediction",
                                          uid = random_string("bisecting_bisecting_kmeans_"),
                                          features = NULL, ...) {
  formula <- ml_standardize_formula(formula, features = features)

  stage <- ml_bisecting_kmeans.spark_connection(
    x = spark_connection(x),
    formula = formula,
    k = k,
    max_iter = max_iter,
    seed = seed,
    min_divisible_cluster_size = min_divisible_cluster_size,
    features_col = features_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_construct_model_clustering(
      new_ml_model_bisecting_kmeans,
      predictor = stage,
      dataset = x,
      formula = formula,
      features_col = features_col
    )
  }
}

validator_ml_bisecting_kmeans <- function(.args) {
  .args <- validate_args_clustering(.args)
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args[["min_divisible_cluster_size"]] <- cast_scalar_double(.args[["min_divisible_cluster_size"]])
  .args
}

new_ml_bisecting_kmeans <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_bisecting_kmeans")
}

new_ml_bisecting_kmeans_model <- function(jobj) {
  has_summary <- tryCatch(invoke(jobj, "hasSummary"),
    error = function(e) FALSE
  )
  summary <- if (has_summary) {
    new_ml_bisecting_kmeans_summary(invoke(jobj, "summary"))
  }

  new_ml_clustering_model(
    jobj,
    cluster_centers = possibly_null(
      ~ invoke(jobj, "clusterCenters") %>%
        lapply(invoke, "toArray")
    ),
    compute_cost = function(dataset) {
      if (is_required_spark(jobj, "2.4.0")) {
        warning("`compute_cost()` has been deprecated since Spark 2.4.0.", call. = FALSE)
      }
      invoke(jobj, "computeCost", spark_dataframe(dataset))
    },
    summary = summary,
    class = "ml_bisecting_kmeans_model"
  )
}

new_ml_bisecting_kmeans_summary <- function(jobj) {
  bisecting_kmeans_summary <- new_ml_clustering_summary(
    jobj,
    class = "ml_bisecting_kmeans_summary"
  )

  if (is_required_spark(jobj, "3.0.0")) {
    bisecting_kmeans_summary[["training_cost"]] <- invoke(jobj, "trainingCost")
  }

  bisecting_kmeans_summary
}
