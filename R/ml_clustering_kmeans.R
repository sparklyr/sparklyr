#' Spark ML -- K-Means Clustering
#'
#' K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
#'   Using `ml_kmeans()` with the formula interface requires Spark 2.0+.
#'
#' @template roxlate-ml-clustering-algo
#' @template roxlate-ml-clustering-params
#' @template roxlate-ml-tol
#' @template roxlate-ml-prediction-col
#' @template roxlate-ml-formula-params
#' @param init_steps Number of steps for the k-means|| initialization mode. This is an advanced setting -- the default of 2 is almost always enough. Must be > 0. Default: 2.
#' @param init_mode Initialization algorithm. This can be either "random" to choose random points as initial cluster centers, or "k-means||" to use a parallel variant of k-means++ (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
#'
#' @examples
#'\dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#' ml_kmeans(iris_tbl, Species ~ .)
#'}
#'
#' @export
ml_kmeans <- function(x, formula = NULL, k = 2, max_iter = 20, tol = 1e-4,
                      init_steps = 2, init_mode = "k-means||", seed = NULL,
                      features_col = "features", prediction_col = "prediction",
                      uid = random_string("kmeans_"), ...) {
  UseMethod("ml_kmeans")
}

#' @export
ml_kmeans.spark_connection <- function(x, formula = NULL, k = 2, max_iter = 20, tol = 1e-4,
                                       init_steps = 2, init_mode = "k-means||", seed = NULL,
                                       features_col = "features", prediction_col = "prediction",
                                       uid = random_string("kmeans_"), ...) {

  .args <- list(
    k = k,
    max_iter = max_iter,
    tol = tol,
    init_steps = init_steps,
    init_mode = init_mode,
    seed = seed,
    features_col = features_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_kmeans()

  jobj <- ml_new_clustering(
    x, "org.apache.spark.ml.clustering.KMeans", uid,
    .args[["features_col"]], .args[["k"]], .args[["max_iter"]], .args[["seed"]]
  ) %>%
    invoke("setTol", .args[["tol"]]) %>%
    invoke("setInitSteps", .args[["init_steps"]]) %>%
    invoke("setInitMode" , .args[["init_mode"]]) %>%
    invoke("setPredictionCol", .args[["prediction_col"]])

  new_ml_kmeans(jobj)
}

#' @export
ml_kmeans.ml_pipeline <- function(x, formula = NULL, k = 2, max_iter = 20, tol = 1e-4,
                                  init_steps = 2, init_mode = "k-means||", seed = NULL,
                                  features_col = "features", prediction_col = "prediction",
                                  uid = random_string("kmeans_"), ...) {
  stage <- ml_kmeans.spark_connection(
    x = spark_connection(x),
    formula = formula,
    k = k,
    max_iter = max_iter,
    tol = tol,
    init_steps = init_steps,
    init_mode = init_mode,
    seed = seed,
    features_col = features_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_kmeans.tbl_spark <- function(x, formula = NULL, k = 2, max_iter = 20, tol = 1e-4,
                                init_steps = 2, init_mode = "k-means||", seed = NULL,
                                features_col = "features", prediction_col = "prediction",
                                uid = random_string("kmeans_"), features = NULL, ...) {
  ml_formula_transformation()

  stage <- ml_kmeans.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    k = k,
    max_iter = max_iter,
    tol = tol,
    init_steps = init_steps,
    init_mode = init_mode,
    seed = seed,
    features_col = features_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor = stage, formula = formula, features_col = features_col,
      type = "clustering", constructor = new_ml_model_kmeans
    )
  }
}

# Validator
ml_validator_kmeans <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    centers = "k",
    tolerance = "tol",
    iter.max = "max_iter"
  )) %>%
    validate_args_clustering()

  .args[["tol"]] <- cast_scalar_double(.args[["tol"]])
  .args[["init_steps"]] <- cast_scalar_integer(.args[["init_steps"]])
  .args[["init_mode"]] <- cast_choice(.args[["init_mode"]], c("random", "k-means||"))
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args
}

new_ml_kmeans <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_kmeans")
}

new_ml_kmeans_model <- function(jobj) {
  summary <- try_null(new_ml_summary_kmeans_model(invoke(jobj, "summary")))
  new_ml_clustering_model(
    jobj,
    # `def clusterCenters`
    cluster_centers = function() try_null(invoke(jobj, "clusterCenters")) %>%
      purrr::map(invoke, "toArray"),
    compute_cost = function(dataset) {
      invoke(jobj, "computeCost", spark_dataframe(dataset))
    },
    summary = summary,
    subclass = "ml_kmeans_model")
}

new_ml_summary_kmeans_model <- function(jobj) {
  new_ml_summary_clustering(
    jobj,
    subclass = "ml_summary_kmeans")
}
