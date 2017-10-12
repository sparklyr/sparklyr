#' Spark ML -- K-Means Clustering
#'
#' K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
#'
#' @export
ml_kmeans <- function(
  x,
  k = 2L,
  max_iter = 20L,
  tol = 1e-4,
  init_steps = 2L,
  init_mode = "k-means||",
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("kmeans_"), ...
) {
  UseMethod("ml_kmeans")
}

#' @export
ml_kmeans.spark_connection <- function(
  x,
  k = 2L,
  max_iter = 20L,
  tol = 1e-4,
  init_steps = 2L,
  init_mode = "k-means||",
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("kmeans_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_clustering(x, "org.apache.spark.ml.clustering.KMeans", uid,
                            features_col, k, max_iter, seed) %>%
    invoke("setTol", tol) %>%
    invoke("setInitSteps", init_steps) %>%
    invoke("setInitMode" , init_mode) %>%
    invoke("setPredictionCol", prediction_col)

  new_ml_kmeans(jobj)
}

#' @export
ml_kmeans.ml_pipeline <- function(
  x,
  k = 2L,
  max_iter = 20L,
  tol = 1e-4,
  init_steps = 2L,
  init_mode = "k-means||",
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("kmeans_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_kmeans.tbl_spark <- function(
  x,
  formula = NULL,
  k = 2L,
  max_iter = 20L,
  tol = 1e-4,
  init_steps = 2L,
  init_mode = "k-means||",
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  uid = random_string("kmeans_"),
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor = predictor, formula = formula, features_col = features_col,
                         type = "clustering", constructor = new_ml_model_kmeans)
  }
}

# Validator
ml_validator_kmeans <- function(args, nms) {
  old_new_mapping <- c(
    ml_tree_param_mapping(),
    list(
      centers = "k",
      tolerance = "tol",
      iter.max = "max_iter"
    ))

  args %>%
    ml_validate_args({
      tol <- ensure_scalar_double(tol)
      init_steps <- ensure_scalar_integer(init_steps)
      init_mode <- rlang::arg_match(init_mode, c("random", "k-means||"))
      prediction_col <- ensure_scalar_character(prediction_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_kmeans <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_kmeans")
}

new_ml_kmeans_model <- function(jobj) {

  summary <- if (invoke(jobj, "hasSummary"))
    new_ml_summary_kmeans_model(invoke(jobj, "summary"))
  else NULL

  new_ml_clustering_model(
    jobj,
    cluster_centers = try_null(invoke(jobj, "clusterCenters")),
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

new_ml_model_kmeans <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  call) {

  summary <- model$summary

  centers <- model$cluster_centers %>%
    sapply(invoke, "toArray") %>%
    t() %>%
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
    subclass = "ml_model_kmeans",
    .features = feature_names,
    .call = call
  )
}

# Generic implementations

#' @export
ml_fit.ml_kmeans <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_kmeans_model(jobj)
}

#' @export
print.ml_model_kmeans <- function(x, ...) {

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
