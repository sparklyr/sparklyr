#' Spark ML -- Gaussian Mixture clustering.
#'
#' This class performs expectation maximization for multivariate Gaussian Mixture Models (GMMs). A GMM represents a composite distribution of independent Gaussian distributions with associated "mixing" weights specifying each's contribution to the composite. Given a set of sample points, this class will maximize the log-likelihood for a mixture of k Gaussians, iterating until the log-likelihood changes by less than \code{tol}, or until it has reached the max number of iterations. While this process is generally guaranteed to converge, it is not guaranteed to find a global optimum.
#'
#' @template roxlate-ml-clustering-algo
#' @template roxlate-ml-clustering-params
#' @template roxlate-ml-tol
#' @template roxlate-ml-prediction-col
#' @template roxlate-ml-formula-params
#' @param probability_col Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities.
#' @export
ml_gaussian_mixture <- function(
  x,
  formula = NULL,
  k = 2L,
  max_iter = 100L,
  tol = 0.01,
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  probability_col = "probability",
  uid = random_string("gaussian_mixture_"), ...
) {
  if (spark_version(spark_connection(x)) < "2.0.0")
    stop("Gaussian mixture requires Spark 2.0.0 or higher")
  UseMethod("ml_gaussian_mixture")
}

#' @export
ml_gaussian_mixture.spark_connection <- function(
  x,
  formula = NULL,
  k = 2L,
  max_iter = 100L,
  tol = 0.01,
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  probability_col = "probability",
  uid = random_string("gaussian_mixture_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_clustering(x, "org.apache.spark.ml.clustering.GaussianMixture", uid,
                            features_col, k, max_iter, seed) %>%
    invoke("setTol", tol) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setProbabilityCol", probability_col)

  new_ml_gaussian_mixture(jobj)
}

#' @export
ml_gaussian_mixture.ml_pipeline <- function(
  x,
  formula = NULL,
  k = 2L,
  max_iter = 100L,
  tol = 0.01,
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  probability_col = "probability",
  uid = random_string("gaussian_mixture_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_gaussian_mixture.tbl_spark <- function(
  x,
  formula = NULL,
  k = 2L,
  max_iter = 100L,
  tol = 0.01,
  seed = NULL,
  features_col = "features",
  prediction_col = "prediction",
  probability_col = "probability",
  uid = random_string("gaussian_mixture_"),
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor = predictor, formula = formula, features_col = features_col,
                         type = "clustering", constructor = new_ml_model_gaussian_mixture)
  }
}

# Validator
ml_validator_gaussian_mixture <- function(args, nms) {
  args %>%
    ml_validate_args({
      tol <- ensure_scalar_double(tol)
      prediction_col <- ensure_scalar_character(prediction_col)
      probability_col <- ensure_scalar_character(probability_col)
    }) %>%
    ml_extract_args(nms)
}

# Constructors

new_ml_gaussian_mixture <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_gaussian_mixture")
}

new_ml_gaussian_mixture_model <- function(jobj) {

  summary <- if (invoke(jobj, "hasSummary"))
    new_ml_summary_gaussian_mixture_model(invoke(jobj, "summary"))
  else NULL

  new_ml_clustering_model(
    jobj,
    gaussians = invoke(jobj, "gaussians"),
    gaussians_df = invoke(jobj, "gaussiansDF") %>%
      sdf_register() %>%
      collect() %>%
      dplyr::mutate(!!rlang::sym("cov") := lapply(!!rlang::sym("cov"), read_spark_matrix)),
    weights = invoke(jobj, "weights"),
    summary = summary,
    subclass = "ml_gaussian_mixture_model")
}

new_ml_summary_gaussian_mixture_model <- function(jobj) {
  new_ml_summary_clustering(
    jobj,
    log_likelihood = invoke(jobj, "logLikelihood"),
    probability = invoke(jobj, "probability") %>% sdf_register(),
    probability_col = invoke(jobj, "probabilityCol"),
    subclass = "ml_summary_gaussian_mixture")
}

new_ml_model_gaussian_mixture <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  call) {

  summary <- model$summary

  new_ml_model_clustering(
    pipeline, pipeline_model,
    model, dataset, formula,
    weights = model$weights,
    gaussians_df = model$gaussians_df,
    summary = summary,
    subclass = "ml_model_gaussian_mixture",
    .features = feature_names,
    .call = call
  )
}
