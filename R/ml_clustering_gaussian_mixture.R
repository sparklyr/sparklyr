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
#'
#'@examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' gmm_model <- ml_gaussian_mixture(iris_tbl, Species ~ .)
#' pred <- sdf_predict(iris_tbl, gmm_model)
#' ml_clustering_evaluator(pred)
#' }


#' @export
ml_gaussian_mixture <- function(x, formula = NULL, k = 2, max_iter = 100,
                                tol = 0.01, seed = NULL, features_col = "features",
                                prediction_col = "prediction", probability_col = "probability",
                                uid = random_string("gaussian_mixture_"), ...) {

  UseMethod("ml_gaussian_mixture")
}

#' @export
ml_gaussian_mixture.spark_connection <- function(x, formula = NULL, k = 2, max_iter = 100,
                                                 tol = 0.01, seed = NULL, features_col = "features",
                                                 prediction_col = "prediction", probability_col = "probability",
                                                 uid = random_string("gaussian_mixture_"), ...) {
  spark_require_version(spark_connection(x), "2.0.0", "GaussianMixture")

  .args <- list(
    k = k,
    max_iter = max_iter,
    tol = tol,
    seed = seed,
    features_col = features_col,
    prediction_col = prediction_col,
    probability_col = probability_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_gaussian_mixture()

  jobj <- ml_new_clustering(
    x, "org.apache.spark.ml.clustering.GaussianMixture", uid,
    .args[["features_col"]], .args[["k"]], .args[["max_iter"]], .args[["seed"]]
  ) %>%
    invoke("setTol", .args[["tol"]]) %>%
    invoke("setPredictionCol", .args[["prediction_col"]]) %>%
    invoke("setProbabilityCol", .args[["probability_col"]])

  new_ml_gaussian_mixture(jobj)
}

#' @export
ml_gaussian_mixture.ml_pipeline <- function(x, formula = NULL, k = 2, max_iter = 100,
                                            tol = 0.01, seed = NULL, features_col = "features",
                                            prediction_col = "prediction", probability_col = "probability",
                                            uid = random_string("gaussian_mixture_"), ...) {

  stage <- ml_gaussian_mixture.spark_connection(
    x = spark_connection(x),
    formula = formula,
    k = k,
    max_iter = max_iter,
    tol = tol,
    seed = seed,
    features_col = features_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_gaussian_mixture.tbl_spark <- function(x, formula = NULL, k = 2, max_iter = 100,
                                          tol = 0.01, seed = NULL, features_col = "features",
                                          prediction_col = "prediction", probability_col = "probability",
                                          uid = random_string("gaussian_mixture_"), features = NULL, ...) {
  ml_formula_transformation()

  stage <- ml_gaussian_mixture.spark_connection(
    x = spark_connection(x),
    formula = formula,
    k = k,
    max_iter = max_iter,
    tol = tol,
    seed = seed,
    features_col = features_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor = stage, formula = formula, features_col = features_col,
                         type = "clustering", constructor = new_ml_model_gaussian_mixture)
  }
}

ml_validator_gaussian_mixture <- function(.args) {
  .args <- validate_args_clustering(.args)
  .args[["tol"]] <- cast_scalar_double(.args[["tol"]])
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args[["probability_col"]] <- cast_string(.args[["probability_col"]])
  .args
}

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
    gaussians_df = function() invoke(jobj, "gaussiansDF") %>% # def
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
