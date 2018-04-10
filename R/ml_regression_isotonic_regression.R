#' Spark ML -- Isotonic Regression
#'
#' Currently implemented using parallelized pool adjacent violators algorithm. Only univariate (single feature) algorithm supported.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @param feature_index Index of the feature if \code{features_col} is a vector column (default: 0), no effect otherwise.
#' @param isotonic Whether the output sequence should be isotonic/increasing (true) or antitonic/decreasing (false). Default: true
#' @template roxlate-ml-weight-col
#' @export
ml_isotonic_regression <- function(
  x,
  formula = NULL,
  feature_index = 0L,
  isotonic = TRUE,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("isotonic_regression_"), ...
) {
  UseMethod("ml_isotonic_regression")
}

#' @export
ml_isotonic_regression.spark_connection <- function(
  x,
  formula = NULL,
  feature_index = 0L,
  isotonic = TRUE,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("isotonic_regression_"), ...) {

  ml_ratify_args()

  class <- "org.apache.spark.ml.regression.IsotonicRegression"

  jobj <- ml_new_predictor(x, class, uid, features_col,
                           label_col, prediction_col) %>%
    invoke("setFeatureIndex", feature_index) %>%
    invoke("setIsotonic", isotonic)

  if (!rlang::is_null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_isotonic_regression(jobj)
}

#' @export
ml_isotonic_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  feature_index = 0L,
  isotonic = TRUE,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("isotonic_regression_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_isotonic_regression.tbl_spark <- function(
  x,
  formula = NULL,
  feature_index = 0L,
  isotonic = TRUE,
  weight_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("isotonic_regression_"),
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor, formula, features_col, label_col,
                         "regression", new_ml_model_isotonic_regression)
  }
}

# Validator
ml_validator_isotonic_regression <- function(args, nms) {
  args %>%
    ml_validate_args({
      feature_index <- ensure_scalar_integer(feature_index)
      isotonic <- ensure_scalar_boolean(isotonic)
      if (!rlang::is_null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
    }) %>%
    ml_extract_args(nms)
}

# Constructors

new_ml_isotonic_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_isotonic_regression")
}

new_ml_isotonic_regression_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    boundaries = read_spark_vector(jobj, "boundaries"),
    predictions = read_spark_vector(jobj, "predictions"),
    feature_index = invoke(jobj, "getFeatureIndex"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    subclass = "ml_isotonic_regression_model")
}

new_ml_model_isotonic_regression <- function(
  pipeline, pipeline_model, model, dataset, formula,
  feature_names, call) {
  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_isotonic_regression",
    .features = feature_names
  )
}
