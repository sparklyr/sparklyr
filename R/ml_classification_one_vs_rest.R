#' Spark ML -- OneVsRest
#'
#' Reduction of Multiclass Classification to Binary Classification. Performs reduction using one against all strategy. For a multiclass classification with k classes, train k models (one per class). Each example is scored against all k models and the model with highest score is picked to label the example.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @param classifier Object of class \code{ml_estimator}. Base binary classifier that we reduce multiclass classification into.
#' @export
ml_one_vs_rest <- function(
  x,
  formula = NULL,
  classifier,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("one_vs_rest_"),
  ...
) {
  UseMethod("ml_one_vs_rest")
}

#' @export
ml_one_vs_rest.spark_connection <- function(
  x,
  formula = NULL,
  classifier,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("one_vs_rest_"),
  ...) {

  ml_ratify_args()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.classification.OneVsRest", uid,
    features_col = features_col, label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    invoke("setClassifier", spark_jobj(classifier))

  new_ml_one_vs_rest(jobj)
}

#' @export
ml_one_vs_rest.ml_pipeline <- function(
  x,
  formula = NULL,
  classifier,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("one_vs_rest_"),
  ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_one_vs_rest.tbl_spark <- function(
  x,
  formula = NULL,
  classifier,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("one_vs_rest_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label", ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor, formula, features_col, label_col,
                         "classification",
                         new_ml_model_one_vs_rest,
                         predicted_label_col)
  }
}

# Validator

ml_validator_one_vs_rest <- function(args, nms) {
  args %>%
    ml_validate_args({
      classifier <- if (inherits(classifier, "ml_predictor")) classifier else
        stop("classifier must be a ml_predictor")
    }) %>%
    ml_extract_args(nms)
}

# Constructors

new_ml_one_vs_rest <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_one_vs_rest")
}

new_ml_one_vs_rest_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    models = invoke(jobj, "models") %>%
      lapply(ml_constructor_dispatch),
    subclass = "ml_one_vs_rest_model")
}

new_ml_model_one_vs_rest <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, index_labels,
  call) {

  new_ml_model_classification(
    pipeline, pipeline_model,
    model, dataset, formula,
    subclass = "ml_model_one_vs_rest",
    .features = feature_names,
    .index_labels = index_labels
  )
}
