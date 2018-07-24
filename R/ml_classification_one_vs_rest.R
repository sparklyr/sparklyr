#' Spark ML -- OneVsRest
#'
#' Reduction of Multiclass Classification to Binary Classification. Performs reduction using one against all strategy. For a multiclass classification with k classes, train k models (one per class). Each example is scored against all k models and the model with highest score is picked to label the example.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @param classifier Object of class \code{ml_estimator}. Base binary classifier that we reduce multiclass classification into.
#' @export
ml_one_vs_rest <- function(x, formula = NULL, classifier = NULL, features_col = "features",
                           label_col = "label", prediction_col = "prediction",
                           uid = random_string("one_vs_rest_"), ...) {
  UseMethod("ml_one_vs_rest")
}

#' @export
ml_one_vs_rest.spark_connection <- function(x, formula = NULL, classifier = NULL, features_col = "features",
                                            label_col = "label", prediction_col = "prediction",
                                            uid = random_string("one_vs_rest_"), ...) {
  .args <- list(
    classifier = classifier,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_one_vs_rest()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.classification.OneVsRest", uid,
    features_col = .args[["features_col"]], label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  ) %>%
    maybe_set_param(
      "setClassifier",
      purrr::possibly(spark_jobj, NULL)(.args[["classifier"]])
    )

  new_ml_one_vs_rest(jobj)
}

#' @export
ml_one_vs_rest.ml_pipeline <- function(x, formula = NULL, classifier = NULL, features_col = "features",
                                       label_col = "label", prediction_col = "prediction",
                                       uid = random_string("one_vs_rest_"), ...) {
  stage <- ml_one_vs_rest.spark_connection(
    x = spark_connection(x),
    formula = formula,
    classifier = classifier,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_one_vs_rest.tbl_spark <- function(x, formula = NULL, classifier = NULL, features_col = "features",
                                     label_col = "label", prediction_col = "prediction",
                                     uid = random_string("one_vs_rest_"), response = NULL,
                                     features = NULL, predicted_label_col = "predicted_label", ...) {
  ml_formula_transformation()

  stage <- ml_one_vs_rest.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    classifier = classifier,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, stage, formula, features_col, label_col,
                         "classification",
                         new_ml_model_one_vs_rest,
                         predicted_label_col)
  }
}

ml_validator_one_vs_rest <- function(.args) {
  .args <- validate_args_predictor(.args)
  .args[["classifier"]] <- if (inherits(.args[["classifier"]], "spark_jobj"))
    ml_constructor_dispatch(.args[["classifier"]])
  else
    .args[["classifier"]]
  if (!is.null(.args[["classifier"]]) && !inherits(.args[["classifier"]], "ml_classifier"))
    stop("`classifier` must be an `ml_classifier`.", call. = FALSE)
  .args
}

new_ml_one_vs_rest <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_one_vs_rest")
}

new_ml_one_vs_rest_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    models = invoke(jobj, "models") %>%
      purrr::map(ml_constructor_dispatch),
    subclass = "ml_one_vs_rest_model")
}
