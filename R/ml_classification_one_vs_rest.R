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
  check_dots_used()
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
    validator_ml_one_vs_rest()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.classification.OneVsRest", uid,
    features_col = .args[["features_col"]], label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  ) %>%
    jobj_set_param(
      "setClassifier",
      possibly_null(spark_jobj)(.args[["classifier"]])
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
  formula <- ml_standardize_formula(formula, response, features)

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
    ml_construct_model_supervised(
      new_ml_model_one_vs_rest,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col,
      predicted_label_col = predicted_label_col
    )
  }
}

validator_ml_one_vs_rest <- function(.args) {
  .args <- validate_args_predictor(.args)
  .args[["classifier"]] <- if (inherits(.args[["classifier"]], "spark_jobj")) {
    ml_call_constructor(.args[["classifier"]])
  } else {
    .args[["classifier"]]
  }
  if (!is.null(.args[["classifier"]]) && !inherits(.args[["classifier"]], "ml_classifier")) {
    stop("`classifier` must be an `ml_classifier`.", call. = FALSE)
  }
  .args
}

new_ml_one_vs_rest <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_one_vs_rest")
}

new_ml_one_vs_rest_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    models = invoke(jobj, "models") %>%
      purrr::map(ml_call_constructor),
    class = "ml_one_vs_rest_model"
  )
}
