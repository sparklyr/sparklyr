ml_new_classifier <- function(sc, class,
                              uid = uid,
                              label_col,
                              prediction_col,
                              probability_col,
                              raw_prediction_col, ...) {
  ensure_scalar_character(label_col)
  ensure_scalar_character(prediction_col)
  ensure_scalar_character(probability_col)
  ensure_scalar_character(raw_prediction_col)
  invoke_new(sc, class, uid) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setProbabilityCol", probability_col) %>%
    invoke("setRawPredictionCol", raw_prediction_col)
}

#' @export
ml_logistic_regression <- function(x, ...) {
  UseMethod("ml_logistic_regression")
}

#' @export
ml_logistic_regression.spark_connection <- function(
  x, intercept = TRUE, alpha = 0,
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction", uid = random_string("logistic_regression_"), ...) {

  alpha <- ensure_scalar_double(alpha)
  intercept <- ensure_scalar_boolean(intercept)

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.LogisticRegression", uid,
    label_col, prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setElasticNetParam", alpha) %>%
    invoke("setFitIntercept", intercept)

  ml_pipeline_stage_info(jobj)
}

#' @export
ml_logistic_regression.ml_pipeline <- function(
  x, intercept = TRUE, alpha = 0,
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction", uid = random_string("logistic_regression_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}
