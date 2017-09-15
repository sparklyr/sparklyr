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
ml_logistic_regression <- function(
  x,
  formula = NULL,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...
) {
  UseMethod("ml_logistic_regression")
}

#' @export
ml_logistic_regression.spark_connection <- function(
  x,
  formula = NULL,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  ml_validate_args(rlang::caller_env())

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.LogisticRegression", uid,
    label_col, prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setElasticNetParam", elastic_net_param) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter)

  if (!is.null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_predictor(jobj)
}

#' @export
ml_logistic_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_logistic_regression.tbl_spark <- function(
  x,
  formula = NULL,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  weight_col = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  logistic_regression <- ml_new_stage_modified_args(rlang::call_frame())

  if (is.null(formula)) {
    logistic_regression %>%
      ml_fit(x)
  } else {
    sc <- spark_connection(x)
    r_formula <- ml_r_formula(sc, formula, features_col,
                              label_col, force_index_label = TRUE,
                              dataset = x)
    pipeline <- ml_pipeline(r_formula, logistic_regression)

    pipeline_model <- pipeline %>%
      ml_fit(x)

    new_ml_model_logistic_regression(
      pipeline,
      pipeline_model,
      logistic_regression$uid,
      formula,
      dataset = x)
  }
}
