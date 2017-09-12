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
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
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
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
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

  ml_pipeline_stage_info(jobj)
}

#' @export
ml_logistic_regression.ml_pipeline <- function(
  x,
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
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
  features_col = "features",
  label_col = "label",
  family = c("auto", "binomial", "multinomial"),
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  # ml_transform(transformer, x)
  jobj <- transformer$.jobj %>%
    invoke("fit", spark_dataframe(x))

  structure(
    list(
      .jobj = jobj
    ),
    class = c("ml_logistic_regression_model", "ml_pipeline_stage")
  )
}
