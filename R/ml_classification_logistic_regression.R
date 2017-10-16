#' Spark ML -- Logistic Regression
#'
#' Perform classification using logistic regression.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-probabilistic-classifier-params
#' @param family Param for the name of family which is a description of the label distribution to be used in the model. Supported options: "auto", "binomial", and "multinomial."
#' @template roxlate-ml-elastic-net-param
#' @param threshold in binary classification prediction, in range [0, 1].
#' @template roxlate-ml-aggregation-depth
#' @export
ml_logistic_regression <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  ...
) {
  UseMethod("ml_logistic_regression")
}

#' @export
ml_logistic_regression.spark_connection <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  ...) {

  ml_ratify_args()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.LogisticRegression", uid,
    features_col = features_col, label_col = label_col,
    prediction_col = prediction_col, probability_col = probability_col,
    raw_prediction_col = raw_prediction_col
  ) %>%
    invoke("setFamily", family) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setElasticNetParam", elastic_net_param) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setThreshold", threshold) %>%
    invoke("setAggregationDepth", aggregation_depth) %>%
    invoke("setTol", tol)

  if (!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_logistic_regression(jobj)
}

#' @export
ml_logistic_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_logistic_regression.tbl_spark <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor, formula, features_col, label_col,
                         "classification", new_ml_model_logistic_regression)
  }
}

# Validator

ml_validator_logistic_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    alpha = "elastic_net_param",
    lambda = "reg_param",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  args %>%
    ml_validate_args({
      elastic_net_param <- ensure_scalar_double(elastic_net_param)
      reg_param <- ensure_scalar_double(reg_param)
      max_iter <- ensure_scalar_integer(max_iter)
      family <- rlang::arg_match(family, c("auto", "binomial", "multinomial"))
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      threshold <- ensure_scalar_double(threshold)
      if (!is.null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
      aggregation_depth <- ensure_scalar_integer(aggregation_depth)
      tol <- ensure_scalar_double(tol)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_logistic_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_logistic_regression")
}

new_ml_logistic_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary"))
    new_ml_summary_logistic_regression_model(invoke(jobj, "summary"))
  else NA

  is_multinomial <- invoke(jobj, "numClasses") > 2

  new_ml_prediction_model(
    jobj,
    coefficients = if (is_multinomial) NA else read_spark_vector(jobj, "coefficients"),
    coefficient_matrix = read_spark_matrix(jobj, "coefficientMatrix"),
    intercept = if (is_multinomial) NA else invoke(jobj, "intercept"),
    intercept_vector = read_spark_vector(jobj, "interceptVector"),
    num_classes = invoke(jobj, "numClasses"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = invoke(jobj, "getProbabilityCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    threshold = if (ml_is_set(jobj, "threshold")) invoke(jobj, "getThreshold") else NA,
    thresholds = if (ml_is_set(jobj, "thresholds")) invoke(jobj, "getThresholds") else NA,
    summary = summary,
    subclass = "ml_logistic_regression_model")
}

new_ml_summary_logistic_regression_model <- function(jobj) {
  new_ml_summary(
    jobj,
    area_under_roc = invoke(jobj, "areaUnderROC"),
    f_measure_by_threshold = invoke(jobj, "fMeasureByThreshold") %>% collect(),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    objective_history = invoke(jobj, "objectiveHistory"),
    pr = invoke(jobj, "pr") %>% collect(),
    precision_by_threshold = invoke(jobj, "precisionByThreshold") %>% collect(),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    probability_col = invoke(jobj, "probabilityCol"),
    recall_by_threshold = invoke(jobj, "recallByThreshold") %>% collect(),
    roc = invoke(jobj, "roc") %>% collect(),
    total_iterations = invoke(jobj, "totalIterations"),
    subclass = "ml_summary_logistic_regression")
}

new_ml_model_logistic_regression <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, index_labels,
  call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  # multinomial vs. binomial models have separate APIs for
  # retrieving results
  is_multinomial <- invoke(jobj, "numClasses") > 2

  # extract coefficients (can be either a vector or matrix, depending
  # on binomial vs. multinomial)
  coefficients <- if (is_multinomial) {
    if (spark_version(sc) < "2.1.0") stop("Multinomial regression requires Spark 2.1.0 or higher.")

    # multinomial
    coefficients <- model$coefficient_matrix
    colnames(coefficients) <- feature_names

    if (ml_param(model, "fit_intercept")) {
      intercept <- model$intercept
      coefficients <- cbind(intercept, coefficients)
      colnames(coefficients) <- c("(Intercept)", feature_names)
    }
    coefficients
  } else {
    # binomial

    coefficients <- if (ml_param(model, "fit_intercept"))
      rlang::set_names(
        c(invoke(jobj, "intercept"), model$coefficients),
        c("(Intercept)", feature_names)
      )
    else
      rlang::set_names(coefficients, feature_names)
    coefficients
  }

  summary <- model$summary

  new_ml_model_classification(
    pipeline, pipeline_model,
    model, dataset, formula,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_logistic_regression",
    .features = feature_names,
    .index_labels = index_labels,
    .call = call
  )
}


# Generic implementations

#' @export
ml_fit.ml_logistic_regression <- function(x, dataset, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(dataset))
  new_ml_logistic_regression_model(jobj)
}

#' @export
print.ml_model_logistic_regression <- function(x, ...) {
  ml_model_print_call(x)
  print_newline()
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
print.ml_summary_logistic_regression <- function(x, ...) {
  cat(ml_short_type(x), "\n")
  out_list <- list(
    area_under_roc = x$area_under_roc,
    features_col = x$features_col,
    label_col = x$label_col,
    probability_col = x$probability_col
  )
  for (item in names(out_list))
    cat("  ", item, ":", capture.output(str(out_list[[item]])), "\n")
}

#' @export
print.ml_logistic_regression_model <- function(x, ...) {
  cat(ml_short_type(x), "(Transformer) \n")
  cat(paste0("<", ml_uid(x), ">"),"\n")
  item_names <- names(x) %>%
    setdiff(c("uid", "type", "param_map", "summary", ".jobj")) %>%
    setdiff(
      if (x$num_classes > 2)
        c("coefficients", "intercept")
      else
        c("coefficient_matrix", "intercept_vector")
    )
  for (item in item_names)
    if (!rlang::is_na(x[[item]]))
      cat("  ", item, ":", capture.output(str(x[[item]])), "\n")
}

#' @export
summary.ml_model_logistic_regression <- function(object, ...) {
  ml_model_print_call(object)
  print_newline()
  ml_model_print_coefficients(object)
  print_newline()
}

#' @export
predict.ml_model_classification <- function(object,
                             newdata = ml_model_data(object),
                             ...)
{
  object$pipeline_model %>%
    ml_transform(newdata) %>%
    ft_index_to_string("prediction", "prediction_labels",
                       labels = object$.index_labels) %>%
    sdf_read_column("prediction_labels")
}
