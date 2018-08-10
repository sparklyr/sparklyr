#' Spark ML -- LinearSVC
#'
#' Perform classification using linear support vector machines (SVM). This binary classifier optimizes the Hinge Loss using the OWLQN optimizer. Only supports L2 regularization currently.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-aggregation-depth
#' @template roxlate-ml-standardization
#' @param threshold in binary classification prediction, in range [0, 1].
#' @param raw_prediction_col Raw prediction (a.k.a. confidence) column name.
#' @export
ml_linear_svc <- function(x, formula = NULL, fit_intercept = TRUE, reg_param = 0,
                          max_iter = 100, standardization = TRUE, weight_col = NULL,
                          tol = 1e-6, threshold = 0, aggregation_depth = 2,
                          features_col = "features", label_col = "label",
                          prediction_col = "prediction", raw_prediction_col = "rawPrediction",
                          uid = random_string("linear_svc_"), ...) {
  UseMethod("ml_linear_svc")
}

#' @export
ml_linear_svc.spark_connection <- function(x, formula = NULL, fit_intercept = TRUE, reg_param = 0,
                                           max_iter = 100, standardization = TRUE, weight_col = NULL,
                                           tol = 1e-6, threshold = 0, aggregation_depth = 2,
                                           features_col = "features", label_col = "label",
                                           prediction_col = "prediction", raw_prediction_col = "rawPrediction",
                                           uid = random_string("linear_svc_"), ...) {

  .args <- list(
    fit_intercept = fit_intercept,
    reg_param = reg_param,
    max_iter = max_iter,
    standardization = standardization,
    weight_col = weight_col,
    tol = tol,
    threshold = threshold,
    aggregation_depth = aggregation_depth,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    raw_prediction_col = raw_prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_linear_svc()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.classification.LinearSVC", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]
  ) %>%
    invoke("setRawPredictionCol", .args[["raw_prediction_col"]]) %>%
    invoke("setFitIntercept", .args[["fit_intercept"]]) %>%
    invoke("setRegParam", .args[["reg_param"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    invoke("setStandardization", .args[["standardization"]]) %>%
    invoke("setTol", .args[["tol"]]) %>%
    invoke("setAggregationDepth", .args[["aggregation_depth"]]) %>%
    invoke("setThreshold", .args[["threshold"]]) %>%
    maybe_set_param("setWeightCol", .args[["weight_col"]])

  new_ml_linear_svc(jobj)
}

#' @export
ml_linear_svc.ml_pipeline <- function(x, formula = NULL, fit_intercept = TRUE, reg_param = 0,
                                      max_iter = 100, standardization = TRUE, weight_col = NULL,
                                      tol = 1e-6, threshold = 0, aggregation_depth = 2,
                                      features_col = "features", label_col = "label",
                                      prediction_col = "prediction", raw_prediction_col = "rawPrediction",
                                      uid = random_string("linear_svc_"), ...) {

  stage <- ml_linear_svc.spark_connection(
    x = spark_connection(x),
    formula = formula,
    fit_intercept = fit_intercept,
    reg_param = reg_param,
    max_iter = max_iter,
    standardization = standardization,
    weight_col = weight_col,
    tol = tol,
    threshold = threshold,
    aggregation_depth = aggregation_depth,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_linear_svc.tbl_spark <- function(x, formula = NULL, fit_intercept = TRUE, reg_param = 0,
                                    max_iter = 100, standardization = TRUE, weight_col = NULL,
                                    tol = 1e-6, threshold = 0, aggregation_depth = 2,
                                    features_col = "features", label_col = "label",
                                    prediction_col = "prediction", raw_prediction_col = "rawPrediction",
                                    uid = random_string("linear_svc_"), response = NULL,
                                    features = NULL, predicted_label_col = "predicted_label", ...) {
  ml_formula_transformation()

  stage <- ml_linear_svc.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    fit_intercept = fit_intercept,
    reg_param = reg_param,
    max_iter = max_iter,
    standardization = standardization,
    weight_col = weight_col,
    tol = tol,
    threshold = threshold,
    aggregation_depth = aggregation_depth,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, stage, formula, features_col, label_col,
      "classification", new_ml_model_linear_svc, predicted_label_col
    )
  }
}

# Validator
ml_validator_linear_svc <- function(.args) {
  .args[["reg_param"]] <- cast_scalar_double(.args[["reg_param"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["fit_intercept"]] <- cast_scalar_logical(.args[["fit_intercept"]])
  .args[["standardization"]] <- cast_scalar_logical(.args[["standardization"]])
  .args[["tol"]] <- cast_scalar_double(.args[["tol"]])
  .args[["aggregation_depth"]] <- cast_scalar_integer(.args[["aggregation_depth"]])
  .args[["raw_prediction_col"]] <- cast_string(.args[["raw_prediction_col"]])
  .args[["threshold"]] <- cast_scalar_double(.args[["threshold"]])
  .args[["weight_col"]] <- cast_nullable_string(.args[["weight_col"]])
  .args
}

# Constructors

new_ml_linear_svc <- function(jobj) {
  new_ml_classifier(jobj, subclass = "ml_linear_svc")
}

new_ml_linear_svc_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    num_classes = invoke(jobj, "numClasses"),
    num_features = invoke(jobj, "numFeatures"),
    threshold = invoke(jobj, "threshold"),
    weight_col = try_null(invoke(jobj, "weightCol")),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    subclass = "ml_linear_svc_model")
}
