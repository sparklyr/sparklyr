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
#' @examples
#' \dontrun{
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   filter(Species != "setosa") %>%
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' svc_model <- iris_training %>%
#'   ml_linear_svc(Species ~ .)
#'
#' pred <- ml_predict(svc_model, iris_test)
#'
#' ml_binary_classification_evaluator(pred)
#' }
#'
#' @export
ml_linear_svc <- function(x, formula = NULL, fit_intercept = TRUE, reg_param = 0,
                          max_iter = 100, standardization = TRUE, weight_col = NULL,
                          tol = 1e-6, threshold = 0, aggregation_depth = 2,
                          features_col = "features", label_col = "label",
                          prediction_col = "prediction", raw_prediction_col = "rawPrediction",
                          uid = random_string("linear_svc_"), ...) {
  check_dots_used()
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
    validator_ml_linear_svc()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.classification.LinearSVC", uid,
    features_col = .args[["features_col"]], label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  ) %>% (
    function(obj) {
      do.call(
        invoke,
        c(obj, "%>%", Filter(
          function(x) !is.null(x),
          list(
            list("setRawPredictionCol", .args[["raw_prediction_col"]]),
            list("setFitIntercept", .args[["fit_intercept"]]),
            list("setRegParam", .args[["reg_param"]]),
            list("setMaxIter", .args[["max_iter"]]),
            list("setStandardization", .args[["standardization"]]),
            list("setTol", .args[["tol"]]),
            list("setAggregationDepth", .args[["aggregation_depth"]]),
            list("setThreshold", .args[["threshold"]]),
            jobj_set_param_helper(obj, "setWeightCol", .args[["weight_col"]])
          )
        ))
      )
    })

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
  formula <- ml_standardize_formula(formula, response, features)

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
    ml_construct_model_supervised(
      new_ml_model_linear_svc,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col,
      predicted_label_col = predicted_label_col
    )
  }
}

# Validator
validator_ml_linear_svc <- function(.args) {
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
  new_ml_classifier(jobj, class = "ml_linear_svc")
}

new_ml_linear_svc_model <- function(jobj) {
  new_ml_classification_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    threshold = invoke(jobj, "threshold"),
    weight_col = possibly_null(~ invoke(jobj, "weightCol"))(),
    class = "ml_linear_svc_model"
  )
}
