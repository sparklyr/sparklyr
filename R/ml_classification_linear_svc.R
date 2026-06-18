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
ml_linear_svc <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  reg_param = 0,
  max_iter = 100,
  standardization = TRUE,
  weight_col = NULL,
  tol = 1e-6,
  threshold = 0,
  aggregation_depth = 2,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  raw_prediction_col = "rawPrediction",
  uid = random_string("linear_svc_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_linear_svc")
}

ml_linear_svc_impl <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  reg_param = 0,
  max_iter = 100,
  standardization = TRUE,
  weight_col = NULL,
  tol = 1e-6,
  threshold = 0,
  aggregation_depth = 2,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  raw_prediction_col = "rawPrediction",
  uid = random_string("linear_svc_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label",
  ...
) {
  sc <- spark_connection(x)
  if (spark_version(sc) >= "3.0" && !is.null(weight_col)) {
    warning(
      "Support for `weight_col` is removed in Spark 3.0 or above because",
      "it is not intended for users (see ",
      "https://spark.apache.org/docs/latest/ml-migration-guide.html#breaking-changes",
      "). The `weight_col` parameter will be ignored."
    )
    weight_col <- NULL
  }

  ml_process_model(
    x = x,
    r_class = "ml_linear_svc",
    ml_function = new_ml_model_linear_svc,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    constructor_args = list(predicted_label_col = predicted_label_col),
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      raw_prediction_col = raw_prediction_col,
      fit_intercept = fit_intercept,
      reg_param = reg_param,
      max_iter = max_iter,
      standardization = standardization,
      tol = tol,
      aggregation_depth = aggregation_depth,
      threshold = threshold,
      weight_col = weight_col
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_linear_svc.spark_connection <- ml_linear_svc_impl

#' @export
ml_linear_svc.ml_pipeline <- ml_linear_svc_impl

#' @export
ml_linear_svc.tbl_spark <- ml_linear_svc_impl

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
