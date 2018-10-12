#' Spark ML -- Isotonic Regression
#'
#' Currently implemented using parallelized pool adjacent violators algorithm. Only univariate (single feature) algorithm supported.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @param feature_index Index of the feature if \code{features_col} is a vector column (default: 0), no effect otherwise.
#' @param isotonic Whether the output sequence should be isotonic/increasing (true) or antitonic/decreasing (false). Default: true
#' @template roxlate-ml-weight-col
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' iso_res <- iris_tbl %>%
#'   ml_isotonic_regression(Petal_Length ~ Petal_Width)
#'
#' pred <- sdf_predict(iris_test, iso_res)
#'
#' pred
#' }
#'
#' @export
ml_isotonic_regression <- function(x, formula = NULL, feature_index = 0, isotonic = TRUE,
                                   weight_col = NULL, features_col = "features",
                                   label_col = "label", prediction_col = "prediction",
                                   uid = random_string("isotonic_regression_"), ...) {
  UseMethod("ml_isotonic_regression")
}

#' @export
ml_isotonic_regression.spark_connection <- function(x, formula = NULL, feature_index = 0, isotonic = TRUE,
                                                    weight_col = NULL, features_col = "features",
                                                    label_col = "label", prediction_col = "prediction",
                                                    uid = random_string("isotonic_regression_"), ...) {

  .args <- list(
    feature_index = feature_index,
    isotonic = isotonic,
    weight_col = weight_col,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_isotonic_regression()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.regression.IsotonicRegression", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]
  ) %>%
    invoke("setFeatureIndex", .args[["feature_index"]]) %>%
    invoke("setIsotonic", .args[["isotonic"]]) %>%
    maybe_set_param("setWeightCol", .args[["weight_col"]])

  new_ml_isotonic_regression(jobj)
}

#' @export
ml_isotonic_regression.ml_pipeline <- function(x, formula = NULL, feature_index = 0, isotonic = TRUE,
                                               weight_col = NULL, features_col = "features",
                                               label_col = "label", prediction_col = "prediction",
                                               uid = random_string("isotonic_regression_"), ...) {

  stage <- ml_isotonic_regression.spark_connection(
    x = spark_connection(x),
    formula = formula,
    feature_index = feature_index,
    isotonic = isotonic,
    weight_col = weight_col,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_isotonic_regression.tbl_spark <- function(x, formula = NULL, feature_index = 0, isotonic = TRUE,
                                             weight_col = NULL, features_col = "features",
                                             label_col = "label", prediction_col = "prediction",
                                             uid = random_string("isotonic_regression_"),
                                             response = NULL, features = NULL, ...) {
  ml_formula_transformation()

  stage <- ml_isotonic_regression.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    feature_index = feature_index,
    isotonic = isotonic,
    weight_col = weight_col,
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
                         "regression", new_ml_model_isotonic_regression)
  }
}

# Validator
ml_validator_isotonic_regression <- function(.args) {
  .args[["feature_index"]] <- cast_scalar_integer(.args[["feature_index"]])
  .args[["isotonic"]] <- cast_scalar_logical(.args[["isotonic"]])
  .args[["weight_col"]] <- cast_nullable_string(.args[["weight_col"]])
  .args
}

new_ml_isotonic_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_isotonic_regression")
}

new_ml_isotonic_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    boundaries = function() read_spark_vector(jobj, "boundaries"), # lazy val
    predictions = function() read_spark_vector(jobj, "predictions"), # lazy val
    feature_index = invoke(jobj, "getFeatureIndex"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    subclass = "ml_isotonic_regression_model")
}
