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
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' iso_res <- iris_tbl %>%
#'   ml_isotonic_regression(Petal_Length ~ Petal_Width)
#'
#' pred <- ml_predict(iso_res, iris_test)
#'
#' pred
#' }
#'
#' @export
ml_isotonic_regression <- function(x, formula = NULL, feature_index = 0, isotonic = TRUE,
                                   weight_col = NULL, features_col = "features",
                                   label_col = "label", prediction_col = "prediction",
                                   uid = random_string("isotonic_regression_"), ...) {
  check_dots_used()
  UseMethod("ml_isotonic_regression")
}

ml_isotonic_regression_impl <- function(x, formula = NULL, feature_index = 0, isotonic = TRUE,
                                           weight_col = NULL, features_col = "features",
                                           label_col = "label", prediction_col = "prediction",
                                           uid = random_string("isotonic_regression_"),
                                           response = NULL, features = NULL, ...) {
  ml_process_model(
    x = x,
    r_class = "ml_isotonic_regression",
    ml_function = new_ml_model_isotonic_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      feature_index = feature_index,
      isotonic = isotonic,
      weight_col = weight_col
    )
  )

}

# ------------------------------- Methods --------------------------------------
#' @export
ml_isotonic_regression.spark_connection <- ml_isotonic_regression_impl

#' @export
ml_isotonic_regression.ml_pipeline <- ml_isotonic_regression_impl

#' @export
ml_isotonic_regression.tbl_spark <- ml_isotonic_regression_impl

# ---------------------------- Constructors ------------------------------------
new_ml_isotonic_regression_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    boundaries = function() read_spark_vector(jobj, "boundaries"), # lazy val
    predictions = function() read_spark_vector(jobj, "predictions"), # lazy val
    feature_index = invoke(jobj, "getFeatureIndex"),
    class = "ml_isotonic_regression_model"
  )
}
