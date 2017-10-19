#' @rdname ml-tuning
#' @param num_folds Number of folds for cross validation. Must be >= 2. Default: 3
#' @export
ml_cross_validator <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  num_folds = 3L,
  seed = NULL,
  uid = random_string("cross_validator_"),
  ...
) {
  UseMethod("ml_cross_validator")
}

#' @export
ml_cross_validator.spark_connection <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  num_folds = 3L,
  seed = NULL,
  uid = random_string("cross_validator_"),
  ...
) {

  num_folds <- ensure_scalar_integer(num_folds)

  ml_new_validator(x, "org.apache.spark.ml.tuning.CrossValidator",
                   estimator, evaluator, estimator_param_maps, seed) %>%
    invoke("setNumFolds", num_folds) %>%
    new_ml_cross_validator()
}

#' @export
ml_cross_validator.ml_pipeline <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  num_folds = 3L,
  seed = NULL,
  uid = random_string("cross_validator_"),
  ...
) {
  cv <- ml_new_stage_modified_args()
  ml_add_stage(x, cv)
}

#' @export
ml_cross_validator.tbl_spark <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  num_folds = 3L,
  seed = NULL,
  uid = random_string("cross_validator_"),
  ...
) {
  cv <- ml_new_stage_modified_args()
  cv %>%
    ml_fit(x)
}

# Constructors
#
new_ml_cross_validator <- function(jobj) {
  new_ml_tuning(jobj,
                num_folds = invoke(jobj, "getNumFolds"),
                subclass = "ml_cross_validator")
}

new_ml_cross_validator_model <- function(jobj) {
  avg_metrics <- invoke(jobj, "avgMetrics")
  metric_name <- jobj %>%
    invoke("getEvaluator") %>%
    invoke("getMetricName") %>%
    rlang::sym()

  new_ml_tuning_model(
    jobj,
    metric_name = metric_name,
    avg_metrics = avg_metrics,
    avg_metrics_df = ml_get_estimator_param_maps(jobj) %>%
      param_maps_to_df() %>%
      dplyr::mutate(!!metric_name := avg_metrics) %>%
      dplyr::select(!!metric_name, dplyr::everything()),
    subclass = "ml_cross_validator_model")
}

# Generic implementations

#' @export
print.ml_cross_validator <- function(x, ...) {
  num_sets <- length(x$estimator_param_maps)

  ml_print_class(x)
  ml_print_uid(x)
  cat(paste0("  ", "Estimator: ", ml_short_type(x$estimator), " "))
  ml_print_uid(x$estimator)
  cat(paste0("  Evaluator: ", ml_short_type(x$evaluator), " "))
  ml_print_uid(x$evaluator)
  cat("    with metric", ml_param(x$evaluator, "metric_name"), "\n")
  cat("  Number of folds:", x$num_folds, "\n")
  cat("  Tuning over", num_sets, "hyperparameter",
      if (num_sets == 1) "set" else "sets")
}
