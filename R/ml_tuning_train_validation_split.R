#' @rdname ml-tuning
#' @param train_ratio Ratio between train and validation data. Must be between 0 and 1. Default: 0.75
#' @export
ml_train_validation_split <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  train_ratio = 0.75,
  seed = NULL,
  uid = random_string("train_validation_split_"),
  ...
) {
  UseMethod("ml_train_validation_split")
}

#' @export
ml_train_validation_split.spark_connection <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  train_ratio = 0.75,
  seed = NULL,
  uid = random_string("train_validation_split_"),
  ...
) {

  train_ratio <- ensure_scalar_double(train_ratio)

  ml_new_validator(x, "org.apache.spark.ml.tuning.TrainValidationSplit",
                   estimator, evaluator, estimator_param_maps, seed) %>%
    invoke("setTrainRatio", train_ratio) %>%
    new_ml_train_validation_split()
}

#' @export
ml_train_validation_split.ml_pipeline <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  train_ratio = 0.75,
  seed = NULL,
  uid = random_string("train_validation_split_"),
  ...
) {
  cv <- ml_new_stage_modified_args()
  ml_add_stage(x, cv)
}

#' @export
ml_train_validation_split.tbl_spark <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  train_ratio = 0.75,
  seed = NULL,
  uid = random_string("train_validation_split_"),
  ...
) {
  cv <- ml_new_stage_modified_args()
  cv %>%
    ml_fit(x)
}

# Constructors

new_ml_train_validation_split <- function(jobj) {
  new_ml_tuning(jobj,
                train_ratio = invoke(jobj, "getTrainRatio"),
                subclass = "ml_train_validation_split")
}

new_ml_train_validation_split_model <- function(jobj) {
  validation_metrics <- invoke(jobj, "validationMetrics")
  metric_name <- jobj %>%
    invoke("getEvaluator") %>%
    invoke("getMetricName") %>%
    rlang::sym()
  new_ml_tuning_model(
    jobj,
    metric_name = metric_name,
    validation_metrics = validation_metrics,
    validation_metrics_df = ml_get_estimator_param_maps(jobj) %>%
      param_maps_to_df() %>%
      dplyr::mutate(!!metric_name := validation_metrics) %>%
      dplyr::select(!!metric_name, dplyr::everything()),
    subclass = "ml_train_validation_split_model")
}

# Generic implementations

#' @export
print.ml_train_validation_split <- function(x, ...) {
  num_sets <- length(x$estimator_param_maps)

  ml_print_class(x)
  ml_print_uid(x)
  cat(paste0("  ", "Estimator: ", ml_short_type(x$estimator), " "))
  ml_print_uid(x$estimator)
  cat(paste0("  Evaluator: ", ml_short_type(x$evaluator), " "))
  ml_print_uid(x$evaluator)
  cat("    with metric", ml_param(x$evaluator, "metric_name"), "\n")
  cat("  Train Ratio:", x$train_ratio, "\n")
  cat("  Tuning over", num_sets, "hyperparameter",
      if (num_sets == 1) "set" else "sets")
}
