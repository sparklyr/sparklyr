#' @rdname ml-tuning
#' @param train_ratio Ratio between train and validation data. Must be between 0 and 1. Default: 0.75
#' @export
ml_train_validation_split <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  train_ratio = 0.75,
  collect_sub_models = FALSE,
  parallelism = 1L,
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
  collect_sub_models = FALSE,
  parallelism = 1L,
  seed = NULL,
  uid = random_string("train_validation_split_"),
  ...
) {

  train_ratio <- ensure_scalar_double(train_ratio)
  collect_sub_models <- ensure_scalar_boolean(collect_sub_models)
  parallelism <- ensure_scalar_integer(parallelism)

  ml_new_validator(x, "org.apache.spark.ml.tuning.TrainValidationSplit", uid,
                   estimator, evaluator, estimator_param_maps, seed) %>%
    invoke("setTrainRatio", train_ratio) %>%
    jobj_set_param("setCollectSubModels", collect_sub_models, FALSE, "2.3.0") %>%
    jobj_set_param("setParallelism", parallelism, 1L, "2.3.0") %>%
    new_ml_train_validation_split()
}

#' @export
ml_train_validation_split.ml_pipeline <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  train_ratio = 0.75,
  collect_sub_models = FALSE,
  parallelism = 1L,
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
  collect_sub_models = FALSE,
  parallelism = 1L,
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
    train_ratio = invoke(jobj, "getTrainRatio"),
    metric_name = metric_name,
    validation_metrics = validation_metrics,
    validation_metrics_df = ml_get_estimator_param_maps(jobj) %>%
      param_maps_to_df() %>%
      dplyr::mutate(!!metric_name := validation_metrics) %>%
      dplyr::select(!!metric_name, dplyr::everything()),
    sub_models = function() {
      try_null(jobj %>%
                 invoke("subModels") %>%
                 lapply(ml_constructor_dispatch)
      )
    },
    subclass = "ml_train_validation_split_model")
}

# Generic implementations

#' @export
print.ml_train_validation_split <- function(x, ...) {
  print_tuning_info(x, "tvs")
}

#' @export
print.ml_train_validation_split_model <- function(x, ...) {
  print_tuning_info(x, "tvs")
  print_best_model(x)
}

#' @export
summary.ml_train_validation_split_model <- function(object, ...) {
  print_tuning_summary(object, "tvs")
}
