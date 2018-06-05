#' @rdname ml-tuning
#' @param num_folds Number of folds for cross validation. Must be >= 2. Default: 3
#' @export
ml_cross_validator <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  num_folds = 3L,
  collect_sub_models = FALSE,
  parallelism = 1L,
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
  collect_sub_models = FALSE,
  parallelism = 1L,
  seed = NULL,
  uid = random_string("cross_validator_"),
  ...
) {

  num_folds <- ensure_scalar_integer(num_folds)
  collect_sub_models <- ensure_scalar_boolean(collect_sub_models)
  parallelism <- ensure_scalar_integer(parallelism)

  ml_new_validator(x, "org.apache.spark.ml.tuning.CrossValidator", uid,
                   estimator, evaluator, estimator_param_maps, seed) %>%
    invoke("setNumFolds", num_folds) %>%
    jobj_set_param("setCollectSubModels", collect_sub_models, FALSE, "2.3.0") %>%
    jobj_set_param("setParallelism", parallelism, 1L, "2.3.0") %>%
    new_ml_cross_validator()
}

#' @export
ml_cross_validator.ml_pipeline <- function(
  x, estimator, estimator_param_maps,
  evaluator,
  num_folds = 3L,
  collect_sub_models = FALSE,
  parallelism = 1L,
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
  collect_sub_models = FALSE,
  parallelism = 1L,
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
    num_folds = invoke(jobj, "getNumFolds"),
    metric_name = metric_name,
    avg_metrics = avg_metrics,
    avg_metrics_df = ml_get_estimator_param_maps(jobj) %>%
      param_maps_to_df() %>%
      dplyr::mutate(!!metric_name := avg_metrics) %>%
      dplyr::select(!!metric_name, dplyr::everything()),
    sub_models = function() {
      try_null(jobj %>%
                 invoke("subModels") %>%
                 lapply(function(fold) lapply(fold, ml_constructor_dispatch))
      )
    },
    subclass = "ml_cross_validator_model")
}

# Generic implementations

#' @export
print.ml_cross_validator <- function(x, ...) {
  print_tuning_info(x, "cv")
}

#' @export
print.ml_cross_validator_model <- function(x, ...) {
  print_tuning_info(x, "cv")
  print_best_model(x)
}

#' @export
summary.ml_cross_validator_model <- function(object, ...) {
  print_tuning_summary(object, "cv")
}
