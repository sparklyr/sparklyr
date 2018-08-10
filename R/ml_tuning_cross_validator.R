#' @rdname ml-tuning
#' @param num_folds Number of folds for cross validation. Must be >= 2. Default: 3
#' @export
ml_cross_validator <- function(x, estimator = NULL, estimator_param_maps = NULL,
                               evaluator = NULL, num_folds = 3, collect_sub_models = FALSE,
                               parallelism = 1, seed = NULL,
                               uid = random_string("cross_validator_"), ...) {
  UseMethod("ml_cross_validator")
}

#' @export
ml_cross_validator.spark_connection <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                                evaluator = NULL, num_folds = 3, collect_sub_models = FALSE,
                                                parallelism = 1, seed = NULL,
                                                uid = random_string("cross_validator_"), ...) {
  .args <- list(
    estimator = estimator,
    estimator_param_maps = estimator_param_maps,
    evaluator = evaluator,
    num_folds = num_folds,
    collect_sub_models = collect_sub_models,
    parallelism = parallelism,
    seed = seed
  ) %>%
    ml_validator_cross_validator()

  ml_new_validator(
    x, "org.apache.spark.ml.tuning.CrossValidator", uid,
    .args[["estimator"]], .args[["evaluator"]], .args[["estimator_param_maps"]],
    .args[["seed"]]
  ) %>%
    invoke("setNumFolds", .args[["num_folds"]]) %>%
    maybe_set_param("setCollectSubModels", .args[["collect_sub_models"]], "2.3.0", FALSE) %>%
    maybe_set_param("setParallelism", .args[["parallelism"]], "2.3.0", 1) %>%
    new_ml_cross_validator()
}

#' @export
ml_cross_validator.ml_pipeline <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                           evaluator = NULL, num_folds = 3, collect_sub_models = FALSE,
                                           parallelism = 1, seed = NULL,
                                           uid = random_string("cross_validator_"), ...) {
  stage <- ml_cross_validator.spark_connection(
    x = spark_connection(x),
    estimator = estimator,
    estimator_param_maps = estimator_param_maps,
    evaluator = evaluator,
    num_folds = num_folds,
    collect_sub_models = collect_sub_models,
    parallelism = parallelism,
    seed = seed,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_cross_validator.tbl_spark <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                         evaluator = NULL, num_folds = 3, collect_sub_models = FALSE,
                                         parallelism = 1, seed = NULL,
                                         uid = random_string("cross_validator_"), ...) {
  stage <- ml_cross_validator.spark_connection(
    x = spark_connection(x),
    estimator = estimator,
    estimator_param_maps = estimator_param_maps,
    evaluator = evaluator,
    num_folds = num_folds,
    collect_sub_models = collect_sub_models,
    parallelism = parallelism,
    seed = seed,
    uid = uid,
    ...
  )
  stage %>%
    ml_fit(x)
}

ml_validator_cross_validator <- function(.args) {
  .args <- validate_args_tuning(.args)
  .args[["num_folds"]] <- cast_scalar_integer(.args[["num_folds"]])
  .args
}

new_ml_cross_validator <- function(jobj) {
  new_ml_tuning(
    jobj,
    num_folds = invoke(jobj, "getNumFolds"),
    subclass = "ml_cross_validator"
  )
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
                 purrr::map(~ purrr::map(.x, ml_constructor_dispatch))
      )
    },
    subclass = "ml_cross_validator_model")
}

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
