#' @rdname ml-tuning
#' @param train_ratio Ratio between train and validation data. Must be between 0 and 1. Default: 0.75
#' @export
ml_train_validation_split <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                      evaluator = NULL, train_ratio = 0.75,
                                      collect_sub_models = FALSE, parallelism = 1,
                                      seed = NULL, uid = random_string("train_validation_split_"),
                                      ...) {
  UseMethod("ml_train_validation_split")
}

#' @export
ml_train_validation_split.spark_connection <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                                       evaluator = NULL, train_ratio = 0.75,
                                                       collect_sub_models = FALSE, parallelism = 1,
                                                       seed = NULL, uid = random_string("train_validation_split_"),
                                                       ...) {
  .args <- list(
    estimator = estimator,
    estimator_param_maps = estimator_param_maps,
    evaluator = evaluator,
    train_ratio = train_ratio,
    collect_sub_models = collect_sub_models,
    parallelism = parallelism,
    seed = seed
  ) %>%
    ml_validator_train_validation_split()

  ml_new_validator(
    x, "org.apache.spark.ml.tuning.TrainValidationSplit", uid,
    .args[["estimator"]], .args[["evaluator"]], .args[["estimator_param_maps"]], .args[["seed"]]
  ) %>%
    invoke("setTrainRatio", .args[["train_ratio"]]) %>%
    maybe_set_param("setCollectSubModels", .args[["collect_sub_models"]], "2.3.0", FALSE) %>%
    maybe_set_param("setParallelism", .args[["parallelism"]], "2.3.0", 1) %>%
    new_ml_train_validation_split()
}

#' @export
ml_train_validation_split.ml_pipeline <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                                  evaluator = NULL, train_ratio = 0.75,
                                                  collect_sub_models = FALSE, parallelism = 1,
                                                  seed = NULL, uid = random_string("train_validation_split_"),
                                                  ...) {
  stage <- ml_train_validation_split.spark_connection(
    x = spark_connection(x),
    estimator = estimator,
    estimator_param_maps = estimator_param_maps,
    evaluator = evaluator,
    train_ratio = train_ratio,
    collect_sub_models = collect_sub_models,
    parallelism = parallelism,
    seed = seed,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_train_validation_split.tbl_spark <- function(x, estimator = NULL, estimator_param_maps = NULL,
                                                evaluator = NULL, train_ratio = 0.75,
                                                collect_sub_models = FALSE, parallelism = 1,
                                                seed = NULL, uid = random_string("train_validation_split_"),
                                                ...) {
  stage <- ml_train_validation_split.spark_connection(
    x = spark_connection(x),
    estimator = estimator,
    estimator_param_maps = estimator_param_maps,
    evaluator = evaluator,
    train_ratio = train_ratio,
    collect_sub_models = collect_sub_models,
    parallelism = parallelism,
    seed = seed,
    uid = uid,
    ...
  )
  stage %>%
    ml_fit(x)
}

ml_validator_train_validation_split <- function(.args) {
  .args <- validate_args_tuning(.args)
  .args[["train_ratio"]] <- cast_scalar_double(.args[["train_ratio"]])
  .args
}

new_ml_train_validation_split <- function(jobj) {
  new_ml_tuning(
    jobj,
    train_ratio = invoke(jobj, "getTrainRatio"),
    subclass = "ml_train_validation_split"
  )
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
                 purrr::map(ml_constructor_dispatch)
      )
    },
    subclass = "ml_train_validation_split_model")
}

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
