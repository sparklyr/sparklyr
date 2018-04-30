#' Spark ML -- Tuning
#'
#' Perform hyper-parameter tuning using either K-fold cross validation or train-validation split.
#'
#' @details \code{ml_cross_validator()} performs k-fold cross validation while \code{ml_train_validation_split()} performs tuning on one pair of train and validation datasets.
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_cross_validator} or \code{ml_traing_validation_split} object.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the tuning estimator appended to the pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a tuning estimator is constructed then
#'   immediately fit with the input \code{tbl_spark}, returning a \code{ml_cross_validation_model} or a
#'   \code{ml_train_validation_split_model} object.
#' }
#'
#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the ML estimator.
#' @param ... Optional arguments; currently unused.
#' @param estimator A \code{ml_estimator} object.
#' @param estimator_param_maps A named list of stages and hyper-parameter sets to tune. See details.
#' @param evaluator A \code{ml_evaluator} object, see \link{ml_evaluator}.
#' @param collect_sub_models Whether to collect a list of sub-models trained during tuning.
#'   If set to \code{FALSE}, then only the single best sub-model will be available after fitting.
#'   If set to true, then all sub-models will be available. Warning: For large models, collecting
#'   all sub-models can cause OOMs on the Spark driver.
#' @param parallelism The number of threads to use when running parallel algorithms. Default is 1 for serial execution.
#' @template roxlate-ml-seed
#' @name ml-tuning
NULL

ml_expand_params <- function(param_grid) {
  param_grid %>%
    lapply(function(stage) {
      params <- names(stage)
      lapply(params, function(param) {
        param_values <- stage[[param]]
        param_values %>%
          lapply(function(value) {
            rlang::set_names(list(value = value), param)
          })
      }) %>%
        # compute param-value combinations within each stage
        expand.grid(stringsAsFactors = FALSE) %>%
        apply(1, list) %>%
        rlang::flatten() %>%
        lapply(. %>%
                 unname() %>%
                 rlang::flatten())
    })
}

ml_validate_params <- function(stages_params, uid_stages, current_param_list) {
  stage_names <- names(stages_params)
  matched_indices <- stage_names %>%
    sapply(function(x) {
      matched_index <- grepl(paste0("^", x), names(uid_stages)) %>% which()
      if (length(matched_index) > 1)
        stop(paste0("The name ", x,
                    " matches more than 1 stage in pipeline"))
      if (length(matched_index) == 0)
        stop(paste0("The name ", x, " matches no stages in pipeline"))
      matched_index
    })

# match stage names
stage_uids <- names(uid_stages)[matched_indices] %>%
  rlang::set_names(stage_names)
stage_names %>%
  lapply(function(stage_name) {
    stage_jobj <- uid_stages %>%
      `[[`(stage_uids[stage_name])
    lapply(stages_params[[stage_name]], function(params) {
      args_to_validate <- ml_args_to_validate(
        args = params,
        # current param list parsed from the pipeline jobj
        current_args = current_param_list %>%
          `[[`(stage_uids[stage_name]) %>%
          ml_map_param_list_names(),
        # default args from the stage constructor, excluding args with no default
        #   and `uid`
        default_args = Filter(
          Negate(rlang::is_symbol),
          stage_jobj %>%
            ml_get_stage_constructor() %>%
            rlang::fn_fmls() %>%
            rlang::modify(uid = NULL)
        ))
      # calls the appropriate validator and returns a list
      rlang::invoke(ml_get_stage_validator(stage_jobj),
                    args = args_to_validate,
                    nms = names(params))
    })
  }) %>%
  rlang::set_names(stage_uids)
}

ml_build_param_maps <- function(param_list) {
  # computes combinations at the stages level
  param_list %>%
    expand.grid(stringsAsFactors = FALSE) %>%
    apply(1, list) %>%
    rlang::flatten()
}

ml_spark_param_map <- function(param_map, sc, uid_stages) {
  stage_uids <- names(param_map)
  param_jobj_value_list <- stage_uids %>%
    lapply(function(stage_uid) {
      params <- param_map[[stage_uid]]
      Filter(function(x) !rlang::is_null(x$value),
             # only create param_map with non-null values
             names(params) %>%
               lapply(function(param_name) {
                 # get the Param object by calling `[stage].[param]` in Scala
                 list(param_jobj = uid_stages[[stage_uid]] %>%
                        invoke(ml_map_param_names(param_name, "rs")),
                      value = params[[param_name]]
                 )
               }))
    }) %>%
    rlang::flatten()

  # put the param pairs into a ParamMap
  Reduce(function(x, pair) invoke(x, "put", pair$param_jobj, pair$value),
         param_jobj_value_list,
         invoke_new(sc, "org.apache.spark.ml.param.ParamMap"))
}


param_maps_to_df <- function(param_maps) {
  param_maps %>%
    lapply(function(param_map) {
      param_map %>%
        lapply(data.frame, stringsAsFactors = FALSE) %>%
        (function(x) lapply(seq_along(x), function(n) {
          fn <- function(x) paste(x, n, sep = "_")
          dplyr::rename_all(x[[n]], fn)
        })) %>% dplyr::bind_cols()
    }) %>%
    dplyr::bind_rows()
}

ml_get_estimator_param_maps <- function(jobj) {
  sc <- spark_connection(jobj)
  jobj %>%
    invoke("getEstimatorParamMaps") %>%
    lapply(function(x)
      invoke_static(sc,
                    "sparklyr.MLUtils",
                    "paramMapToNestedList",
                    x)) %>%
    lapply(function(x)
      lapply(x, ml_map_param_list_names))
}

ml_new_validator <- function(
  sc, class, uid, estimator, evaluator, estimator_param_maps, seed) {
  seed <- ensure_scalar_integer(seed, allow.null = TRUE)

  uid <- ensure_scalar_character(uid)

  if (!inherits(evaluator, "ml_evaluator"))
    stop("evaluator must be a 'ml_evaluator'")
  if (!is_ml_estimator(estimator))
    stop("estimator must be a 'ml_estimator'")

  uid_stages <- if (inherits(estimator, "ml_pipeline"))
    invoke_static(sc,
                  "sparklyr.MLUtils",
                  "uidStagesMapping",
                  spark_jobj(estimator)) else
                    rlang::set_names(list(spark_jobj(estimator)), ml_uid(estimator))

  current_param_list <- uid_stages %>%
    lapply(invoke, "extractParamMap") %>%
    lapply(function(x) invoke_static(sc,
                                     "sparklyr.MLUtils",
                                     "paramMapToList",
                                     x))

  param_maps <- estimator_param_maps %>%
    ml_expand_params() %>%
    ml_validate_params(uid_stages, current_param_list) %>%
    ml_build_param_maps() %>%
    lapply(ml_spark_param_map, sc, uid_stages)

  jobj <- invoke_new(sc, class, uid) %>%
    invoke_static(sc, "sparklyr.MLUtils", "setParamMaps",
                  ., param_maps) %>%
    invoke("setEstimator", spark_jobj(estimator)) %>%
    invoke("setEvaluator", spark_jobj(evaluator))

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  jobj
}

new_ml_tuning <- function(jobj, ..., subclass = NULL) {
  new_ml_estimator(jobj,
                   estimator = invoke(jobj, "getEstimator") %>%
                     ml_constructor_dispatch(),
                   evaluator = invoke(jobj, "getEvaluator") %>%
                     ml_constructor_dispatch(),
                   estimator_param_maps = ml_get_estimator_param_maps(jobj),
                   ...,
                   subclass = c(subclass, "ml_tuning"))
}

new_ml_tuning_model <- function(jobj, ..., subclass = NULL) {
  new_ml_transformer(
    jobj,
    estimator = invoke(jobj, "getEstimator") %>%
      ml_constructor_dispatch(),
    evaluator = invoke(jobj, "getEvaluator") %>%
      ml_constructor_dispatch(),
    estimator_param_maps = ml_get_estimator_param_maps(jobj),
    best_model = ml_constructor_dispatch(invoke(jobj, "bestModel")),
    ...,
    subclass = c(subclass, "ml_tuning_model"))
}

print_tuning_info <- function(x, type = c("cv", "tvs")) {
  type <- match.arg(type)
  num_sets <- length(x$estimator_param_maps)

  ml_print_class(x)
  ml_print_uid(x)
  cat(" (Parameters -- Tuning)\n")
  cat(paste0("  estimator: ", ml_short_type(x$estimator), "\n"))
  cat(paste0("             "))
  ml_print_uid(x$estimator)
  cat(paste0("  evaluator: ", ml_short_type(x$evaluator), "\n"))
  cat(paste0("             "))
  ml_print_uid(x$evaluator)
  cat("    with metric", ml_param(x$evaluator, "metric_name"), "\n")
  if (identical(type, "cv"))
    cat("  num_folds:", x$num_folds, "\n")
  else
    cat("  train_ratio:", x$train_ratio, "\n")
  cat("  [Tuned over", num_sets, "hyperparameter",
      if (num_sets == 1) "set]" else "sets]")
}

print_best_model <- function(x) {
  cat("\n (Best Model)\n")
  best_model_output <- capture.output(print(x$best_model))
  cat(paste0("  ", best_model_output), sep = "\n")
}

print_tuning_summary <- function(x, type = c("cv", "tvs")) {
  type <- match.arg(type)
  num_sets <- length(x$estimator_param_maps)

  cat(paste0("Summary for ", ml_short_type(x)), "\n")
  cat(paste0("            "))
  ml_print_uid(x)
  cat("\n")

  cat(paste0("Tuned ", ml_short_type(x$estimator), "\n"))
  cat(paste0("  with metric ", ml_param(x$evaluator, "metric_name"), "\n"))
  cat(paste0("  over ", num_sets, " hyperparameter ",
             if (num_sets == 1) "set" else "sets"), "\n")

  if (identical(type, "cv"))
    cat("  via", paste0(x$num_folds, "-fold cross validation"))
  else
    cat("  via", paste0(x$train_ratio, "/", 1 - x$train_ratio, " train-validation split"))
  cat("\n\n")

  cat(paste0("Estimator: ", ml_short_type(x$estimator), "\n"))
  cat(paste0("           "))
  ml_print_uid(x$estimator)
  cat(paste0("Evaluator: ", ml_short_type(x$evaluator), "\n"))
  cat(paste0("           "))
  ml_print_uid(x$evaluator)
  cat("\n")

  cat(paste0("Results Summary:"), "\n")
  if (identical(type, "cv"))
    print(x$avg_metrics_df)
  else
    print(x$validation_metrics_df)
}

#' @rdname ml-tuning
#' @param model A cross validation or train-validation-split model.
#' @return For cross validation, \code{ml_sub_models()} returns a nested
#'   list of models, where the first layer represents fold indices and the
#'   second layer represents param maps. For train-validation split,
#'   \code{ml_sub_models()} returns a list of models, corresponding to the
#'   order of the estimator param maps.
#' @export
ml_sub_models <- function(model) {
  fn <- model$sub_models %||% stop(
    "Cannot extract sub models. `collect_sub_models` must be set to TRUE in ",
    "ml_cross_validator() or ml_train_split_validation()."
  )
  fn()
}

#' @rdname ml-tuning
#' @return \code{ml_validation_metrics()} returns a data frame of performance
#'   metrics and hyperparameter combinations.
#' @export
ml_validation_metrics <- function(model) {
  if (inherits(model, "ml_cross_validator_model"))
    model$avg_metrics_df
  else if (inherits(model, "ml_train_validation_split_model"))
    model$validation_metrics_df
  else
    stop("ml_validation_metrics() must be called on `ml_cross_validator_model` ",
         "or `ml_train_validation_split_model`.", call. = FALSE)
}
