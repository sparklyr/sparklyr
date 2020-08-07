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

ml_validate_params <- function(expanded_params, stage_jobjs, current_param_list) {
  stage_uids <- names(stage_jobjs)
  stage_indices <- integer(0)

  expanded_params %>%
    purrr::imap(function(param_sets, user_input_name) {

      # Determine the pipeline stage based on the user specified name.
      matched <- paste0("^", user_input_name) %>%
        grepl(stage_uids) %>%
        which()

      # Error if we find more than one or no stage in the pipeline with the name.
      if (length(matched) > 1) {
        stop("The name ", user_input_name, " matches more than one stage in the pipeline.",
          call. = FALSE
        )
      }
      if (length(matched) == 0) {
        stop("The name ", user_input_name, " matches no stages in the pipeline.",
          call. = FALSE
        )
      }

      # Save the index of the matched stage, this will be used for naming later.
      stage_indices[[user_input_name]] <<- matched

      stage_jobj <- stage_jobjs[[matched]]

      purrr::map(param_sets, function(params) {
        # Parameters currently specified in the pipeline object.
        current_params <- current_param_list[[matched]] %>%
          ml_map_param_list_names()

        # Default arguments based on function formals.
        default_params <- stage_jobj %>%
          ml_get_stage_constructor() %>%
          formals() %>%
          as.list() %>%
          purrr::discard(~ is.symbol(.x) || is.language(.x)) %>%
          purrr::compact()

        # Create a list of arguments to be validated. The precedence is as follows:
        #   1. User specified values in `estimator_param_maps`
        #   2. Values already set in pipeline `estimator`
        #   3. Default arguments based on constructor function
        input_param_names <- names(params)
        current_param_names <- names(current_params)
        default_param_names <- names(default_params)

        current_params_keep <- setdiff(current_param_names, input_param_names)
        default_params_keep <- setdiff(default_param_names, current_params_keep)

        args_to_validate <- c(
          params, current_params[current_params_keep], default_params[default_params_keep]
        )

        # Call the validator associated with the stage, and return the (validated)
        #   parameters the user specified.
        do.call(ml_get_stage_validator(stage_jobj), list(args_to_validate)) %>%
          `[`(input_param_names)
      })
    }) %>%
    rlang::set_names(stage_uids[stage_indices])
}

ml_spark_param_map <- function(param_map, sc, stage_jobjs) {
  purrr::imap(param_map, function(param_set, stage_uid) {
    purrr::imap(param_set, function(value, param_name) {
      # Get the Param object by calling `[stage].[param]` in Scala
      list(
        param_jobj = stage_jobjs[[stage_uid]] %>%
          invoke(ml_map_param_names(param_name, "rs")),
        value = value
      )
    }) %>%
      purrr::discard(~ is.null(.x[["value"]]))
  }) %>%
    unname() %>%
    rlang::flatten() %>%
    purrr::reduce(
      function(x, pair) invoke(x, "put", pair$param_jobj, pair$value),
      .init = invoke_new(sc, "org.apache.spark.ml.param.ParamMap")
    )
}

ml_get_estimator_param_maps <- function(jobj) {
  sc <- spark_connection(jobj)
  jobj %>%
    invoke("getEstimatorParamMaps") %>%
    purrr::map(~ invoke_static(sc, "sparklyr.MLUtils", "paramMapToNestedList", .x)) %>%
    purrr::map(~ lapply(.x, ml_map_param_list_names))
}

ml_new_validator <- function(sc, class, uid, estimator, evaluator,
                             estimator_param_maps, seed) {
  uid <- cast_string(uid)

  possibly_spark_jobj <- possibly_null(spark_jobj)

  param_maps <- if (!is.null(estimator) && !is.null(estimator_param_maps)) {
    stage_jobjs <- if (inherits(estimator, "ml_pipeline")) {
      invoke_static(sc, "sparklyr.MLUtils", "uidStagesMapping", spark_jobj(estimator))
    } else {
      rlang::set_names(list(spark_jobj(estimator)), ml_uid(estimator))
    }

    current_param_list <- stage_jobjs %>%
      purrr::map(invoke, "extractParamMap") %>%
      purrr::map(~ invoke_static(sc, "sparklyr.MLUtils", "paramMapToList", .x))

    estimator_param_maps %>%
      purrr::map(purrr::cross) %>%
      ml_validate_params(stage_jobjs, current_param_list) %>%
      purrr::cross() %>%
      purrr::map(ml_spark_param_map, sc, stage_jobjs)
  }

  jobj <- invoke_new(sc, class, uid) %>%
    jobj_set_param("setEstimator", possibly_spark_jobj(estimator)) %>%
    jobj_set_param("setEvaluator", possibly_spark_jobj(evaluator)) %>%
    jobj_set_param("setSeed", seed)

  if (!is.null(param_maps)) {
    invoke_static(
      sc, "sparklyr.MLUtils", "setParamMaps",
      jobj, param_maps
    )
  } else {
    jobj
  }
}

new_ml_tuning <- function(jobj, ..., class = character()) {
  new_ml_estimator(
    jobj,
    estimator = possibly_null(
      ~ invoke(jobj, "getEstimator") %>% ml_call_constructor()
    )(),
    evaluator = possibly_null(
      ~ invoke(jobj, "getEvaluator") %>% ml_call_constructor()
    )(),
    estimator_param_maps = possibly_null(ml_get_estimator_param_maps)(jobj),
    ...,
    class = c(class, "ml_tuning")
  )
}

new_ml_tuning_model <- function(jobj, ..., class = character()) {
  new_ml_transformer(
    jobj,
    estimator = invoke(jobj, "getEstimator") %>%
      ml_call_constructor(),
    evaluator = invoke(jobj, "getEvaluator") %>%
      ml_call_constructor(),
    estimator_param_maps = ml_get_estimator_param_maps(jobj),
    best_model = ml_call_constructor(invoke(jobj, "bestModel")),
    ...,
    class = c(class, "ml_tuning_model")
  )
}

print_tuning_info <- function(x, type = c("cv", "tvs")) {
  type <- match.arg(type)
  num_sets <- length(x$estimator_param_maps)

  ml_print_class(x)
  ml_print_uid(x)

  # Abort if no hyperparameter grid is set.
  if (!num_sets) {
    return(invisible(NULL))
  }

  cat(" (Parameters -- Tuning)\n")

  if (!is.null(x$estimator)) {
    cat(paste0("  estimator: ", ml_short_type(x$estimator), "\n"))
    cat(paste0("             "))
    ml_print_uid(x$estimator)
  }

  if (!is.null(x$evaluator)) {
    cat(paste0("  evaluator: ", ml_short_type(x$evaluator), "\n"))
    cat(paste0("             "))
    ml_print_uid(x$evaluator)
    cat("    with metric", ml_param(x$evaluator, "metric_name"), "\n")
  }


  if (identical(type, "cv")) {
    cat("  num_folds:", x$num_folds, "\n")
  } else {
    cat("  train_ratio:", x$train_ratio, "\n")
  }
  cat(
    "  [Tuned over", num_sets, "hyperparameter",
    if (num_sets == 1) "set]" else "sets]"
  )
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
  cat(paste0(
    "  over ", num_sets, " hyperparameter ",
    if (num_sets == 1) "set" else "sets"
  ), "\n")

  if (identical(type, "cv")) {
    cat("  via", paste0(x$num_folds, "-fold cross validation"))
  } else {
    cat("  via", paste0(x$train_ratio, "/", 1 - x$train_ratio, " train-validation split"))
  }
  cat("\n\n")

  cat(paste0("Estimator: ", ml_short_type(x$estimator), "\n"))
  cat(paste0("           "))
  ml_print_uid(x$estimator)
  cat(paste0("Evaluator: ", ml_short_type(x$evaluator), "\n"))
  cat(paste0("           "))
  ml_print_uid(x$evaluator)
  cat("\n")

  cat(paste0("Results Summary:"), "\n")
  if (identical(type, "cv")) {
    print(x$avg_metrics_df)
  } else {
    print(x$validation_metrics_df)
  }
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
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' # Create a pipeline
#' pipeline <- ml_pipeline(sc) %>%
#'   ft_r_formula(Species ~ .) %>%
#'   ml_random_forest_classifier()
#'
#' # Specify hyperparameter grid
#' grid <- list(
#'   random_forest = list(
#'     num_trees = c(5, 10),
#'     max_depth = c(5, 10),
#'     impurity = c("entropy", "gini")
#'   )
#' )
#'
#' # Create the cross validator object
#' cv <- ml_cross_validator(
#'   sc,
#'   estimator = pipeline, estimator_param_maps = grid,
#'   evaluator = ml_multiclass_classification_evaluator(sc),
#'   num_folds = 3,
#'   parallelism = 4
#' )
#'
#' # Train the models
#' cv_model <- ml_fit(cv, iris_tbl)
#'
#' # Print the metrics
#' ml_validation_metrics(cv_model)
#' }
#'
#' @export
ml_validation_metrics <- function(model) {
  if (inherits(model, "ml_cross_validator_model")) {
    model$avg_metrics_df
  } else if (inherits(model, "ml_train_validation_split_model")) {
    model$validation_metrics_df
  } else {
    stop("ml_validation_metrics() must be called on `ml_cross_validator_model` ",
      "or `ml_train_validation_split_model`.",
      call. = FALSE
    )
  }
}

param_maps_to_df <- function(param_maps) {
  param_maps %>%
    lapply(function(param_map) {
      param_map %>%
        lapply(data.frame, stringsAsFactors = FALSE) %>%
        (function(x) {
          lapply(seq_along(x), function(n) {
            fn <- function(x) paste(x, n, sep = "_")
            dplyr::rename_all(x[[n]], fn)
          })
        }) %>%
        dplyr::bind_cols()
    }) %>%
    dplyr::bind_rows()
}

validate_args_tuning <- function(.args) {
  .args[["collect_sub_models"]] <- cast_scalar_logical(.args[["collect_sub_models"]])
  .args[["parallelism"]] <- cast_scalar_integer(.args[["parallelism"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  if (!is.null(.args[["estimator"]]) && !inherits(.args[["estimator"]], "ml_estimator")) {
    stop("`estimator` must be an `ml_estimator`.")
  }
  if (!is.null(.args[["estimator_param_maps"]]) && !rlang::is_bare_list(.args[["estimator_param_maps"]])) {
    stop("`estimator_param_maps` must be a list.")
  }
  if (!is.null(.args[["evaluator"]]) && !inherits(.args[["evaluator"]], "ml_evaluator")) {
    stop("`evaluator` must be an `ml_evaluator`.")
  }
  .args
}
