
#' Add a Stage to a Pipeline
#'
#' Adds a stage to a pipeline.
#'
#' @param x A pipeline or a pipeline stage.
#' @param stage A pipeline stage.
#' @keywords internal
#' @export
ml_add_stage <- function(x, stage) {
  sc <- spark_connection(x)
  stages <- if (rlang::is_null(ml_stages(x))) list(spark_jobj(stage)) else {
    tryCatch(spark_jobj(x) %>%
               invoke("getStages") %>%
               c(spark_jobj(stage)),
             error = function(e) spark_jobj(x) %>%
               invoke("stages") %>%
               c(spark_jobj(stage))
    )
  }

  jobj <- invoke_static(sc, "sparklyr.MLUtils",
                        "createPipelineFromStages",
                        ml_uid(x),
                        stages)
  new_ml_pipeline(jobj)
}

#' Parameter Setting for JVM Objects
#'
#' Sets a parameter value for a pipeline stage object.
#'
#' @param jobj A pipeline stage jobj.
#' @param setter The name of the setter method as a string.
#' @param value The value to be set.
#' @param min_version The minimum required Spark version for this parameter to be valid.
#' @param default The default value of the parameter, to be used together with `min_version`.
#'   An error is thrown if the user's Spark version is older than `min_version` and `value`
#'   differs from `default`.
#'
#' @keywords internal
#'
#' @export
jobj_set_param <- function(jobj, setter, value, min_version = NULL, default = NULL) {
  set_param_args <- jobj_set_param_helper(jobj, setter, value, min_version, default)
  if (is.null(set_param_args))
    # not setting any param
    jobj
  else
    do.call(invoke, c(jobj, set_param_args))
}

# returns NULL if no setter should be called, otherwise returns a list of params
# (aside from jobj itself) to be passed to `invoke`
jobj_set_param_helper <- function(jobj, setter, value, min_version = NULL, default = NULL) {
  # if value is NULL, don't set
  if (is.null(value)) return(NULL)

  if (!is.null(min_version)) {
    # if min_version specified, check Spark version
    ver <- jobj %>%
      spark_connection() %>%
      spark_version()

    if (ver < min_version) {
      if (!isTRUE(all.equal(c(value), default))) {
        # if user does not have required version, and tries to set parameter, throw error
        stop(paste0("Parameter `", deparse(substitute(value)),
                    "` is only available for Spark ", min_version, " and later."))
      } else {
        # otherwise, don't call the setter
        return(NULL)
      }
    }
  }

  list(setter, value)
}


#' Create a Pipeline Stage Object
#'
#' Helper function to create pipeline stage objects with common parameter setters.
#'
#' @param sc A `spark_connection` object.
#' @param class Class name for the pipeline stage.
#' @template roxlate-ml-uid
#' @template roxlate-ml-features-col
#' @template roxlate-ml-label-col
#' @template roxlate-ml-prediction-col
#' @template roxlate-ml-probabilistic-classifier-params
#' @template roxlate-ml-clustering-params
#' @template roxlate-ml-feature-input-output-col
#' @param input_cols Names of input columns.
#' @param input_cols Names of output columns.
#'
#' @keywords internal
#'
#' @export
spark_pipeline_stage <- function(sc, class, uid, features_col = NULL, label_col = NULL, prediction_col = NULL,
                                 probability_col = NULL, raw_prediction_col = NULL,
                                 k = NULL, max_iter = NULL, seed = NULL, input_col = NULL, input_cols = NULL,
                                 output_col = NULL, output_cols = NULL) {
  uid <- cast_string(uid)
  invoke_new(sc, class, uid) %>%
    jobj_set_ml_params(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      k = k,
      max_iter = max_iter,
      seed = seed,
      input_col = input_col,
      input_cols = input_cols,
      output_col = output_col,
      output_cols = output_cols
    )
}

jobj_set_ml_params <- function(jobj, features_col, label_col, prediction_col,
                               probability_col, raw_prediction_col,
                               k, max_iter, seed, input_col, input_cols,
                               output_col, output_cols) {
  params_to_set <- Filter(function(x) !is.null(x),
                          list(
                               jobj_set_param_helper(jobj, "setFeaturesCol", features_col),
                               jobj_set_param_helper(jobj, "setLabelCol", label_col),
                               jobj_set_param_helper(jobj, "setPredictionCol", prediction_col),
                               jobj_set_param_helper(jobj, "setProbabilityCol", probability_col),
                               jobj_set_param_helper(jobj, "setRawPredictionCol", raw_prediction_col),
                               jobj_set_param_helper(jobj, "setK", k),
                               jobj_set_param_helper(jobj, "setMaxIter", max_iter),
                               jobj_set_param_helper(jobj, "setSeed", seed),
                               jobj_set_param_helper(jobj, "setInputCol", input_col),
                               jobj_set_param_helper(jobj, "setInputCols", input_cols),
                               jobj_set_param_helper(jobj, "setOutputCol", output_col),
                               jobj_set_param_helper(jobj, "setOutputCols", output_cols)))
  if (length(params_to_set) > 0)
    do.call(invoke, c(jobj, "%>%", params_to_set))
  else
    # no need to set any param
    jobj
}

validate_args_transformer <- function(.args) {
  .args[["input_col"]] <- cast_nullable_string(.args[["input_col"]])
  .args[["input_cols"]] <- cast_nullable_string_list(.args[["input_cols"]])
  .args[["output_col"]] <- cast_nullable_string(.args[["output_col"]])
  .args[["output_cols"]] <- cast_nullable_string_list(.args[["output_cols"]])
  .args
}

# ml_wrap_in_pipeline <- function(jobj) {
#   sc <- spark_connection(jobj)
#   invoke_static(sc,
#                 "sparklyr.MLUtils",
#                 "wrapInPipeline",
#                 jobj)
# }

#' Spark ML -- UID
#'
#' Extracts the UID of an ML object.
#'
#' @param x A Spark ML object
#' @export
ml_uid <- function(x) {
  x$uid %||% stop("uid not found")
}

#' Spark ML -- Pipeline stage extraction
#'
#' Extraction of stages from a Pipeline or PipelineModel object.
#'
#' @param x A \code{ml_pipeline} or a \code{ml_pipeline_model} object
#' @param stage The UID of a stage in the pipeline.
#' @return For \code{ml_stage()}: The stage specified.
#' @export
ml_stage <- function(x, stage) {
  matched_index <- if (is.numeric(stage))
    stage
  else
    grep(paste0("^", stage), x$stage_uids)

  switch(length(matched_index) %>% as.character(),
         "0" = stop("stage not found"),
         "1" = x$stages[[matched_index]],
         stop("multiple stages found")
  )
}

#' @rdname ml_stage
#' @param stages The UIDs of stages in the pipeline as a character vector.
#' @return For \code{ml_stages()}: A list of stages. If \code{stages} is not set, the function returns all stages of the pipeline in a list.
#' @export
ml_stages <- function(x, stages = NULL) {
  if (rlang::is_null(stages)) {
    x$stages
  } else {
    matched_indexes <- if (is.numeric(stages))
    {
      stages
    } else {
      lapply(stages, function(stage) grep(paste0("^", stage), x$stage_uids)) %>%
        rlang::set_names(stages)
    }

    bad_matches <- Filter(function(idx) length(idx) != 1, matched_indexes)

    if (length(bad_matches)) {
      bad_match <- bad_matches[[1]]
      switch(as.character(length(bad_match)),
             "0" = stop("no stages found for identifier ", names(bad_matches)[1]),
             stop("multiple stages found for identifier ", names(bad_matches)[1])
      )
    }
    x$stages[unlist(matched_indexes)]
  }
}

ml_column_metadata <- function(tbl, column) {
  sdf <- spark_dataframe(tbl)
  sdf_schema <- invoke(sdf, "schema")
  field_index <- sdf_schema %>% invoke("fieldIndex", column) %>% cast_scalar_integer()
  sdf_schema %>% invoke("%>%",
                        list("apply", field_index),
                        list("metadata"),
                        list("json")) %>%
    jsonlite::fromJSON() %>%
    `[[`("ml_attr")
}

#' Spark ML -- Extraction of summary metrics
#'
#' Extracts a metric from the summary object of a Spark ML model.
#'
#' @param x A Spark ML model that has a summary.
#' @param metric The name of the metric to extract. If not set, returns the summary object.
#' @param allow_null Whether null results are allowed when the metric is not found in the summary.
#' @export
ml_summary <- function(x, metric = NULL, allow_null = FALSE) {
  if (rlang::is_null(metric))
    x$summary %||% stop(deparse(substitute(x)), " has no summary")
  else if (allow_null)
    x$summary[[metric]]
  else
    x$summary[[metric]] %||% stop("metric ", metric, " not found")
}
