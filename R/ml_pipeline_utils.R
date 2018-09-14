ml_add_stage <- function(x, transformer) {
  sc <- spark_connection(x)
  stages <- if (rlang::is_null(ml_stages(x))) list(spark_jobj(transformer)) else {
    tryCatch(spark_jobj(x) %>%
               invoke("getStages") %>%
               c(spark_jobj(transformer)),
             error = function(e) spark_jobj(x) %>%
               invoke("stages") %>%
               c(spark_jobj(transformer))
    )
  }

  jobj <- invoke_static(sc, "sparklyr.MLUtils",
                        "createPipelineFromStages",
                        ml_uid(x),
                        stages)
  new_ml_pipeline(jobj)
}

maybe_set_param <- function(jobj, setter, value, min_version = NULL, default = NULL) {
  # if value is NULL, don't set
  if (is.null(value)) return(jobj)

  if (!is.null(min_version)) {
    # if min_version specified, check Spark version
    ver <- jobj %>%
      spark_connection() %>%
      spark_version()

    if (ver < min_version) {
      if (!isTRUE(all.equal(value, default))) {
        # if user does not have required version, and tries to set parameter, throw error
        stop(paste0("Parameter '", deparse(substitute(value)),
                    "' is only available for Spark ", min_version, " and later."))
      } else {
        # otherwise, return jobj untouched
        return(jobj)
      }
    }
  }

  invoke(jobj, setter, value)
}

ml_new_transformer <- function(sc, class, uid,
                               input_col = NULL, output_col = NULL,
                               input_cols = NULL, output_cols = NULL) {
  uid <- cast_string(uid)
  invoke_new(sc, class, uid) %>%
    maybe_set_param("setInputCol", input_col) %>%
    maybe_set_param("setInputCols", input_cols) %>%
    maybe_set_param("setOutputCol", output_col) %>%
    maybe_set_param("setOutputCols", output_cols)
}

validate_args_transformer <- function(.args) {
  .args <- ml_backwards_compatibility(.args)
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
  sdf %>%
    invoke("schema") %>%
    invoke("apply", sdf %>%
             invoke("schema") %>%
             invoke("fieldIndex", column) %>%
             cast_scalar_integer()) %>%
    invoke("metadata") %>%
    invoke("json") %>%
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

ml_new_identifiable <- function(sc, class, uid) {
  uid <- cast_string(uid)
  invoke_new(sc, class, uid)
}

ml_backwards_compatibility <- function(.args, mapping_list = NULL) {
  mapping_list <- mapping_list %||%
    c(input.col = "input_col",
      output.col = "output_col")

  purrr::iwalk(
    mapping_list,
    ~ if (!is.null(.args[[.y]])) {
      .args[[.x]] <<- .args[[.y]]
      warning("The parameter `", .y,
              "` is deprecated and will be removed in a future release. Please use `",
              .x, "` instead.",
              call. = FALSE)
    }
  )

  .args
}
