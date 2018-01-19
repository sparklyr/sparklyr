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

ml_new_transformer <- function(sc, class, input_col, output_col, uid) {
  ensure_scalar_character(input_col)
  ensure_scalar_character(output_col)
  ensure_scalar_character(uid)
  invoke_new(sc, class, uid) %>%
    invoke("setInputCol", input_col) %>%
    invoke("setOutputCol", output_col)
}


ml_wrap_in_pipeline <- function(jobj) {
  sc <- spark_connection(jobj)
  invoke_static(sc,
                "sparklyr.MLUtils",
                "wrapInPipeline",
                jobj)
}



ml_new_stage_modified_args <- function(envir = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  modified_args <- caller_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    lapply(rlang::new_quosure, env = envir) %>%
    rlang::modify(
      x = rlang::new_quosure(rlang::parse_expr("spark_connection(x)"), env = caller_frame$env)
    ) %>%
    # filter `features` so it doesn't get partial matched to `features_col`
    (function(x) x[setdiff(names(x), "features")])

  stage_constructor <- sub("\\..*$", ".spark_connection", rlang::lang_name(caller_frame))
  rlang::lang(stage_constructor, !!!modified_args) %>%
    rlang::eval_tidy()
}

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
             ensure_scalar_integer()) %>%
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
  uid <- ensure_scalar_character(uid)
  invoke_new(sc, class, uid)
}
