ml_add_stage <- function(x, transformer) {
  sc <- spark_connection(x)
  stages <- if (rlang::is_na(x$stages)) list(spark_jobj(transformer)) else {
    spark_jobj(x) %>%
      invoke("getStages") %>%
      c(spark_jobj(transformer))
  }

  jobj <- invoke_static(sc, "sparklyr.MLUtils",
                        "createPipelineFromStages",
                        x$uid,
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

ml_get_param_map <- function(jobj) {
  sc <- spark_connection(jobj)
  object <- if (spark_version(sc) < "2.0.0")
    "sparklyr.MLUtils"
  else
    "sparklyr.MLUtils2"

    invoke_static(sc,
                  object,
                  "getParamMap",
                  jobj) %>%
      ml_map_param_list_names()
}

ml_new_stage_modified_args <- function(envir = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  modified_args <- caller_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    lapply(rlang::new_quosure, env = envir) %>%
    rlang::modify(
      x = rlang::new_quosure(rlang::expr(spark_connection(x)), env = caller_frame$env)
    )
  stage_constructor <- sub("\\..*$", ".spark_connection", rlang::lang_name(caller_frame))
  rlang::lang(stage_constructor, !!!modified_args) %>%
    rlang::eval_tidy()
}

ml_map_param_list_names <- function(x, direction = c("sr", "rs"), ...) {
  direction <- rlang::arg_match(direction)
  mapping <- if (identical(direction, "sr"))
    param_mapping_s_to_r
  else
    param_mapping_r_to_s

  rlang::set_names(x, unname(sapply(names(x), function(nm) mapping[[nm]] %||% nm)))
}

ml_map_param_names <- function(x, direction = c("sr", "rs"), ...) {
  direction <- rlang::arg_match(direction)
  mapping <- if (identical(direction, "sr"))
    param_mapping_s_to_r
  else
    param_mapping_r_to_s

  unname(sapply(x, function(nm) mapping[[nm]] %||% nm))
}

#' @export
ml_param_map <- function(x, ...) {
  x$param_map %||% stop("'x' does not have a param map")
}

#' @export
ml_get_param <- function(x, param, ...) {
  ml_param_map(x)[[param]] %||% stop("param ", param, " not found")
}

#' @export
ml_get_params <- function(x, params, ...) {
  params %>%
    lapply(function(param) ml_get_param(x, param)) %>%
    rlang::set_names(unlist(params))
}

#' @export
ml_uid <- function(x) {
  x$uid %||% stop("uid not found")
}

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
