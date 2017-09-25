ml_add_stage <- function(x, transformer) {
  sc <- spark_connection(x)
  stages <- if (rlang::is_na(x$stages)) list(transformer$.jobj) else {
    x$.jobj %>%
      invoke("getStages") %>%
      c(transformer$.jobj)
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
  invoke_static(sc,
                "sparklyr.MLUtils",
                "getParamMap",
                jobj) %>%
    ml_map_param_list_names()
}

ml_new_stage_modified_args <- function(envir = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  arg_quos <- caller_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    lapply(rlang::new_quosure, env = envir)
  modified_args <- rlang::modify(
    arg_quos,
    # x = quo(spark_connection(!!modified_args[["x"]]))
    x = rlang::new_quosure(rlang::expr(spark_connection(x)), env = caller_frame$env)
    )
  # %>%
  #   lapply(rlang::eval_tidy)
  # str(rlang::eval_tidy(quo(!!spark_connection(!!modified_args[["x"]]))))
  # modified_args <- caller_frame %>%
  #   rlang::lang_standardise() %>%
  #   rlang::lang_args() %>%
  #   rlang::modify(x = rlang::new_quosure(
  #     rlang::parse_expr("spark_connection(x)"),
  #     env = caller_frame$env))
  stage_constructor <- sub("\\..*$", ".spark_connection", rlang::lang_name(caller_frame))
  # rlang::invoke(stage_constructor, modified_args)
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
