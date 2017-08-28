ml_pipeline_stage <- function(jobj) {
  structure(stats::setNames(list(
    list(
      type = jobj_info(jobj)$class,
      params = ml_get_param_map(jobj),
      .stage = jobj)
  ), invoke(jobj, "uid")),
  class = "ml_pipeline_stage"
  )
}

ml_pipeline <- function(jobj) {
  stages <- invoke_static(spark_connection(jobj),
                          "sparklyr.MLUtils",
                          "explodePipeline",
                          jobj) %>%
    sapply(ml_pipeline_stage)

  structure(
    list(
      stages = stages,
      .pipeline = ml_wrap_in_pipeline(jobj)),
    class = "ml_pipeline"
  )
}

ml_new_transformer <- function(sc, class, input_col, output_col) {
  ensure_scalar_character(input_col)
  ensure_scalar_character(output_col)
  invoke_new(sc, class) %>%
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
    ml_map_param_names()
}

ml_map_param_names <- function(params_list) {
  names(params_list) <- sapply(
    names(params_list),
    function(param) param_mapping_s_to_r[[param]] %||% param)
  params_list
}

ml_fit_and_transform <- function(x, pipeline) {
  sdf <- spark_dataframe(x)
  pipeline$.pipeline %>%
    invoke("fit", sdf) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

ml_new_stage_modified_args <- function(call_frame) {
  envir <- rlang::caller_env()
  modified_args <- call_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    rlang::modify(x = rlang::parse_expr("spark_connection(x)"))
  stage_constructor <- sub("\\..*$", "", rlang::lang_name(call_frame))
  rlang::lang(stage_constructor, rlang::splice(modified_args)) %>%
    rlang::eval_tidy(env = envir)
}
