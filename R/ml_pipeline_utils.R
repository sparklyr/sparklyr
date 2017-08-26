ml_pipeline_stage <- function(jobj, name) {
  name <- name %||% invoke(jobj, "uid")

  structure(stats::setNames(list(
    list(
      name = name,
      type = jobj_info(jobj)$class,
      params = ml_get_param_map(jobj),
      .stage = jobj)
  ), name),
  class = "ml_pipeline_stage"
  )
}

ml_pipeline <- function(jobj, name) {
  structure(
    list(
      stages = ml_pipeline_stage(jobj, name),
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

