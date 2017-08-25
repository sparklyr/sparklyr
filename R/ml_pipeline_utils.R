ml_pipeline_stage <- function(jobj, name) {
  name <- name %||% invoke(jobj, "uid")

  structure(setNames(list(
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
