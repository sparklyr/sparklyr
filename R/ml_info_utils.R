ml_pipeline_stage_info <- function(jobj) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      param_map = ml_get_param_map(jobj),
      .jobj = jobj
    ),
    class = "ml_pipeline_stage"
  )
}

ml_transformer_info <- function(jobj) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      param_map = ml_get_param_map(jobj),
      .jobj = jobj
    ),
    class = c("ml_transformer", "ml_pipeline_stage")
  )
}

ml_info <- function(jobj) {
  uid <- invoke(jobj, "uid")
  type <- jobj_info(jobj)$class
  if (identical(type, "org.apache.spark.ml.Pipeline")) {
    stages <- tryCatch({
      jobj %>%
        invoke("getStages") %>%
        lapply(ml_info)
    },
    error = function(e) {
      NA
    })
    structure(
      list(uid = uid,
           type = type,
           stages = stages,
           stage_uids = if (rlang::is_na(stages)) NA else sapply(stages, function(x) x$uid),
           .jobj = jobj),
      class = c("ml_pipeline", "ml_pipeline_stage")
    )
  } else {
    ml_pipeline_stage_info(jobj)
  }
}

ml_pipeline_model_info <- function(jobj) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      stages = jobj %>%
        invoke("stages") %>%
        lapply(ml_info),
      stage_uids = jobj %>%
        invoke("stages") %>%
        sapply(function(x) invoke(x, "uid")),
      .jobj = jobj
    ),
    class = c("ml_pipeline_model", "ml_pipeline_stage")
  )
}
