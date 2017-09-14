new_ml_pipeline_stage <- function(jobj, ..., subclass = NULL) {
  if (identical(jobj_info(jobj)$class, "org.apache.spark.ml.Pipeline"))
    new_ml_pipeline(jobj)
  else
    structure(
      list(
        uid = invoke(jobj, "uid"),
        type = jobj_info(jobj)$class,
        param_map = ml_get_param_map(jobj),
        .jobj = jobj
      ),
      class = c(subclass, "ml_pipeline_stage")
    )
}

new_ml_transformer <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj, subclass = "ml_transformer")
}

new_ml_estimator <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj, subclass = "ml_estimator")
}

new_ml_pipeline <- function(jobj, ..., subclass = NULL) {
  stages <- tryCatch({
    jobj %>%
      invoke("getStages") %>%
      lapply(new_ml_pipeline_stage)
  },
  error = function(e) {
    NA
  })
  c(new_ml_estimator(jobj, subclass = "ml_pipeline"),
    stages = stages,
    stage_uids = if (rlang::is_na(stages)) NA else sapply(stages, function(x) x$uid))
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
    new_ml_pipeline_stage(jobj)
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
