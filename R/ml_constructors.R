ml_is_instance_of <- function(jobj, type) {
  sc <- spark_connection(jobj)
  invoke_static(sc, "java.lang.Class", "forName",
                paste0("org.apache.spark.ml.", type)) %>%
    invoke("isInstance", jobj)
}

ml_inherited <- function(jobj) {
  classes <- c("classification.Classifier",
               "classification.ClassificationModel",
               "Pipeline", "PipelineModel",
               "Estimator", "Transformer")

  Filter(function(x) ml_is_instance_of(jobj, x),
         classes)
}

ml_package <- function(jobj) {
  jobj_info(jobj)$class %>%
    strsplit("\\.") %>%
    rlang::flatten_chr() %>%
    dplyr::nth(-2L)
}

ml_constructor_dispatch <- function(jobj) {
  switch(ml_inherited(jobj)[1],
         "Pipeline" = new_ml_pipeline(jobj),
         "PipelineModel" = new_ml_pipeline_model(jobj),
         "Transformer" = new_ml_transformer(jobj),
         "Estimator" = new_ml_estimator(jobj),
         new_ml_pipeline_stage(jobj))
}

new_ml_pipeline_stage <- function(jobj, ..., subclass = NULL) {
  # if (identical(jobj_info(jobj)$class, "org.apache.spark.ml.Pipeline"))
  #   new_ml_pipeline(jobj)
  # else
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(subclass, "ml_pipeline_stage")
  )
}

new_ml_transformer <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_transformer"))
}

new_ml_estimator <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_estimator"))
}

new_ml_pipeline <- function(jobj, ..., subclass = NULL) {
  stages <- tryCatch({
    jobj %>%
      invoke("getStages") %>%
      lapply(ml_constructor_dispatch)
  },
  error = function(e) {
    NA
  })
  new_ml_estimator(jobj,
                   stages = stages,
                   stage_uids = if (rlang::is_na(stages)) NA else sapply(stages, function(x) x$uid),
                   ...,
                   subclass = c(subclass, "ml_pipeline"))
}

new_ml_pipeline_model <- function(jobj, ..., subclass = NULL) {
  stages <- tryCatch({
    jobj %>%
      invoke("getStages") %>%
      lapply(ml_constructor_dispatch)
  },
  error = function(e) {
    NA
  })
  new_ml_transformer(jobj,
                     stages = stages,
                     stage_uids = if (rlang::is_na(stages)) NA else sapply(stages, function(x) x$uid),
                     ...,
                     subclass = c(subclass, "ml_pipeline_model"))
}
