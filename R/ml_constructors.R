ml_is_instance_of <- function(jobj, type) {
  sc <- spark_connection(jobj)
  invoke_static(sc, "java.lang.Class", "forName",
                paste0("org.apache.spark.ml.", type)) %>%
    invoke("isInstance", jobj)
}

ml_ancestry <- function(jobj) {
  classes <- c("feature.CountVectorizer", "feature.CountVectorizerModel",
               "classification.Classifier", "classification.ClassificationModel",
               "tuning.CrossValidator",
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
  switch(ml_ancestry(jobj)[1],
         "feature.CountVectorizer" = new_ml_count_vectorizer(jobj),
         "feature.CountVectorizerModel" = new_ml_count_vectorizer_model(jobj),
         "Pipeline" = new_ml_pipeline(jobj),
         "PipelineModel" = new_ml_pipeline_model(jobj),
         "Transformer" = new_ml_transformer(jobj),
         "Estimator" = new_ml_estimator(jobj),
         "tuning.CrossValidator" = new_ml_cross_validator(jobj),
         new_ml_pipeline_stage(jobj))
}

new_ml_pipeline_stage <- function(jobj, ..., subclass = NULL) {
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

new_ml_prediction_model <- function(jobj, ..., subclass = NULL) {
  new_ml_transformer(jobj,
                     ...,
                     subclass = c(subclass, "ml_prediction_model"))
}

new_ml_estimator <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_estimator"))
}

new_ml_count_vectorizer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_count_vectorizer")
}

new_ml_count_vectorizer_model <- function(jobj) {
  new_ml_transformer(jobj,
                   vocabulary = invoke(jobj, "vocabulary"),
                   subclass = "ml_count_vectorizer_model")
}

new_ml_predictor <- function(jobj, ..., subclass = NULL) {
  new_ml_estimator(jobj,
                   ...,
                   subclass = c(subclass, "ml_predictor"))
}

new_ml_cross_validator <- function(jobj) {
  sc <- spark_connection(jobj)
  param_maps <- jobj %>%
    invoke("getEstimatorParamMaps") %>%
    lapply(function(x) invoke_static(sc,
                                     "sparklyr.MLUtils",
                                     "paramMapToNestedList",
                                     x)) %>%
    lapply(function(x) lapply(x, ml_map_param_list_names))

  new_ml_estimator(jobj,
                   estimator_param_maps = param_maps,
                   subclass = "ml_cross_validator")
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
      invoke("stages") %>%
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

#' @export
print.ml_transformer <- function(x, ...) {
  short_type <- strsplit(x$type, "\\.") %>%
    rlang::flatten_chr() %>%
    dplyr::last()
  cat("A Spark", short_type, "(Transformer) \n")
  cat("UID:", x$uid, "\n")
  cat("Class:", x$type, "\n")
  cat("Params:", "\n")
  for (param in names(x$param_map))
    cat("  ", param, ":", capture.output(str(x$param_map[[param]])), "\n")
}

#' @export
print.ml_estimator <- function(x, ...) {
  short_type <- strsplit(x$type, "\\.") %>%
    rlang::flatten_chr() %>%
    dplyr::last()
  cat("A Spark", short_type, "(Estimator) \n")
  cat("UID:", x$uid, "\n")
  cat("Class:", x$type, "\n")
  cat("Params:", "\n")
  for (param in names(x$param_map))
    cat("  ", param, ":", capture.output(str(x$param_map[[param]])), "\n")
}
