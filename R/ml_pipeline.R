#' @export
ml_pipeline <- function(x, ..., uid = random_string("pipeline_")) {
  UseMethod("ml_pipeline")
}

#' @export
ml_pipeline.spark_connection <-
  function(x, uid = random_string("pipeline_")) {
    ensure_scalar_character(uid)
    jobj <- invoke_new(x, "org.apache.spark.ml.Pipeline", uid)
    new_ml_pipeline(jobj)
  }

#' @export
ml_pipeline.ml_pipeline_stage <-
  function(x, ..., uid = random_string("pipeline_")) {
    ensure_scalar_character(uid)
    sc <- spark_connection(x)
    dots <- list(...) %>%
      lapply(function(x)
        spark_jobj(x))
    stages <- c(spark_jobj(x), dots)
    jobj <- invoke_static(sc,
                          "sparklyr.MLUtils",
                          "createPipelineFromStages",
                          uid,
                          stages)
    new_ml_pipeline(jobj)
  }

# Constructors

new_ml_pipeline <- function(jobj, ..., subclass = NULL) {
  stages <- tryCatch({
    jobj %>%
      invoke("getStages") %>%
      lapply(ml_constructor_dispatch)
  },
  error = function(e) {
    NA
  })
  new_ml_estimator(
    jobj,
    stages = stages,
    stage_uids = if (rlang::is_na(stages))
      NA
    else
      sapply(stages, function(x)
        x$uid),
    ...,
    subclass = c(subclass, "ml_pipeline")
  )
}

new_ml_pipeline_model <- function(jobj, ..., subclass = NULL) {
  stages <- tryCatch({
    jobj %>%
      invoke("stages")
  },
  error = function(e) {
    NA
  })

  if (!rlang::is_na(stages))
    stages <- lapply(stages, ml_constructor_dispatch)

  new_ml_transformer(
    jobj,
    stages = stages,
    stage_uids = if (rlang::is_na(stages))
      NA
    else
      sapply(stages, function(x)
        x$uid),
    ...,
    subclass = c(subclass, "ml_pipeline_model")
  )
}

#' @export
spark_connection.ml_pipeline <- function(x, ...) {
  spark_connection(spark_jobj(x))
}

#' @export
spark_connection.ml_pipeline_stage <- function(x, ...) {
  spark_connection(spark_jobj(x))
}

#' @export
spark_connection.ml_pipeline_model <- function(x, ...) {
  spark_connection(spark_jobj(x))
}

#' @export
print.ml_pipeline <- function(x, ...) {
  cat("Pipeline \n")
  cat(paste0("<", x$uid, ">"), "\n")
  cat("  Stages", "\n")
  for (stage in ml_stages(x)) {
    stage_output <- capture.output(print(stage))
    cat(paste0("  |--", stage_output[1]), sep = "\n")
    cat(paste0("  |  ", stage_output[-1]), sep = "\n")
  }
}

#' @export
spark_jobj.ml_pipeline_stage <- function(x, ...) x$.jobj
