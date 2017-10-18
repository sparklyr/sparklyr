#' Spark ML -- Pipelines
#'
#' Create Spark ML Pipelines
#'
#' @param x Either a \code{spark_connection} or \code{ml_pipeline_stage} objects
#' @template roxlate-ml-uid
#' @param ... \code{ml_pipeline_stage} objects.
#'
#' @return When \code{x} is a \code{spark_connection}, \code{ml_pipeline()} returns an empty pipeline object. When \code{x} is a \code{ml_pipeline_stage}, \code{ml_pipeline()} returns an \code{ml_pipeline} with the stages set to \code{x} and any transformers or estimators given in \code{...}.
#' @export
ml_pipeline <- function(x, ..., uid = random_string("pipeline_")) {
  UseMethod("ml_pipeline")
}

#' @export
ml_pipeline.spark_connection <-
  function(x, ..., uid = random_string("pipeline_")) {
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
  cat(paste0("Pipeline (Estimator) with "))
  num_stages <- length(ml_stages(x))
  if (num_stages == 1)
    cat("1 stage \n")
  else
    cat(paste0(num_stages, " stages \n"))
  cat(paste0("<", x$uid, ">"), "\n")
  cat("  Stages", "\n")
  for (n in seq_len(num_stages)) {
    stage_output <- capture.output(print(ml_stage(x, n)))
    cat(paste0("  |--[", n, "]", stage_output[1]), sep = "\n")
    cat(paste0("  |     ", stage_output[-1]), sep = "\n")
  }
}

#' @export
print.ml_pipeline_model <- function(x, ...) {
  cat(paste0("PipelineModel (Transformer) with "))
  num_stages <- length(ml_stages(x))
  if (num_stages == 1)
    cat("1 stage \n")
  else
    cat(paste0(num_stages, " stages \n"))
  cat(paste0("<", x$uid, ">"), "\n")
  cat("  Stages", "\n")
  for (n in seq_len(num_stages)) {
    stage_output <- capture.output(print(ml_stage(x, n)))
    cat(paste0("  |--[", n, "]", stage_output[1]), sep = "\n")
    cat(paste0("  |     ", stage_output[-1]), sep = "\n")
  }
}

#' @export
spark_jobj.ml_pipeline_stage <- function(x, ...) x$.jobj
