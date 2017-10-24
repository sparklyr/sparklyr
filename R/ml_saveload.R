#' Save / Load a Spark ML Model Fit
#'
#' Save / load a \code{ml_model} fit.
#'
#' These functions are currently experimental and not yet ready for production
#' use. Unfortunately, the training summary information for regression fits
#' (linear, logistic, generalized) are currently not serialized as part of the
#' model fit, and so model fits recovered through \code{ml_load} will not work
#' with e.g. \code{fitted}, \code{residuals}, and so on. Such fits should still
#' be suitable for generating predictions with new data, however.
#'
#' @param sc A \code{spark_connection}.
#' @param model A \code{ml_model} fit.
#' @param file The path where the Spark model should be serialized / deserialized.
#' @param meta The path where the \R metadata should be serialized / deserialized.
#'   Currently, this must be a local filesystem path. Alternatively, this can be
#'   an \R function that saves / loads the metadata object.
#'
#' @rdname ml_saveload
#' @name   ml_saveload
#' @export
ml_load <- function(sc, file) {
  ensure_scalar_character(file)
  file <- path.expand(file)

  #Read the R metadata
  system(paste0("hdfs dfs -get ",paste0(file,"/metadata.rds ~/")))
  r <- readRDS("~/metadata.rds")
  rModelName <- r$model.parameters$model

  # read the Spark model
  modelName <- paste0(rModelName, "Model")
  model <- invoke_static(sc, modelName, "load", file)

  # attach back to R object
  r$.model <- model

  # return object
  r
}

#' @rdname ml_saveload
#' @export
ml_save <- function(model, file) {
  ensure_scalar_character(file)
  file <- path.expand(file)
  
  # Remove old copy in HDFS if exists
  system(paste0("hdfs dfs -rm -r ",file))
  
  # save the Spark bits
  invoke(model$.model, "save", file)
  
  # save the R bits
  r <- model
  r$.model <- NULL
  saveRDS(r, "~/metadata.rds")
  system(
    paste("hdfs dfs -put -f ~/metadata.rds", file)
  )
  
  system("rm ~/metadata.rds")

  file
}
