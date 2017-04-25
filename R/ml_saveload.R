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
ml_load <- function(sc, file, meta = ml_load_meta(file)) {
  ensure_scalar_character(file)
  file <- path.expand(file)

  # read the R metadata
  r <- resolve_fn(meta, file)

  # determine Model class name from fit object name
  rModelName <- regex_replace(
    r$model.parameters$model,
    "Classifier$" = "Classification",
    "Regressor$"  = "Regression"
  )

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
ml_save <- function(model, file, meta = ml_save_meta(model, file)) {
  ensure_scalar_character(file)
  file <- path.expand(file)

  # save the Spark bits
  invoke(model$.model, "save", file)

  # save the R bits
  r <- model
  r$.model <- NULL
  resolve_fn(meta, r, file)

  file
}

ml_save_meta <- function(model, file) {
  fn <- getOption("sparklyr.ml.save")
  if (is.function(fn))
    return(fn(model, file))

  saveRDS(model, file = file.path(file, "metadata.rds"))
}

ml_load_meta <- function(file) {
  fn <- getOption("sparklyr.ml.load")
  if (is.function(fn))
    return(fn(file))

  readRDS(file.path(file, "metadata.rds"))
}
