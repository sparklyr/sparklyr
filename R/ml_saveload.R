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
#' @param file  The filepath used for model save / load. Currently, only local
#'   filepaths are supported.
#'
#' @rdname ml_saveload
#' @name   ml_saveload
#' @export
ml_load <- function(sc, file) {
  ensure_scalar_character(file)
  file <- path.expand(file)

  # read the R metadata
  r <- readRDS(file.path(file, "metadata.rds"))

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
ml_save <- function(model, file) {
  ensure_scalar_character(file)
  file <- path.expand(file)

  # save the Spark bits
  invoke(model$.model, "save", file)

  # save the R bits
  r <- model
  r$.model <- NULL
  saveRDS(r, file = file.path(file, "metadata.rds"))

  file
}

