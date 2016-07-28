ml_save <- function(model, file) {
  ensure_scalar_character(file)

  # save the Spark bits
  invoke(model$.model, "save", file)

  # save the R bits
  r <- model
  r$.model <- NULL
  saveRDS(r, file = file.path(file, "metadata.rds"))

  file
}

ml_load <- function(file) {

  # read the R metadata
  r <- readRDS(file.path(file, "metadata.rds"))

  # read the Spark model
  modelName <- paste(r$model.parameters$model, "Model", sep = "")
  model <- invoke_static(sc, modelName, "load", file)

  # attach back to R object
  r$.model <- model

  # return object
  r
}
