#' Save / Load a Spark DataFrame
#'
#' Save / load a Spark DataFrame.
#'
#' @param sc A \code{spark_connection} object.
#' @template roxlate-ml-x
#' @param path The path where the Spark DataFrame should be saved.
#' @param overwrite Boolean; overwrite a pre-existing table of the same name?
#' @param append Boolean; append to a pre-existing table of the same name?
#'
#' @rdname sdf-saveload
#' @export
sdf_save_parquet <- function(x, path, overwrite = FALSE, append = FALSE) {
  sdf <- spark_dataframe(x)
  path <- ensure_scalar_character(path)

  write <- invoke(sdf, "write")
  if (overwrite) write <- invoke(write, "mode", "overwrite")
  if (append)    write <- invoke(write, "mode", "append")

  invoke(write, "parquet", path)
}

#' @rdname sdf-saveload
#' @export
sdf_load_parquet <- function(sc, path) {
  session <- spark_session(sc)
  sdf <- session %>%
    invoke("read") %>%
    invoke("parquet", as.list(path))
  sdf_register(sdf)
}
