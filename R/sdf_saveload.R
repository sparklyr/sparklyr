#' Save / Load a Spark DataFrame
#'
#' Save / load a Spark DataFrame.
#'
#' @param sc A \code{spark_connection} object.
#' @template roxlate-ml-x
#' @param path The path where the Spark DataFrame should be saved.
#' @param name The table name to assign to the saved Spark DataFrame.
#' @param overwrite Boolean; overwrite a pre-existing table of the same name?
#' @param append Boolean; append to a pre-existing table of the same name?
#'
#' @rdname sdf-saveload
#' @name sdf-saveload
NULL

#' @rdname sdf-saveload
#' @export
sdf_save_table <- function(x, name, overwrite = FALSE, append = FALSE) {
  sdf <- spark_dataframe(x)
  name <- ensure_scalar_character(name)

  write <- invoke(sdf, "write")
  if (overwrite) write <- invoke(write, "mode", "overwrite")
  if (append)    write <- invoke(write, "mode", "append")

  invoke(write, "saveAsTable", name)
}

#' @rdname sdf-saveload
#' @export
sdf_load_table <- function(sc, name) {
  session <- spark_session(sc)
  name <- ensure_scalar_character(name)

  sdf <- session %>%
    invoke("read") %>%
    invoke("table", name)

  sdf_register(sdf)
}

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
  path <- as.character(path)

  sdf <- session %>%
    invoke("read") %>%
    invoke("parquet", as.list(path))

  sdf_register(sdf)
}
