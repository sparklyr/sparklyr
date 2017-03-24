#' Save / Load a Spark DataFrame
#'
#' Routines for saving and loading Spark DataFrames.
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
  .Deprecated("spark_write_table")

  sdf <- spark_dataframe(x)
  name <- ensure_scalar_character(name)

  writer <- invoke(sdf, "write")
  if (overwrite) writer <- invoke(writer, "mode", "overwrite")
  if (append)    writer <- invoke(writer, "mode", "append")

  # Spark < 2.0.0 doesn't respect the metastore directory when
  # using the 'saveAsTable' API, so we directly call 'save'.
  sc <- spark_connection(sdf)
  if (spark_version(sc) < "2.0.0") {
    hc <- hive_context(sc)
    metastore <- invoke(hc, "getConf", "hive.metastore.warehouse.dir")
    path <- path.expand(file.path(metastore, name))
    invoke(writer, "save", path)
  } else {
    invoke(writer, "saveAsTable", name)
  }
}

#' @rdname sdf-saveload
#' @export
sdf_load_table <- function(sc, name) {
  .Deprecated("spark_read_table")

  session <- spark_session(sc)
  name <- ensure_scalar_character(name)

  # NOTE: need to explicitly provide path to metastore for
  # Spark < 2.0.0
  reader <- invoke(session, "read")
  sdf <- if (spark_version(sc) < "2.0.0") {
    hc <- hive_context(sc)
    metastore <- invoke(hc, "getConf", "hive.metastore.warehouse.dir")
    path <- file.path(metastore, name)
    invoke(reader, "load", path)
  } else {
    invoke(reader, "table", name)
  }

  sdf_register(sdf)
}

#' @rdname sdf-saveload
#' @export
sdf_save_parquet <- function(x, path, overwrite = FALSE, append = FALSE) {
  .Deprecated("spark_write_parquet")

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
  .Deprecated("spark_read_parquet")

  session <- spark_session(sc)
  path <- as.character(path)

  sdf <- session %>%
    invoke("read") %>%
    invoke("parquet", as.list(path))

  sdf_register(sdf)
}
