#' Set/Get Spark checkpoint directory
#'
#' @name checkpoint_directory
#' @param sc A \code{spark_connection}.
#' @param dir checkpoint directory, must be HDFS path of running on cluster
#' @export
spark_set_checkpoint_dir <- function(sc, dir) {
  invisible(sc %>%
              spark_context() %>%
              invoke("setCheckpointDir",
                     spark_normalize_path(dir))
  )
}

#' @rdname checkpoint_directory
#' @export
spark_get_checkpoint_dir <- function(sc) {
  dir <- sc %>%
    spark_context() %>%
    invoke("getCheckpointDir")

  invoke_static(spark_connection(sc),
                "sparklyr.Utils",
                "unboxString",
                dir)
}

#' Generate a Table Name from Expression
#'
#' Attempts to generate a table name from an expression; otherwise,
#' assigns an auto-generated generic name with "sparklyr_" prefix.
#'
#' @param expr The expression to attempt to use as name
#'
#' @export
spark_table_name <- function(expr) {
  table_name <- deparse(expr)
  if (identical(length(table_name), 1L) &&
      grepl("^[a-zA-Z][a-zA-Z0-9_]*$", table_name[[1]])
  ) table_name else random_string(prefix = "sparklyr_tmp_")
}

#' Superclasses of object
#'
#' Extract the classes that a Java object inherits from. This is the jobj equivalent of \code{class()}.
#'
#' @param jobj A \code{spark_jobj}
#' @param simple_name Whether to return simple names, defaults to TRUE
#' @keywords internal
#' @export
jobj_class <- function(jobj, simple_name = TRUE) {
  invoke_static(spark_connection(jobj),
                "sparklyr.Utils",
                "getAncestry",
                jobj,
                cast_scalar_logical(simple_name)) %>%
    unlist()
}
